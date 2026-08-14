package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	mihomoProviderName = "gateway-subscription"
	mihomoGroupName    = "GATEWAY"
	mihomoPortBase     = 10801
	mihomoUserAgent    = "clash.meta/v1.19.29"
	mihomoMaxSubBytes  = 16 << 20
)

type mihomoSettings struct {
	SubscriptionURL string   `json:"subscription_url"`
	Nodes           []string `json:"nodes,omitempty"`
}

type mihomoUpdateRequest struct {
	SubscriptionURL string `json:"subscription_url"`
	Clear           bool   `json:"clear"`
}

type mihomoGroup struct {
	Name string   `json:"name"`
	Type string   `json:"type"`
	Now  string   `json:"now"`
	All  []string `json:"all"`
}

type mihomoProviderProxy struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Alive bool   `json:"alive"`
}

type mihomoProvider struct {
	Proxies []mihomoProviderProxy `json:"proxies"`
}

type mihomoProviderCacheSnapshot struct {
	path    string
	data    []byte
	existed bool
}

func (s *Server) mihomoStatus(w http.ResponseWriter) {
	settings := s.loadMihomoSettings()
	status, err := s.docker.containerStatus(s.cfg.MihomoContainer)
	if err != nil {
		status = "unavailable"
	}
	controllerOnline := false
	active := ""
	if status == "running" {
		if group, groupErr := s.getMihomoGroup(); groupErr == nil {
			controllerOnline = true
			active = group.Now
		} else if settings.SubscriptionURL == "" && s.mihomoVersionReady() {
			controllerOnline = true
		}
	}
	if status == "running" && !controllerOnline {
		status = "degraded"
	}
	endpoints := make([]map[string]any, 0, len(settings.Nodes))
	provider, _ := s.getMihomoProvider()
	providerNodes := make(map[string]mihomoProviderProxy, len(provider.Proxies))
	for _, proxy := range provider.Proxies {
		providerNodes[proxy.Name] = proxy
	}
	for index, node := range settings.Nodes {
		activeNode := ""
		proxyInfo := providerNodes[node]
		if status == "running" {
			groupName := fmt.Sprintf("GATEWAY-SLOT-%d", index+1)
			if group, groupErr := s.getMihomoProxyGroup(groupName); groupErr == nil {
				activeNode = group.Now
			}
		}
		if activeNode != "" {
			proxyInfo = providerNodes[activeNode]
		}
		endpoints = append(endpoints, map[string]any{
			"url":         fmt.Sprintf("socks5h://mihomo:%d", mihomoPortBase+index),
			"node":        node,
			"active_node": activeNode,
			"type":        proxyInfo.Type,
			"alive":       proxyInfo.Alive,
			"healthy":     proxyInfo.Alive,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":            status,
		"configured":        settings.SubscriptionURL != "",
		"controller_online": controllerOnline,
		"node_count":        len(settings.Nodes),
		"active_node":       active,
		"proxy_urls":        mihomoProxyURLs(len(settings.Nodes)),
		"endpoints":         endpoints,
	})
}

func (s *Server) mihomoProbe(w http.ResponseWriter) {
	if _, err := s.getMihomoProvider(); err != nil {
		writeAPIError(w, http.StatusBadGateway, "mihomo_controller_unavailable", err)
		return
	}
	payload := strings.NewReader(`{"health-check":true}`)
	req, err := http.NewRequest(http.MethodPut, s.cfg.MihomoAPIURL+"/providers/proxies/"+url.PathEscape(mihomoProviderName), payload)
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, "mihomo_probe_failed", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, "mihomo_probe_failed", err)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		writeAPIError(w, http.StatusBadGateway, "mihomo_probe_failed", fmt.Errorf("Mihomo controller returned HTTP %d", resp.StatusCode))
		return
	}
	s.addRotationLog("info", "Mihomo health probe requested", "mihomo", nil)
	provider, _ := s.getMihomoProvider()
	healthy := 0
	for _, proxy := range provider.Proxies {
		if proxy.Alive {
			healthy++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"healthy": healthy, "total": len(provider.Proxies)})
}

func (s *Server) mihomoUpdate(w http.ResponseWriter, r *http.Request) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.rotationMu.Lock()
	defer s.rotationMu.Unlock()
	current := s.loadMihomoSettings()
	var update mihomoUpdateRequest
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&update) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	request := resolveMihomoSettings(current, update)
	if request.SubscriptionURL != "" {
		parsed, err := url.Parse(request.SubscriptionURL)
		if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			http.Error(w, `{"error":"subscription_url_must_be_http_without_userinfo"}`, http.StatusBadRequest)
			return
		}
		if err := s.validateMihomoSubscription(r.Context(), request.SubscriptionURL); err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "mihomo_subscription_invalid", err)
			return
		}
	}
	if err := os.MkdirAll(s.cfg.MihomoConfigDir, 0o755); err != nil {
		http.Error(w, `{"error":"mihomo_config_failed"}`, http.StatusInternalServerError)
		return
	}
	configPath := filepath.Join(s.cfg.MihomoConfigDir, "config.yaml")
	settingsPath := filepath.Join(s.cfg.DataDir, "mihomo.json")
	previousConfig, _ := os.ReadFile(configPath)
	previousSettings, _ := os.ReadFile(settingsPath)
	var providerCache mihomoProviderCacheSnapshot
	rollback := func() {
		if len(previousConfig) > 0 {
			_ = writeAtomic(configPath, previousConfig, 0o600)
		}
		_ = providerCache.restore()
		if len(previousConfig) > 0 {
			_ = s.docker.restartByName(s.cfg.MihomoContainer)
		}
		if len(previousSettings) > 0 {
			_ = writeAtomic(settingsPath, previousSettings, 0o600)
		}
	}
	// Mihomo keeps HTTP providers at a fixed path. Without invalidating that
	// cache, a restart after replacing the URL can expose the previous
	// subscription long enough for waitMihomoNodes to persist its old nodes.
	if update.Clear || strings.TrimSpace(update.SubscriptionURL) != "" {
		var err error
		providerCache, err = invalidateMihomoProviderCache(s.cfg.MihomoConfigDir)
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "mihomo_provider_cache_reset_failed", fmt.Errorf("reset Mihomo provider cache: %w", err))
			return
		}
	}

	request.Nodes = nil
	bootstrap, _ := buildMihomoConfig(request.SubscriptionURL, nil)
	if err := writeAtomic(configPath, []byte(bootstrap), 0o600); err != nil {
		rollback()
		writeAPIError(w, http.StatusInternalServerError, "mihomo_config_write_failed", fmt.Errorf("write Mihomo configuration: %w", err))
		return
	}
	if err := s.validateMihomoConfig(); err != nil {
		rollback()
		writeAPIError(w, http.StatusBadGateway, "mihomo_config_invalid", fmt.Errorf("Mihomo rejected generated configuration: %w", err))
		return
	}
	if err := s.docker.restartByName(s.cfg.MihomoContainer); err != nil {
		rollback()
		writeAPIError(w, http.StatusBadGateway, "mihomo_restart_failed", fmt.Errorf("restart Mihomo container: %w", err))
		return
	}
	if request.SubscriptionURL != "" {
		nodes, err := s.waitMihomoNodes(15 * time.Second)
		if err != nil {
			rollback()
			writeAPIError(w, http.StatusBadGateway, "mihomo_subscription_unavailable", err)
			return
		}
		if len(nodes) > s.cfg.MihomoMaxSlots {
			nodes = nodes[:s.cfg.MihomoMaxSlots]
		}
		request.Nodes = nodes
		finalConfig, _ := buildMihomoConfig(request.SubscriptionURL, request.Nodes)
		if err := writeAtomic(configPath, []byte(finalConfig), 0o600); err != nil {
			rollback()
			writeAPIError(w, http.StatusInternalServerError, "mihomo_config_write_failed", fmt.Errorf("write Mihomo listener configuration: %w", err))
			return
		}
		if err := s.validateMihomoConfig(); err != nil {
			rollback()
			writeAPIError(w, http.StatusBadGateway, "mihomo_config_invalid", fmt.Errorf("Mihomo rejected listener configuration: %w", err))
			return
		}
		if err := s.docker.restartByName(s.cfg.MihomoContainer); err != nil {
			rollback()
			writeAPIError(w, http.StatusBadGateway, "mihomo_restart_failed", fmt.Errorf("restart Mihomo container with listeners: %w", err))
			return
		}
		if !s.waitMihomoReady(10 * time.Second) {
			rollback()
			writeAPIError(w, http.StatusBadGateway, "mihomo_controller_unavailable", errors.New("Mihomo controller did not become ready after restart"))
			return
		}
		if !s.waitMihomoListeners(len(request.Nodes), 10*time.Second) {
			rollback()
			writeAPIError(w, http.StatusBadGateway, "mihomo_listener_start_failed", fmt.Errorf("Mihomo did not open all %d SOCKS5 listeners", len(request.Nodes)))
			return
		}
		if err := s.initializeMihomoSelections(request.Nodes); err != nil {
			rollback()
			writeAPIError(w, http.StatusBadGateway, "mihomo_node_assignment_failed", err)
			return
		}
	} else if !s.waitMihomoReady(10 * time.Second) {
		rollback()
		writeAPIError(w, http.StatusBadGateway, "mihomo_controller_unavailable", errors.New("Mihomo controller did not become ready after restart"))
		return
	}
	settingsData, _ := json.MarshalIndent(request, "", "  ")
	if err := os.MkdirAll(s.cfg.DataDir, 0o700); err != nil || writeAtomic(settingsPath, settingsData, 0o600) != nil {
		rollback()
		http.Error(w, `{"error":"mihomo_settings_persist_failed"}`, http.StatusInternalServerError)
		return
	}
	if request.SubscriptionURL != "" {
		go func() {
			_ = s.RefreshMihomoEgresses()
			_ = s.ReconcileEgresses()
		}()
	}
	s.addRotationLog("info", "Mihomo configuration updated", "control", map[string]any{"configured": request.SubscriptionURL != "", "node_count": len(request.Nodes)})
	writeJSON(w, http.StatusOK, map[string]any{"configured": request.SubscriptionURL != "", "node_count": len(request.Nodes), "proxy_urls": mihomoProxyURLs(len(request.Nodes))})
}

func (s *Server) validateMihomoSubscription(ctx context.Context, subscriptionURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, subscriptionURL, nil)
	if err != nil {
		return errors.New("cannot create subscription request")
	}
	req.Header.Set("User-Agent", mihomoUserAgent)
	req.Header.Set("Accept", "application/yaml, text/yaml, text/plain, */*")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("subscription request failed: %s", safeNetworkError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("subscription server returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, mihomoMaxSubBytes+1))
	if err != nil {
		return fmt.Errorf("cannot read subscription response: %s", safeNetworkError(err))
	}
	if len(body) > mihomoMaxSubBytes {
		return fmt.Errorf("subscription response exceeds %d MiB", mihomoMaxSubBytes>>20)
	}
	content := strings.TrimSpace(strings.TrimPrefix(string(body), "\ufeff"))
	if content == "" {
		return errors.New("subscription response is empty")
	}
	if !containsClashProxies(content) {
		return errors.New("subscription is not Clash/Mihomo YAML (missing proxies:); use the provider's Clash/Clash.Meta subscription URL instead of a Base64 or web-login URL")
	}
	return nil
}

func containsClashProxies(content string) bool {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "proxies:" || strings.HasPrefix(line, "proxies: ") {
			return true
		}
	}
	return false
}

func safeNetworkError(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Err != nil {
			return urlErr.Err.Error()
		}
		return "network error"
	}
	return err.Error()
}

func resolveMihomoSettings(current mihomoSettings, update mihomoUpdateRequest) mihomoSettings {
	result := current
	result.SubscriptionURL = strings.TrimSpace(update.SubscriptionURL)
	if result.SubscriptionURL == "" && !update.Clear {
		result.SubscriptionURL = current.SubscriptionURL
	}
	if update.Clear {
		result.SubscriptionURL = ""
	}
	return result
}

func (s *Server) loadMihomoSettings() mihomoSettings {
	var settings mihomoSettings
	data, err := os.ReadFile(filepath.Join(s.cfg.DataDir, "mihomo.json"))
	if err == nil {
		_ = json.Unmarshal(data, &settings)
	}
	return settings
}

func (s *Server) getMihomoGroup() (mihomoGroup, error) {
	return s.getMihomoProxyGroup(mihomoGroupName)
}

func (s *Server) getMihomoProxyGroup(name string) (mihomoGroup, error) {
	var group mihomoGroup
	req, err := http.NewRequest(http.MethodGet, s.cfg.MihomoAPIURL+"/proxies/"+url.PathEscape(name), nil)
	if err != nil {
		return group, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return group, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return group, fmt.Errorf("mihomo controller returned HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&group); err != nil {
		return group, err
	}
	return group, nil
}

func (s *Server) selectMihomoProxyGroup(name, node string) error {
	payload, err := json.Marshal(map[string]string{"name": node})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, s.cfg.MihomoAPIURL+"/proxies/"+url.PathEscape(name), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusNoContent && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		return fmt.Errorf("mihomo selector %s returned HTTP %d", name, resp.StatusCode)
	}
	return nil
}

func (s *Server) initializeMihomoSelections(nodes []string) error {
	for index, node := range nodes {
		group := fmt.Sprintf("GATEWAY-SLOT-%d", index+1)
		if err := s.selectMihomoProxyGroup(group, node); err != nil {
			return fmt.Errorf("assign %s to %s: %w", node, group, err)
		}
	}
	return nil
}

func (s *Server) getMihomoProvider() (mihomoProvider, error) {
	var provider mihomoProvider
	req, err := http.NewRequest(http.MethodGet, s.cfg.MihomoAPIURL+"/providers/proxies/"+url.PathEscape(mihomoProviderName), nil)
	if err != nil {
		return provider, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return provider, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return provider, fmt.Errorf("mihomo provider returned HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&provider); err != nil {
		return provider, err
	}
	return provider, nil
}

func (s *Server) mihomoVersionReady() bool {
	req, err := http.NewRequest(http.MethodGet, s.cfg.MihomoAPIURL+"/version", nil)
	if err != nil {
		return false
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode == http.StatusOK
}

func (s *Server) waitMihomoReady(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.mihomoVersionReady() {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func (s *Server) validateMihomoConfig() error {
	return s.docker.exec(s.cfg.MihomoContainer, []string{"/mihomo", "-t", "-d", "/etc/mihomo", "-f", "/etc/mihomo/config.yaml"})
}

func (s *Server) waitMihomoListeners(count int, timeout time.Duration) bool {
	if count == 0 {
		return true
	}
	controller, err := url.Parse(s.cfg.MihomoAPIURL)
	if err != nil || controller.Hostname() == "" {
		return false
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allReady := true
		for index := 0; index < count; index++ {
			address := net.JoinHostPort(controller.Hostname(), strconv.Itoa(mihomoPortBase+index))
			conn, dialErr := net.DialTimeout("tcp", address, 300*time.Millisecond)
			if dialErr != nil {
				allReady = false
				break
			}
			_ = conn.Close()
		}
		if allReady {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func (s *Server) waitMihomoNodes(timeout time.Duration) ([]string, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		group, err := s.getMihomoGroup()
		if err == nil {
			nodes := s.routableMihomoNodes(group.All)
			if len(nodes) > 0 {
				return nodes, nil
			}
			err = fmt.Errorf("subscription contains no routable proxy nodes")
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("mihomo nodes unavailable: %w", lastErr)
}

func (s *Server) routableMihomoNodes(values []string) []string {
	candidates := uniqueMihomoNodes(values)
	provider, err := s.getMihomoProvider()
	if err != nil {
		return nil
	}
	types := make(map[string]string, len(provider.Proxies))
	for _, proxy := range provider.Proxies {
		types[proxy.Name] = proxy.Type
	}
	result := make([]string, 0, min(len(candidates), s.cfg.MihomoMaxSlots))
	for _, candidate := range candidates {
		if !isRoutableMihomoType(types[candidate]) {
			continue
		}
		result = append(result, candidate)
		if len(result) >= s.cfg.MihomoMaxSlots {
			break
		}
	}
	return result
}

func isRoutableMihomoType(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "direct", "reject", "reject-drop", "pass", "selector", "urltest", "fallback", "loadbalance", "relay":
		return false
	default:
		return true
	}
}

func uniqueMihomoNodes(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || value == "DIRECT" || value == "REJECT" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func mihomoProxyURLs(count int) []string {
	result := make([]string, 0, count)
	for index := 0; index < count; index++ {
		result = append(result, fmt.Sprintf("socks5h://mihomo:%d", mihomoPortBase+index))
	}
	return result
}

func buildMihomoConfig(subscriptionURL string, nodes []string) (string, []string) {
	var b strings.Builder
	b.WriteString("mixed-port: 7890\nallow-lan: true\nbind-address: \"*\"\nmode: rule\nlog-level: warning\nipv6: true\nexternal-controller: 0.0.0.0:9090\n")
	if subscriptionURL == "" {
		b.WriteString("rules:\n  - MATCH,DIRECT\n")
		return b.String(), nil
	}
	b.WriteString("proxy-providers:\n  " + mihomoProviderName + ":\n    type: http\n    url: " + strconv.Quote(subscriptionURL) + "\n    path: ./providers/" + mihomoProviderName + ".yaml\n    interval: 3600\n    header:\n      User-Agent:\n        - " + strconv.Quote(mihomoUserAgent) + "\n    health-check:\n      enable: true\n      url: https://www.gstatic.com/generate_204\n      interval: 300\n")
	b.WriteString("proxy-groups:\n  - name: " + mihomoGroupName + "\n    type: url-test\n    use:\n      - " + mihomoProviderName + "\n    url: https://www.gstatic.com/generate_204\n    interval: 300\n")
	for index := range nodes {
		group := fmt.Sprintf("GATEWAY-SLOT-%d", index+1)
		b.WriteString("  - name: " + group + "\n    type: select\n    use:\n      - " + mihomoProviderName + "\n")
	}
	if len(nodes) > 0 {
		b.WriteString("listeners:\n")
		for index := range nodes {
			b.WriteString(fmt.Sprintf("  - name: gateway-slot-%d\n    type: socks\n    listen: 0.0.0.0\n    port: %d\n    udp: false\n    proxy: GATEWAY-SLOT-%d\n", index+1, mihomoPortBase+index, index+1))
		}
	}
	b.WriteString("rules:\n  - MATCH," + mihomoGroupName + "\n")
	return b.String(), mihomoProxyURLs(len(nodes))
}

func invalidateMihomoProviderCache(configDir string) (mihomoProviderCacheSnapshot, error) {
	cachePath := filepath.Join(configDir, "providers", mihomoProviderName+".yaml")
	snapshot := mihomoProviderCacheSnapshot{path: cachePath}
	data, err := os.ReadFile(cachePath)
	if err == nil {
		snapshot.data = data
		snapshot.existed = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return snapshot, err
	}
	if err := os.Remove(cachePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return snapshot, err
	}
	return snapshot, nil
}

func (snapshot mihomoProviderCacheSnapshot) restore() error {
	if snapshot.path == "" {
		return nil
	}
	if err := os.Remove(snapshot.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !snapshot.existed {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(snapshot.path), 0o755); err != nil {
		return err
	}
	return writeAtomic(snapshot.path, snapshot.data, 0o600)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, mode); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}
