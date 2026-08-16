package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type proxySlot struct {
	client         *http.Client
	url            string
	mu             sync.Mutex
	fails          int
	until          time.Time
	egress         string
	disabled       bool
	probeError     string
	modelCooldowns map[string]cooldownState
}

type cooldownState struct {
	Fails int
	Until time.Time
}

var (
	errNoUntriedSlot        = errors.New("no untried upstream slot")
	errUpstreamResponseRead = errors.New("upstream response read")
	errUpstreamStreamEmpty  = errors.New("upstream stream ended before first output")
)

const (
	maxModelCooldownsPerSlot = 256
	maxBufferedResponseBytes = 32 << 20
)

func (s *proxySlot) identity() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.egress != "" {
		return "egress:" + s.egress
	}
	if s.url != "" {
		return "proxy:" + s.url
	}
	return "direct"
}

func (s *proxySlot) available(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !now.Before(s.until)
}

func nextCooldown(fails int, base, max, minimum time.Duration) (int, time.Time, time.Duration) {
	fails++
	d := base
	for i := 1; i < fails && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	d += time.Duration(mathrand.Int64N(int64(d/5) + 1))
	if minimum > d {
		d = minimum
	}
	return fails, time.Now().Add(d), d
}

func (s *proxySlot) cooldown(base, max, minimum time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	var d time.Duration
	s.fails, s.until, d = nextCooldown(s.fails, base, max, minimum)
	return d
}

func (s *proxySlot) cooldownModel(model string, base, max, minimum time.Duration) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.modelCooldowns == nil {
		s.modelCooldowns = make(map[string]cooldownState)
	}
	now := time.Now()
	var oldestModel string
	var oldestUntil time.Time
	for currentModel, current := range s.modelCooldowns {
		if !now.Before(current.Until) {
			delete(s.modelCooldowns, currentModel)
			continue
		}
		if oldestUntil.IsZero() || current.Until.Before(oldestUntil) {
			oldestModel, oldestUntil = currentModel, current.Until
		}
	}
	if _, exists := s.modelCooldowns[model]; !exists && len(s.modelCooldowns) >= maxModelCooldownsPerSlot {
		delete(s.modelCooldowns, oldestModel)
	}
	state := s.modelCooldowns[model]
	var d time.Duration
	state.Fails, state.Until, d = nextCooldown(state.Fails, base, max, minimum)
	s.modelCooldowns[model] = state
	return d
}

func (s *proxySlot) success(model string) {
	s.mu.Lock()
	s.fails = 0
	s.until = time.Time{}
	delete(s.modelCooldowns, model)
	s.mu.Unlock()
}

func (s *proxySlot) readiness(model string, now time.Time) (disabled bool, ready time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ready = s.until
	if state, exists := s.modelCooldowns[model]; exists {
		if now.Before(state.Until) && state.Until.After(ready) {
			ready = state.Until
		} else if !now.Before(state.Until) {
			delete(s.modelCooldowns, model)
		}
	}
	return s.disabled, ready
}

func (s *proxySlot) readyAt() time.Time { s.mu.Lock(); defer s.mu.Unlock(); return s.until }

type Stats struct {
	Requests    atomic.Uint64
	Success     atomic.Uint64
	Upstream429 atomic.Uint64
	Gateway429  atomic.Uint64
	Errors      atomic.Uint64
}
type Gateway struct {
	cfg     Config
	client  *http.Client
	slotsMu sync.RWMutex
	slots   []*proxySlot
	base    []*proxySlot
	sem     chan struct{}
	queue   chan struct{}
	active  atomic.Uint64
	stats   Stats
	log     *slog.Logger
	keyMu   sync.RWMutex
	keys    map[string]struct{}
	auditMu sync.RWMutex
	audits  []auditRecord
	logsMu  sync.RWMutex
	logs    []systemLog
}

type auditRecord struct {
	At               time.Time `json:"at"`
	RequestID        string    `json:"request_id,omitempty"`
	Method           string    `json:"method"`
	Path             string    `json:"path"`
	Model            string    `json:"model,omitempty"`
	Status           int       `json:"status"`
	Slot             string    `json:"slot"`
	Egress           string    `json:"egress,omitempty"`
	LatencyMS        int64     `json:"latency_ms"`
	Source           string    `json:"source"`
	Attempts         int       `json:"attempts"`
	RetryAfter       string    `json:"retry_after,omitempty"`
	ClientKey        string    `json:"client_key,omitempty"`
	Stream           bool      `json:"stream"`
	PromptTokens     int64     `json:"prompt_tokens,omitempty"`
	CompletionTokens int64     `json:"completion_tokens,omitempty"`
	TotalTokens      int64     `json:"total_tokens,omitempty"`
	CachedTokens     int64     `json:"cached_tokens,omitempty"`
	FirstTokenMS     int64     `json:"first_token_ms,omitempty"`
}

type tokenUsage struct {
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CachedTokens     int64
}

type auditMetadata struct {
	ClientKey string
	RequestID string
	Stream    bool
}

type auditMetadataKey struct{}

type systemLog struct {
	At      time.Time      `json:"at"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

func New(cfg Config, logger *slog.Logger) (*Gateway, error) {
	g := &Gateway{cfg: cfg, client: &http.Client{Timeout: cfg.RequestTimeout}, sem: make(chan struct{}, cfg.MaxConcurrency), queue: make(chan struct{}, cfg.MaxConcurrency+cfg.QueueSize), log: logger, keys: make(map[string]struct{})}
	for _, key := range cfg.GatewayKeys {
		g.keys[key] = struct{}{}
	}
	g.loadKeys()
	var directSlot *proxySlot
	if cfg.DirectEnabled {
		directSlot = &proxySlot{client: g.client}
		g.slots = append(g.slots, directSlot)
	}
	staticSlots, err := g.buildProxySlots(cfg.ProxyURLs)
	if err != nil {
		return nil, err
	}
	g.slots = append(g.slots, staticSlots...)
	if len(staticSlots) > 0 {
		for _, slot := range staticSlots {
			slot.disabled = true
		}
		go g.probeAndDeduplicateStaticSlots()
	} else if directSlot != nil && len(cfg.ProxyProbeURLs) > 0 {
		go g.probeDirectSlot(directSlot)
	}
	g.base = append([]*proxySlot(nil), g.slots...)
	if cfg.ProxyListFile != "" {
		if err := g.refreshProxyList(); err != nil {
			g.log.Warn("initial dynamic proxy refresh failed", "error", err)
			g.addLog("warn", "initial dynamic proxy refresh failed", map[string]any{"error": err.Error()})
		}
		go g.proxyRefreshLoop()
	}
	if len(g.snapshotSlots()) == 0 {
		return nil, errors.New("no usable direct or proxy slots")
	}
	return g, nil
}

func (g *Gateway) probeDirectSlot(slot *proxySlot) {
	if slot == nil || slot.url != "" {
		return
	}
	ip, err := g.refreshSlotEgress(slot)
	if err != nil {
		g.addLog("warn", "direct egress probe failed", map[string]any{"error": err.Error()})
		return
	}
	_ = ip
}

func (g *Gateway) probeAndDeduplicateStaticSlots() {
	slots := g.snapshotSlots()
	type probeResult struct {
		slot *proxySlot
		ip   string
		err  error
	}
	results := make(chan probeResult, len(slots))
	jobs := make(chan *proxySlot)
	workerCount := min(g.cfg.ProxyProbeJobs, len(slots))
	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slot := range jobs {
				ip, err := g.probeSlot(slot)
				if err != nil {
					g.addLog("warn", "proxy egress probe failed", map[string]any{"proxy": safeProxyURL(slot.url), "error": err.Error()})
				}
				results <- probeResult{slot: slot, ip: ip, err: err}
			}
		}()
	}
	go func() {
		for _, slot := range slots {
			jobs <- slot
		}
		close(jobs)
	}()
	wg.Wait()
	close(results)
	probed := make(map[*proxySlot]string, len(slots))
	for result := range results {
		result.slot.mu.Lock()
		if result.err != nil {
			result.slot.disabled = true
			result.slot.probeError = result.err.Error()
			result.slot.mu.Unlock()
			continue
		}
		result.slot.egress = result.ip
		result.slot.disabled = false
		result.slot.probeError = ""
		result.slot.mu.Unlock()
		probed[result.slot] = result.ip
	}
	seen := make(map[string]int, len(slots))
	deduplicated := make([]*proxySlot, 0, len(slots))
	for _, slot := range slots {
		slot.mu.Lock()
		disabled, egress := slot.disabled, slot.egress
		slot.mu.Unlock()
		if disabled || egress == "" {
			deduplicated = append(deduplicated, slot)
			continue
		}
		key := slot.identity()
		if ip := probed[slot]; ip != "" {
			key = "egress:" + ip
		}
		if index, exists := seen[key]; exists {
			deduplicated[index].mu.Lock()
			if deduplicated[index].url == "" && slot.url != "" {
				deduplicated[index].disabled = true
				deduplicated[index].probeError = "duplicate egress"
				deduplicated[index].mu.Unlock()
				seen[key] = len(deduplicated)
				deduplicated = append(deduplicated, slot)
			} else {
				deduplicated[index].mu.Unlock()
				slot.mu.Lock()
				slot.disabled = true
				slot.probeError = "duplicate egress"
				slot.mu.Unlock()
				deduplicated = append(deduplicated, slot)
			}
			continue
		}
		seen[key] = len(deduplicated)
		deduplicated = append(deduplicated, slot)
	}
	g.slotsMu.Lock()
	g.slots = deduplicated
	g.slotsMu.Unlock()
}

func (g *Gateway) buildProxySlots(rawURLs []string) ([]*proxySlot, error) {
	var slots []*proxySlot
	for _, raw := range rawURLs {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
			return nil, errors.New("proxy URL must use http, https, socks5, or socks5h: " + raw)
		}
		transport := &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 16, IdleConnTimeout: 90 * time.Second}
		if u.Scheme == "http" || u.Scheme == "https" {
			transport.Proxy = http.ProxyURL(u)
		} else {
			username, password := "", ""
			if u.User != nil {
				username = u.User.Username()
				password, _ = u.User.Password()
			}
			proxyAddress := u.Host
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				return dialSOCKS5(ctx, network, proxyAddress, address, username, password)
			}
		}
		slots = append(slots, &proxySlot{client: &http.Client{Transport: transport, Timeout: g.cfg.RequestTimeout}, url: raw})
	}
	return slots, nil
}

func dialSOCKS5(ctx context.Context, network, proxyAddress, target, username, password string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("socks5 supports tcp only")
	}
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddress)
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = conn.Close()
		}
	}()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	methods := []byte{0x00}
	if username != "" || password != "" {
		methods = append(methods, 0x02)
	}
	if _, err = conn.Write(append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		return nil, err
	}
	choice := make([]byte, 2)
	if _, err = io.ReadFull(conn, choice); err != nil {
		return nil, err
	}
	if choice[0] != 0x05 || choice[1] == 0xff {
		return nil, fmt.Errorf("socks5 proxy rejected authentication methods")
	}
	switch choice[1] {
	case 0x00:
	case 0x02:
		if len(username) > 255 || len(password) > 255 {
			return nil, fmt.Errorf("socks5 credentials too long")
		}
		auth := append([]byte{0x01, byte(len(username))}, []byte(username)...)
		auth = append(auth, byte(len(password)))
		auth = append(auth, []byte(password)...)
		if _, err = conn.Write(auth); err != nil {
			return nil, err
		}
		response := make([]byte, 2)
		if _, err = io.ReadFull(conn, response); err != nil {
			return nil, err
		}
		if response[1] != 0x00 {
			return nil, fmt.Errorf("socks5 username/password authentication failed")
		}
	default:
		return nil, fmt.Errorf("socks5 proxy selected unsupported authentication method %d", choice[1])
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, err
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, fmt.Errorf("invalid target port %q", port)
	}
	request := []byte{0x05, 0x01, 0x00}
	if ip := net.ParseIP(host); ip != nil {
		if ipv4 := ip.To4(); ipv4 != nil {
			request = append(request, 0x01)
			request = append(request, ipv4...)
		} else {
			request = append(request, 0x04)
			request = append(request, ip.To16()...)
		}
	} else {
		if len(host) > 255 {
			return nil, fmt.Errorf("target hostname too long")
		}
		request = append(request, 0x03, byte(len(host)))
		request = append(request, []byte(host)...)
	}
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], uint16(portNumber))
	request = append(request, portBytes[:]...)
	if _, err = conn.Write(request); err != nil {
		return nil, err
	}
	header := make([]byte, 4)
	if _, err = io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	if header[1] != 0x00 {
		return nil, fmt.Errorf("socks5 connect failed with code %d", header[1])
	}
	var skip int
	switch header[3] {
	case 0x01:
		skip = 4
	case 0x04:
		skip = 16
	case 0x03:
		length := make([]byte, 1)
		if _, err = io.ReadFull(conn, length); err != nil {
			return nil, err
		}
		skip = int(length[0])
	default:
		return nil, fmt.Errorf("socks5 returned invalid address type")
	}
	if _, err = io.CopyN(io.Discard, conn, int64(skip+2)); err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	closeOnError = false
	return conn, nil
}

func (g *Gateway) snapshotSlots() []*proxySlot {
	g.slotsMu.RLock()
	defer g.slotsMu.RUnlock()
	return append([]*proxySlot(nil), g.slots...)
}

func (g *Gateway) proxyRefreshLoop() {
	ticker := time.NewTicker(g.cfg.ProxyRefresh)
	defer ticker.Stop()
	for range ticker.C {
		if err := g.refreshProxyList(); err != nil {
			g.log.Warn("dynamic proxy refresh failed", "error", err)
			g.addLog("warn", "dynamic proxy refresh failed", map[string]any{"error": err.Error()})
		}
	}
}

func (g *Gateway) refreshProxyList() error {
	data, err := os.ReadFile(g.cfg.ProxyListFile)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{})
	var rawURLs []string
	for _, line := range strings.Split(string(data), "\n") {
		raw := strings.TrimSpace(strings.SplitN(line, "#", 2)[0])
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; !ok {
			seen[raw] = struct{}{}
			rawURLs = append(rawURLs, raw)
		}
	}
	candidates, err := g.buildProxySlots(rawURLs)
	if err != nil {
		return err
	}
	existing := make(map[string]*proxySlot)
	for _, slot := range g.snapshotSlots() {
		if slot.url != "" {
			existing[slot.url] = slot
		}
	}
	for i, slot := range candidates {
		if previous := existing[slot.url]; previous != nil {
			candidates[i] = previous
		}
	}
	type probeResult struct {
		slot *proxySlot
		ip   string
	}
	results := make(chan probeResult, len(candidates))
	jobs := make(chan *proxySlot)
	var wg sync.WaitGroup
	workerCount := min(g.cfg.ProxyProbeJobs, len(candidates))
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				ip, err := g.probeSlot(s)
				if err == nil {
					results <- probeResult{slot: s, ip: ip}
				}
			}
		}()
	}
	go func() {
		for _, slot := range candidates {
			jobs <- slot
		}
		close(jobs)
	}()
	wg.Wait()
	close(results)

	// Multiple local ports can resolve to the same tunnel egress. Keep one slot per observed IP.
	byEgress := make(map[string]*proxySlot)
	for result := range results {
		result.slot.mu.Lock()
		result.slot.egress = result.ip
		result.slot.mu.Unlock()
		key := result.ip
		if key == "" {
			key = result.slot.url
		}
		if _, exists := byEgress[key]; !exists {
			byEgress[key] = result.slot
		}
	}
	var dynamic []*proxySlot
	for _, raw := range rawURLs {
		for _, slot := range byEgress {
			if slot.url == raw {
				dynamic = append(dynamic, slot)
				break
			}
		}
	}
	target := g.cfg.TargetEgress
	if target == 0 {
		target = g.cfg.MaxConcurrency
	}
	if len(dynamic) > target {
		dynamic = dynamic[:target]
	}

	base := append(make([]*proxySlot, 0, len(g.base)+len(dynamic)), g.base...)
	base = append(base, dynamic...)
	if len(base) == 0 {
		return fmt.Errorf("no proxy in %s passed the health probe", g.cfg.ProxyListFile)
	}
	g.slotsMu.Lock()
	g.slots = base
	g.slotsMu.Unlock()
	g.log.Info("dynamic proxy slots refreshed", "candidates", len(candidates), "healthy_unique_egress", len(dynamic), "total_slots", len(base))
	g.addLog("info", "dynamic proxy slots refreshed", map[string]any{"candidates": len(candidates), "healthy_unique_egress": len(dynamic), "total_slots": len(base)})
	return nil
}

func (g *Gateway) probeSlot(slot *proxySlot) (string, error) {
	var failures []string
	for _, probeURL := range g.cfg.ProxyProbeURLs {
		ctx, cancel := context.WithTimeout(context.Background(), g.cfg.ProxyProbeWait)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL, nil)
		if err == nil {
			var resp *http.Response
			resp, err = slot.client.Do(req)
			if err == nil {
				body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
				resp.Body.Close()
				if readErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 400 {
					if ip := parseProbeIP(body); ip != "" {
						cancel()
						return ip, nil
					}
					err = fmt.Errorf("invalid IP response")
				} else if readErr != nil {
					err = readErr
				} else {
					err = fmt.Errorf("HTTP %d", resp.StatusCode)
				}
			}
		}
		cancel()
		failures = append(failures, probeURL+": "+err.Error())
	}
	return "", fmt.Errorf("probe via %s failed (%s)", safeProxyURL(slot.url), strings.Join(failures, "; "))
}

func (g *Gateway) refreshSlotEgress(slot *proxySlot) (string, error) {
	if slot == nil || slot.client == nil {
		return "", errors.New("proxy slot is unavailable")
	}
	// A Mihomo selector switch only affects new connections. Close any pooled
	// upstream connection before probing so subsequent requests use the new node.
	slot.client.CloseIdleConnections()
	ip, err := g.probeSlot(slot)
	if err != nil {
		slot.mu.Lock()
		slot.disabled = true
		slot.probeError = err.Error()
		slot.mu.Unlock()
		return "", err
	}
	slot.mu.Lock()
	previous := slot.egress
	slot.egress = ip
	slot.fails = 0
	slot.until = time.Time{}
	slot.disabled = false
	slot.probeError = ""
	if previous != ip {
		slot.modelCooldowns = nil
	}
	slot.mu.Unlock()
	if previous != ip {
		g.addLog("info", "proxy slot egress changed", map[string]any{"url": safeProxyURL(slot.url), "previous": previous, "egress": ip})
	}
	return ip, nil
}

func safeProxyURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "[redacted-proxy]"
	}
	parsed.User = nil
	return parsed.String()
}

func parseProbeIP(body []byte) string {
	text := strings.TrimSpace(string(body))
	if net.ParseIP(text) != nil {
		return text
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ip=") {
			if ip := strings.TrimSpace(strings.TrimPrefix(line, "ip=")); net.ParseIP(ip) != nil {
				return ip
			}
		}
	}
	return ""
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/", g.admin)
	mux.HandleFunc("/healthz", g.health)
	mux.HandleFunc("/metrics", g.metrics)
	mux.HandleFunc("/v1/", g.serve)
	mux.HandleFunc("/openai/", g.serve)
	mux.HandleFunc("/anthropic/", g.serve)
	mux.HandleFunc("/codex/", g.serve)
	return mux
}

func (g *Gateway) admin(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin")
	if !g.adminAuthorized(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	switch {
	case path == "/summary" && r.Method == http.MethodGet:
		g.writeJSON(w, http.StatusOK, g.summary())
	case path == "/keys" && r.Method == http.MethodGet:
		g.keyMu.RLock()
		keys := make([]string, 0, len(g.keys))
		for key := range g.keys {
			keys = append(keys, key)
		}
		g.keyMu.RUnlock()
		g.writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
	case path == "/keys" && r.Method == http.MethodPost:
		key, err := newGatewayKey()
		if err != nil {
			http.Error(w, `{"error":"key_generation_failed"}`, http.StatusInternalServerError)
			return
		}
		g.keyMu.Lock()
		g.keys[key] = struct{}{}
		g.persistKeysLocked()
		g.keyMu.Unlock()
		g.writeJSON(w, http.StatusCreated, map[string]string{"key": key})
	case path == "/keys" && r.Method == http.MethodPut:
		var body struct {
			Keys []string `json:"keys"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body) != nil || len(body.Keys) == 0 {
			http.Error(w, `{"error":"invalid_keys"}`, http.StatusBadRequest)
			return
		}
		replacement := make(map[string]struct{}, len(body.Keys))
		for _, key := range body.Keys {
			if key = strings.TrimSpace(key); key != "" {
				replacement[key] = struct{}{}
			}
		}
		if len(replacement) == 0 {
			http.Error(w, `{"error":"at_least_one_key_required"}`, http.StatusBadRequest)
			return
		}
		g.keyMu.Lock()
		g.keys = replacement
		g.persistKeysLocked()
		g.keyMu.Unlock()
		g.writeJSON(w, http.StatusOK, map[string]int{"count": len(replacement)})
	case strings.HasPrefix(path, "/keys/") && r.Method == http.MethodDelete:
		key := strings.TrimPrefix(path, "/keys/")
		g.keyMu.Lock()
		delete(g.keys, key)
		g.persistKeysLocked()
		g.keyMu.Unlock()
		g.writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
	case path == "/audit" && r.Method == http.MethodGet:
		g.auditMu.RLock()
		records := append([]auditRecord(nil), g.audits...)
		g.auditMu.RUnlock()
		g.writeJSON(w, http.StatusOK, map[string]any{"records": records})
	case path == "/logs" && r.Method == http.MethodGet:
		g.logsMu.RLock()
		logs := append([]systemLog(nil), g.logs...)
		g.logsMu.RUnlock()
		g.writeJSON(w, http.StatusOK, map[string]any{"logs": logs})
	case path == "/slots" && r.Method == http.MethodPost:
		var body struct {
			URL     string `json:"url"`
			Enabled bool   `json:"enabled"`
		}
		if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		found := false
		for _, slot := range g.snapshotSlots() {
			slot.mu.Lock()
			if slot.url == body.URL {
				slot.disabled = !body.Enabled
				found = true
			}
			slot.mu.Unlock()
		}
		if !found {
			http.Error(w, `{"error":"slot_not_found"}`, http.StatusNotFound)
			return
		}
		g.writeJSON(w, http.StatusOK, map[string]bool{"enabled": body.Enabled})
		g.addLog("info", "proxy slot state changed", map[string]any{"url": body.URL, "enabled": body.Enabled})
	case path == "/probe" && r.Method == http.MethodPost:
		var body struct {
			URL string `json:"url"`
		}
		if r.Body != nil && r.ContentLength != 0 && json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		results := make([]map[string]string, 0)
		matched := false
		for _, slot := range g.snapshotSlots() {
			if body.URL != "" && slot.url != body.URL {
				continue
			}
			matched = true
			ip, err := g.refreshSlotEgress(slot)
			if err != nil {
				g.writeJSON(w, http.StatusBadGateway, map[string]string{"error": "probe_failed", "url": slot.url, "detail": err.Error()})
				return
			}
			results = append(results, map[string]string{"url": slot.url, "egress": ip})
		}
		if !matched {
			http.Error(w, `{"error":"slot_not_found"}`, http.StatusNotFound)
			return
		}
		g.writeJSON(w, http.StatusOK, map[string]any{"slots": results})
	case path == "/rotate" && r.Method == http.MethodPost:
		var body struct {
			Forbidden []string `json:"forbidden"`
		}
		if r.Body != nil && r.ContentLength != 0 && json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&body) != nil {
			http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
			return
		}
		slots := g.snapshotSlots()
		if len(slots) < 2 {
			http.Error(w, `{"error":"no_alternative_slot"}`, http.StatusConflict)
			return
		}
		forbidden := make(map[string]struct{}, len(body.Forbidden))
		for _, ip := range body.Forbidden {
			if ip = strings.TrimSpace(ip); ip != "" {
				forbidden[ip] = struct{}{}
			}
		}
		current := int(g.active.Load() % uint64(len(slots)))
		previous := slots[current]
		previous.mu.Lock()
		previousURL, previousEgress := previous.url, previous.egress
		previous.mu.Unlock()
		now := time.Now()
		for offset := 1; offset < len(slots); offset++ {
			index := (current + offset) % len(slots)
			slot := slots[index]
			disabled, ready := slot.readiness("", now)
			if disabled || now.Before(ready) {
				continue
			}
			slot.mu.Lock()
			egress, raw := slot.egress, slot.url
			slot.mu.Unlock()
			if egress == "" {
				var err error
				egress, err = g.refreshSlotEgress(slot)
				if err != nil {
					continue
				}
			}
			if egress == previousEgress {
				continue
			}
			if _, occupied := forbidden[egress]; occupied {
				continue
			}
			slot.client.CloseIdleConnections()
			g.active.Store(uint64(index))
			g.addLog("info", "active proxy slot changed", map[string]any{"previous_url": previousURL, "previous_egress": previousEgress, "url": raw, "egress": egress})
			g.writeJSON(w, http.StatusOK, map[string]any{"previous_url": previousURL, "previous_egress": previousEgress, "url": raw, "egress": egress, "index": index})
			return
		}
		http.Error(w, `{"error":"no_unique_alternative_slot"}`, http.StatusConflict)
	case path == "/refresh" && r.Method == http.MethodPost:
		if g.cfg.ProxyListFile == "" {
			http.Error(w, `{"error":"dynamic_proxy_list_disabled"}`, http.StatusConflict)
			return
		}
		if err := g.refreshProxyList(); err != nil {
			http.Error(w, `{"error":"refresh_failed"}`, http.StatusBadGateway)
			return
		}
		g.writeJSON(w, http.StatusOK, g.summary())
	default:
		http.NotFound(w, r)
	}
}

func (g *Gateway) adminAuthorized(r *http.Request) bool {
	return (g.cfg.AdminToken != "" && constantTimeBearer(r, g.cfg.AdminToken)) ||
		(g.cfg.InstanceAdminToken != "" && constantTimeBearer(r, g.cfg.InstanceAdminToken))
}

func (g *Gateway) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (g *Gateway) summary() map[string]any {
	type modelCooldownInfo struct {
		Model   string    `json:"model"`
		Fails   int       `json:"fails"`
		ReadyAt time.Time `json:"ready_at"`
	}
	type slotInfo struct {
		URL            string              `json:"url"`
		Egress         string              `json:"egress,omitempty"`
		ReadyAt        time.Time           `json:"ready_at,omitempty"`
		Fails          int                 `json:"fails"`
		Direct         bool                `json:"direct"`
		Enabled        bool                `json:"enabled"`
		Healthy        bool                `json:"healthy"`
		Active         bool                `json:"active"`
		ModelCooldowns []modelCooldownInfo `json:"model_cooldowns,omitempty"`
	}
	items := make([]slotInfo, 0)
	now := time.Now()
	slots := g.snapshotSlots()
	activeIndex := 0
	if len(slots) > 0 {
		activeIndex = int(g.active.Load() % uint64(len(slots)))
	}
	for index, slot := range slots {
		slot.mu.Lock()
		cooldowns := make([]modelCooldownInfo, 0, len(slot.modelCooldowns))
		for model, state := range slot.modelCooldowns {
			if now.Before(state.Until) {
				cooldowns = append(cooldowns, modelCooldownInfo{Model: model, Fails: state.Fails, ReadyAt: state.Until})
			} else {
				delete(slot.modelCooldowns, model)
			}
		}
		sort.Slice(cooldowns, func(i, j int) bool { return cooldowns[i].Model < cooldowns[j].Model })
		items = append(items, slotInfo{URL: slot.url, Egress: slot.egress, ReadyAt: slot.until, Fails: slot.fails, Direct: slot.url == "", Enabled: !slot.disabled, Healthy: !slot.disabled && (slot.url == "" || slot.egress != ""), Active: index == activeIndex, ModelCooldowns: cooldowns})
		slot.mu.Unlock()
	}
	return map[string]any{
		"slots":           items,
		"stats":           map[string]uint64{"requests": g.stats.Requests.Load(), "success": g.stats.Success.Load(), "upstream429": g.stats.Upstream429.Load(), "gateway429": g.stats.Gateway429.Load(), "errors": g.stats.Errors.Load()},
		"max_concurrency": g.cfg.MaxConcurrency,
	}
}

func (g *Gateway) loadKeys() {
	data, err := os.ReadFile(g.cfg.DataDir + "/keys.json")
	if err != nil {
		return
	}
	var keys []string
	if json.Unmarshal(data, &keys) != nil {
		return
	}
	g.keyMu.Lock()
	defer g.keyMu.Unlock()
	for _, key := range keys {
		if key != "" {
			g.keys[key] = struct{}{}
		}
	}
}

func (g *Gateway) persistKeysLocked() {
	if err := os.MkdirAll(g.cfg.DataDir, 0o700); err != nil {
		return
	}
	keys := make([]string, 0, len(g.keys))
	for key := range g.keys {
		keys = append(keys, key)
	}
	data, _ := json.MarshalIndent(keys, "", "  ")
	_ = os.WriteFile(g.cfg.DataDir+"/keys.json", data, 0o600)
}

func (g *Gateway) addLog(level, message string, fields map[string]any) {
	g.logsMu.Lock()
	g.logs = append(g.logs, systemLog{At: time.Now(), Level: level, Message: message, Fields: fields})
	if len(g.logs) > 500 {
		g.logs = g.logs[len(g.logs)-500:]
	}
	g.logsMu.Unlock()
}

func newGatewayKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "gw_" + hex.EncodeToString(buf), nil
}
func (g *Gateway) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	now := time.Now()
	for _, slot := range g.snapshotSlots() {
		slot.mu.Lock()
		healthy := !slot.disabled && !now.Before(slot.until) && (slot.url == "" || slot.egress != "")
		slot.mu.Unlock()
		if healthy {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"status":"ok"}`)
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, `{"status":"probing","error":"no_healthy_proxy_slot"}`)
}
func (g *Gateway) metrics(w http.ResponseWriter, r *http.Request) {
	if g.cfg.AdminToken != "" && !constantTimeBearer(r, g.cfg.AdminToken) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	w.Header().Set("content-type", "application/json")
	_, _ = io.WriteString(w, `{"requests":`+strconv.FormatUint(g.stats.Requests.Load(), 10)+`,"success":`+strconv.FormatUint(g.stats.Success.Load(), 10)+`,"upstream429":`+strconv.FormatUint(g.stats.Upstream429.Load(), 10)+`,"gateway429":`+strconv.FormatUint(g.stats.Gateway429.Load(), 10)+`,"errors":`+strconv.FormatUint(g.stats.Errors.Load(), 10)+`,"slots":`+strconv.Itoa(len(g.snapshotSlots()))+`}`)
}

func (g *Gateway) serve(w http.ResponseWriter, r *http.Request) {
	g.stats.Requests.Add(1)
	if !g.authorized(r) {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), auditMetadataKey{}, auditMetadata{ClientKey: maskGatewayKey(requestBearer(r))}))
	if allowed, allow := methodAllowed(r.URL.Path, r.Method); !allowed {
		w.Header().Set("Allow", allow)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		payload, _ := json.Marshal(map[string]string{
			"error":  "method_not_allowed",
			"method": r.Method,
			"path":   r.URL.Path,
			"allow":  allow,
		})
		_, _ = w.Write(payload)
		return
	}
	select {
	case g.queue <- struct{}{}:
		defer func() { <-g.queue }()
	default:
		g.stats.Gateway429.Add(1)
		w.Header().Set("Retry-After", "1")
		http.Error(w, `{"error":"gateway_overloaded"}`, http.StatusTooManyRequests)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.cfg.RequestTimeout)
	defer cancel()
	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	case <-ctx.Done():
		g.stats.Gateway429.Add(1)
		http.Error(w, `{"error":"queue_timeout"}`, http.StatusGatewayTimeout)
		return
	}
	if err := g.forward(ctx, w, r); err != nil {
		g.stats.Errors.Add(1)
		g.addLog("error", "forward failed", map[string]any{"error": err.Error(), "path": r.URL.Path})
		if !errors.Is(err, context.Canceled) {
			g.log.Error("forward failed", "error", err)
		}
	}
}

// methodAllowed prevents malformed method/path combinations from being sent
// to the upstream, where they otherwise appear as misleading 404 responses.
func methodAllowed(path, method string) (bool, string) {
	for _, prefix := range []string{"/openai", "/anthropic", "/codex"} {
		path = strings.TrimPrefix(path, prefix)
	}
	switch path {
	case "/v1/chat/completions":
		return method == http.MethodPost, http.MethodPost
	case "/v1/models":
		return method == http.MethodGet, http.MethodGet
	default:
		return true, ""
	}
}

func constantTimeBearer(r *http.Request, expected string) bool {
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	return len(got) == len(expected) && subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
func (g *Gateway) authorized(r *http.Request) bool {
	got := requestBearer(r)
	g.keyMu.RLock()
	defer g.keyMu.RUnlock()
	for key := range g.keys {
		if len(got) == len(key) && subtle.ConstantTimeCompare([]byte(got), []byte(key)) == 1 {
			return true
		}
	}
	return false
}

func requestBearer(r *http.Request) string {
	return strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
}

func maskGatewayKey(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 8 {
		return "key_***"
	}
	return key[:3] + "..." + key[len(key)-4:]
}

func auditMetadataFor(r *http.Request) auditMetadata {
	meta, _ := r.Context().Value(auditMetadataKey{}).(auditMetadata)
	return meta
}

func (g *Gateway) forward(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
	if err != nil {
		http.Error(w, `{"error":"request_body_too_large"}`, http.StatusRequestEntityTooLarge)
		return nil
	}
	path := r.URL.Path
	for _, prefix := range []string{"/openai", "/anthropic", "/codex"} {
		path = strings.TrimPrefix(path, prefix)
	}
	if path == "" {
		path = "/"
	}
	body = g.normalizeRequestBody(path, body)
	model := requestModel(body)
	streaming := streamingRequest(body)
	gatewayRequestID := requestID()
	meta := auditMetadataFor(r)
	meta.Stream = streaming
	meta.RequestID = gatewayRequestID
	r = r.WithContext(context.WithValue(r.Context(), auditMetadataKey{}, meta))
	w.Header().Set("X-Gateway-Request-ID", gatewayRequestID)
	target := strings.TrimRight(g.cfg.UpstreamURL, "/") + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	tried := make(map[string]struct{})
	upstreamRequestID := strings.TrimSpace(r.Header.Get("X-OpenCode-Request"))
	if upstreamRequestID == "" {
		upstreamRequestID = gatewayRequestID
	}
	for attempt := 0; attempt <= g.cfg.MaxRetries; attempt++ {
		slot, err := g.waitForSlotExcluding(ctx, model, tried)
		if err != nil {
			if errors.Is(err, errNoUntriedSlot) {
				http.Error(w, `{"error":"no_untried_upstream"}`, http.StatusBadGateway)
			} else {
				http.Error(w, `{"error":"upstream_cooldown_timeout"}`, http.StatusGatewayTimeout)
			}
			return nil
		}
		tried[slot.identity()] = struct{}{}
		req, err := http.NewRequestWithContext(ctx, r.Method, target, bytes.NewReader(body))
		if err != nil {
			return err
		}
		copyHeaders(req.Header, r.Header)
		req.Header.Set("Authorization", "Bearer "+g.cfg.OpenCodeAPIKey)
		version := g.cfg.OpenCodeVersion
		if version == "" {
			version = opencodeVersion
		}
		referer := g.cfg.OpenCodeReferer
		if referer == "" {
			referer = opencodeReferer
		}
		title := g.cfg.OpenCodeTitle
		if title == "" {
			title = opencodeTitle
		}
		req.Header.Set("User-Agent", "opencode/"+version)
		req.Header.Set("HTTP-Referer", referer)
		req.Header.Set("X-Title", title)
		client := g.cfg.OpenCodeClient
		if client == "" {
			client = opencodeClient
		}
		req.Header.Set("X-OpenCode-Client", client)
		if req.Header.Get("X-OpenCode-Request") == "" {
			req.Header.Set("X-OpenCode-Request", upstreamRequestID)
		}
		req.Header.Del("Host")
		req.Host = ""
		started := time.Now()
		resp, err := slot.client.Do(req)
		if err != nil {
			g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
			slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
			if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
				continue
			}
			http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
			return nil
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			retryAfterHeader := resp.Header.Get("Retry-After")
			retryAfter := parseRetryAfter(retryAfterHeader)
			if resp.StatusCode == 429 {
				g.stats.Upstream429.Add(1)
				slot.cooldownModel(model, g.cfg.CooldownBase, g.cfg.CooldownMax, retryAfter)
			} else {
				slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, retryAfter)
			}
			g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, retryAfterHeader)
			if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
				resp.Body.Close()
				continue
			}
		}
		defer resp.Body.Close()
		if g.cfg.FreeModelsOnly && path == "/v1/models" && resp.StatusCode == http.StatusOK {
			if err := g.copyFilteredModels(w, resp); err != nil {
				g.handleUpstreamBodyFailure(r, slot, model, err)
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
				if errors.Is(err, errUpstreamResponseRead) {
					http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
				}
				return fmt.Errorf("upstream response body: %w", err)
			}
			slot.success(model)
			g.stats.Success.Add(1)
			g.recordAudit(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "")
			return nil
		}
		if !streaming && resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			data, readErr := readBufferedResponse(resp.Body)
			if readErr != nil {
				g.handleUpstreamBodyFailure(r, slot, model, readErr)
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
				if attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) && errors.Is(readErr, errUpstreamResponseRead) {
					resp.Body.Close()
					continue
				}
				http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
				return fmt.Errorf("upstream response body: %w", readErr)
			}
			copyResponseHeaders(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			if _, writeErr := w.Write(data); writeErr != nil {
				return fmt.Errorf("client response write: %w", writeErr)
			}
			slot.success(model)
			if resp.StatusCode >= 200 && resp.StatusCode < 400 {
				g.stats.Success.Add(1)
			}
			g.recordAuditWithUsage(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "", parseTokenUsage(data), 0)
			return nil
		}
		copyResponseHeaders(w.Header(), resp.Header)
		delayStreamCommit := streaming && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
		if !delayStreamCommit {
			w.WriteHeader(resp.StatusCode)
		}
		usage, firstTokenMS, committed, err := copyResponse(w, resp.Body, started, delayStreamCommit)
		if err != nil {
			g.handleUpstreamBodyFailure(r, slot, model, err)
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				g.recordAudit(r, model, http.StatusBadGateway, slot, started, "gateway", attempt+1, "")
			}
			if delayStreamCommit && !committed && attempt < g.cfg.MaxRetries && g.hasUntriedSlot(model, tried) {
				resp.Body.Close()
				continue
			}
			if delayStreamCommit && !committed {
				w.Header().Del("Content-Length")
				w.Header().Del("Content-Encoding")
				http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
			}
			return fmt.Errorf("upstream response body: %w", err)
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			slot.success(model)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			g.stats.Success.Add(1)
		}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			g.recordAuditWithUsage(r, model, resp.StatusCode, slot, started, "upstream", attempt+1, "", usage, firstTokenMS)
		}
		return err
	}
	return nil
}

func (g *Gateway) normalizeRequestBody(path string, body []byte) []byte {
	if !supportsReasoningControls(path) || len(body) == 0 {
		return body
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return body
	}
	changed := false
	model, _ := payload["model"].(string)
	if g.cfg.FreeModelsOnly && model != "" && model != "big-pickle" && !strings.HasSuffix(model, "-free") {
		model += "-free"
		payload["model"] = model
		changed = true
	}
	if g.cfg.DisableThinkingByDefault && isDeepSeekFlashFree(model) {
		_, hasReasoningEffort := payload["reasoning_effort"]
		thinking, hasThinking := payload["thinking"]
		if !hasReasoningEffort && (!hasThinking || thinkingIsDisabled(thinking)) {
			payload["reasoning_effort"] = "none"
			changed = true
		}
		if !hasThinking && !hasReasoningEffort {
			payload["thinking"] = map[string]string{"type": "disabled"}
			changed = true
		}
	}
	if g.cfg.MinThinkingMaxTokens > 0 && isDeepSeekFlashFree(model) && reasoningIsEnabled(payload) {
		if raiseThinkingTokenBudget(path, payload, g.cfg.MinThinkingMaxTokens) {
			changed = true
		}
	}
	if !changed {
		return body
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return encoded
}

func supportsReasoningControls(path string) bool {
	return path == "/v1/chat/completions" || path == "/v1/responses"
}

func thinkingIsDisabled(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		mode, _ := typed["type"].(string)
		return strings.EqualFold(strings.TrimSpace(mode), "disabled")
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "disabled")
	case bool:
		return !typed
	default:
		return false
	}
}

func reasoningIsEnabled(payload map[string]any) bool {
	if value, exists := payload["reasoning_effort"]; exists {
		effort, _ := value.(string)
		effort = strings.ToLower(strings.TrimSpace(effort))
		return effort != "" && effort != "none"
	}
	value, exists := payload["thinking"]
	if !exists {
		return false
	}
	switch typed := value.(type) {
	case map[string]any:
		mode, _ := typed["type"].(string)
		return strings.EqualFold(strings.TrimSpace(mode), "enabled")
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "enabled")
	case bool:
		return typed
	default:
		return false
	}
}

func raiseThinkingTokenBudget(path string, payload map[string]any, minimum int) bool {
	keys := []string{"max_tokens", "max_completion_tokens"}
	defaultKey := "max_tokens"
	if path == "/v1/responses" {
		keys = []string{"max_output_tokens"}
		defaultKey = "max_output_tokens"
	}
	changed := false
	found := false
	for _, key := range keys {
		value, exists := payload[key]
		if !exists {
			continue
		}
		found = true
		if current, ok := value.(float64); ok && current < float64(minimum) {
			payload[key] = minimum
			changed = true
		}
	}
	if !found {
		payload[defaultKey] = minimum
		changed = true
	}
	return changed
}

func isDeepSeekFlashFree(model string) bool {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(model)), "-free") == "deepseek-v4-flash"
}

func requestModel(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return ""
	}
	return strings.TrimSpace(payload.Model)
}

func streamingRequest(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	var payload struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &payload) == nil && payload.Stream
}

func (g *Gateway) handleUpstreamBodyFailure(r *http.Request, slot *proxySlot, model string, err error) {
	if slot == nil || !errors.Is(err, errUpstreamResponseRead) {
		return
	}
	// A truncated upstream response is tied to this connection or exit. Close
	// the pooled connection and cool the slot so the next request uses another
	// healthy candidate instead of immediately repeating the same failure.
	slot.client.CloseIdleConnections()
	slot.cooldown(g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
	if model != "" {
		slot.cooldownModel(model, g.cfg.CooldownBase, g.cfg.CooldownMax, 0)
	}
	failedEgress := ""
	slot.mu.Lock()
	failedEgress = slot.egress
	slot.mu.Unlock()
	if next, _ := g.selectSlotExcluding(model, map[string]struct{}{slot.identity(): {}}); next != nil {
		next.mu.Lock()
		nextEgress := next.egress
		next.mu.Unlock()
		g.addLog("warn", "upstream response truncated; switched exit", map[string]any{"request_id": auditMetadataFor(r).RequestID, "previous_egress": failedEgress, "egress": nextEgress, "model": model})
	}
}

func (g *Gateway) copyFilteredModels(w http.ResponseWriter, resp *http.Response) error {
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
	}
	var payload struct {
		Object string           `json:"object,omitempty"`
		Data   []map[string]any `json:"data"`
	}
	if json.Unmarshal(data, &payload) != nil {
		w.WriteHeader(resp.StatusCode)
		_, err = w.Write(data)
		if err != nil {
			return fmt.Errorf("client response write: %w", err)
		}
		return nil
	}
	filtered := payload.Data[:0]
	for _, model := range payload.Data {
		id, _ := model["id"].(string)
		if strings.HasSuffix(id, "-free") || id == "big-pickle" {
			filtered = append(filtered, model)
		}
	}
	payload.Data = filtered
	copyResponseHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.Header().Del("Content-Encoding")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		return fmt.Errorf("client response write: %w", err)
	}
	return nil
}

func readBufferedResponse(src io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(src, maxBufferedResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
	}
	if len(data) > maxBufferedResponseBytes {
		return nil, fmt.Errorf("upstream response exceeds %d bytes", maxBufferedResponseBytes)
	}
	return data, nil
}

func requestID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(bytes[:])
}

func (g *Gateway) recordAudit(r *http.Request, model string, status int, slot *proxySlot, started time.Time, source string, attempts int, retryAfter string) {
	g.recordAuditWithUsage(r, model, status, slot, started, source, attempts, retryAfter, tokenUsage{}, 0)
}

func (g *Gateway) recordAuditWithUsage(r *http.Request, model string, status int, slot *proxySlot, started time.Time, source string, attempts int, retryAfter string, usage tokenUsage, firstTokenMS int64) {
	slot.mu.Lock()
	egress := slot.egress
	slot.mu.Unlock()
	meta := auditMetadataFor(r)
	record := auditRecord{At: time.Now(), RequestID: meta.RequestID, Method: r.Method, Path: r.URL.Path, Model: model, Status: status, Slot: slot.url, Egress: egress, LatencyMS: time.Since(started).Milliseconds(), Source: source, Attempts: attempts, RetryAfter: retryAfter, ClientKey: meta.ClientKey, Stream: meta.Stream, PromptTokens: usage.PromptTokens, CompletionTokens: usage.CompletionTokens, TotalTokens: usage.TotalTokens, CachedTokens: usage.CachedTokens, FirstTokenMS: firstTokenMS}
	g.auditMu.Lock()
	g.audits = append(g.audits, record)
	if len(g.audits) > 500 {
		g.audits = g.audits[len(g.audits)-500:]
	}
	g.auditMu.Unlock()
}

func (g *Gateway) selectSlot() (*proxySlot, time.Duration) {
	return g.selectSlotExcluding("", nil)
}

func (g *Gateway) selectSlotExcluding(model string, excluded map[string]struct{}) (*proxySlot, time.Duration) {
	now := time.Now()
	slots := g.snapshotSlots()
	if len(slots) == 0 {
		return nil, time.Second
	}
	start := int(g.active.Load() % uint64(len(slots)))
	var earliest time.Time
	for i := 0; i < len(slots); i++ {
		index := (start + i) % len(slots)
		s := slots[index]
		if _, tried := excluded[s.identity()]; tried {
			continue
		}
		disabled, ready := s.readiness(model, now)
		if disabled {
			continue
		}
		if !now.Before(ready) {
			g.active.Store(uint64(index))
			return s, 0
		}
		if earliest.IsZero() || ready.Before(earliest) {
			earliest = ready
		}
	}
	if earliest.IsZero() {
		return nil, -1
	}
	return nil, time.Until(earliest)
}
func (g *Gateway) waitForSlot(ctx context.Context) (*proxySlot, error) {
	for {
		slot, wait := g.selectSlot()
		if slot != nil {
			return slot, nil
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func (g *Gateway) waitForSlotExcluding(ctx context.Context, model string, excluded map[string]struct{}) (*proxySlot, error) {
	for {
		slot, wait := g.selectSlotExcluding(model, excluded)
		if slot != nil {
			return slot, nil
		}
		if wait < 0 {
			return nil, errNoUntriedSlot
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func (g *Gateway) hasUntriedSlot(model string, excluded map[string]struct{}) bool {
	now := time.Now()
	for _, slot := range g.snapshotSlots() {
		disabled, ready := slot.readiness(model, now)
		if disabled || now.Before(ready) {
			continue
		}
		if _, tried := excluded[slot.identity()]; !tried {
			return true
		}
	}
	return false
}
func copyHeaders(dst, src http.Header) {
	for _, key := range []string{"content-type", "accept", "x-opencode-client", "x-opencode-session", "x-opencode-project", "x-opencode-request", "anthropic-version", "anthropic-beta"} {
		if v := src.Values(key); len(v) > 0 {
			dst.Del(key)
			for _, item := range v {
				dst.Add(key, item)
			}
		}
	}
}
func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		switch strings.ToLower(key) {
		case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
func copyResponse(w http.ResponseWriter, src io.Reader, started time.Time, delayUntilFirstOutput bool) (tokenUsage, int64, bool, error) {
	reader := bufio.NewReaderSize(src, 32<<10)
	flusher, _ := w.(http.Flusher)
	var usage tokenUsage
	var firstTokenMS int64
	seenFinishReason := false
	var pending strings.Builder
	committed := false
	write := func(data string) error {
		if _, writeErr := io.WriteString(w, data); writeErr != nil {
			return fmt.Errorf("client response write: %w", writeErr)
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			usage = mergeTokenUsage(usage, parseSSETokenUsage(line))
			hasOutput := sseLineHasOutput(line)
			if sseLineHasFinishReason(line) {
				seenFinishReason = true
			}
			if sseLineIsDone(line) && !seenFinishReason {
				// Some OpenAI-compatible upstreams emit [DONE] without a final
				// choice carrying finish_reason. Preserve the declared completion
				// while giving strict clients a standards-compatible terminal chunk.
				line = `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n" + line
				seenFinishReason = true
			}
			if delayUntilFirstOutput && !committed {
				pending.WriteString(line)
				if hasOutput {
					firstTokenMS = time.Since(started).Milliseconds()
					if writeErr := write(pending.String()); writeErr != nil {
						return usage, firstTokenMS, false, writeErr
					}
					pending.Reset()
					committed = true
				}
			} else {
				if firstTokenMS == 0 && hasOutput {
					firstTokenMS = time.Since(started).Milliseconds()
				}
				if writeErr := write(line); writeErr != nil {
					return usage, firstTokenMS, committed, writeErr
				}
				committed = true
			}
		}
		if err == io.EOF {
			if delayUntilFirstOutput && !committed {
				return usage, firstTokenMS, false, fmt.Errorf("%w: %w", errUpstreamResponseRead, errUpstreamStreamEmpty)
			}
			return usage, firstTokenMS, committed, nil
		}
		if err != nil {
			return usage, firstTokenMS, committed, fmt.Errorf("%w: %w", errUpstreamResponseRead, err)
		}
	}
}

func sseLineIsDone(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasPrefix(line, "data:") && strings.TrimSpace(strings.TrimPrefix(line, "data:")) == "[DONE]"
}

func sseLineHasFinishReason(line string) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return false
	}
	var payload struct {
		Choices []struct {
			FinishReason json.RawMessage `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &payload) != nil {
		return false
	}
	for _, choice := range payload.Choices {
		if value := strings.TrimSpace(string(choice.FinishReason)); value != "" && value != "null" && value != `""` {
			return true
		}
	}
	return false
}

func sseLineHasOutput(line string) bool {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return false
	}
	var payload struct {
		Type    string `json:"type"`
		Delta   string `json:"delta"`
		Choices []struct {
			Delta struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &payload) != nil {
		return false
	}
	if payload.Type == "response.output_text.delta" && payload.Delta != "" {
		return true
	}
	for _, choice := range payload.Choices {
		if choice.Delta.Content != "" || choice.Delta.ReasoningContent != "" {
			return true
		}
	}
	return false
}

func parseSSETokenUsage(line string) tokenUsage {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "data:") {
		return tokenUsage{}
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if payload == "" || payload == "[DONE]" {
		return tokenUsage{}
	}
	return parseTokenUsage([]byte(payload))
}

func parseTokenUsage(data []byte) tokenUsage {
	type usagePayload struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
		InputTokens      int64 `json:"input_tokens"`
		OutputTokens     int64 `json:"output_tokens"`
		TotalTokens      int64 `json:"total_tokens"`
		PromptDetails    struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
		InputDetails struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	}
	var payload struct {
		Usage    usagePayload `json:"usage"`
		Response struct {
			Usage usagePayload `json:"usage"`
		} `json:"response"`
	}
	if json.Unmarshal(data, &payload) != nil {
		return tokenUsage{}
	}
	usage := payload.Usage
	if usage.PromptTokens == 0 && usage.CompletionTokens == 0 && usage.InputTokens == 0 && usage.OutputTokens == 0 && usage.TotalTokens == 0 {
		usage = payload.Response.Usage
	}
	promptTokens := usage.PromptTokens
	if promptTokens == 0 {
		promptTokens = usage.InputTokens
	}
	completionTokens := usage.CompletionTokens
	if completionTokens == 0 {
		completionTokens = usage.OutputTokens
	}
	cachedTokens := usage.PromptDetails.CachedTokens
	if cachedTokens == 0 {
		cachedTokens = usage.InputDetails.CachedTokens
	}
	totalTokens := usage.TotalTokens
	if totalTokens == 0 && (promptTokens != 0 || completionTokens != 0) {
		totalTokens = promptTokens + completionTokens
	}
	return tokenUsage{PromptTokens: promptTokens, CompletionTokens: completionTokens, TotalTokens: totalTokens, CachedTokens: cachedTokens}
}

func mergeTokenUsage(current, next tokenUsage) tokenUsage {
	if next.PromptTokens != 0 || next.CompletionTokens != 0 || next.TotalTokens != 0 || next.CachedTokens != 0 {
		return next
	}
	return current
}
func parseRetryAfter(v string) time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
