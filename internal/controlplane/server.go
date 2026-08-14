package controlplane

import (
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

//go:embed web/index.html web/app.js web/style.css
var assets embed.FS

const defaultUpstreamURL = "https://opencode.ai/zen"

const (
	zenAuthModePublic = "public"
	zenAuthModeCustom = "custom"
)

type Config struct {
	ListenAddr      string
	APIListenAddr   string
	AdminToken      string
	InstanceToken   string
	Instances       []Instance
	DataDir         string
	DockerSocket    string
	DockerNetwork   string
	GatewayImage    string
	DirectFallback  bool
	MihomoContainer string
	MihomoConfigDir string
	MihomoAPIURL    string
	MihomoMaxSlots  int
	MaxInstances    int
	BootstrapKeys   []string
}

type Instance struct {
	Name           string   `json:"name"`
	URL            string   `json:"url"`
	Container      string   `json:"container"`
	ContainerID    string   `json:"container_id,omitempty"`
	Managed        bool     `json:"managed"`
	Status         string   `json:"status"`
	ProxyURLs      []string `json:"proxy_urls,omitempty"`
	MaxConcurrency int      `json:"max_concurrency,omitempty"`
	QueueSize      int      `json:"queue_size,omitempty"`
}
type Server struct {
	cfg              Config
	client           *http.Client
	mu               sync.RWMutex
	lifecycleMu      sync.Mutex
	rotationMu       sync.Mutex
	keys             []string
	zenKeys          map[string]string
	instances        []Instance
	rotationLogs     []SystemLog
	rotationFailures map[string]string
	lastUpstream429  map[string]uint64
	docker           *dockerClient
	apiMu            sync.Mutex
	apiInflight      map[string]int
	apiCursor        atomic.Uint64
}
type AuditRecord struct {
	At         time.Time `json:"at"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Model      string    `json:"model,omitempty"`
	Status     int       `json:"status"`
	Slot       string    `json:"slot"`
	Egress     string    `json:"egress,omitempty"`
	LatencyMS  int64     `json:"latency_ms"`
	Source     string    `json:"source"`
	Attempts   int       `json:"attempts"`
	RetryAfter string    `json:"retry_after,omitempty"`
	Instance   string    `json:"instance"`
}
type SystemLog struct {
	At       time.Time      `json:"at"`
	Level    string         `json:"level"`
	Message  string         `json:"message"`
	Fields   map[string]any `json:"fields,omitempty"`
	Instance string         `json:"instance"`
}
type LogSource struct {
	ID      string           `json:"id"`
	Label   string           `json:"label"`
	Entries []map[string]any `json:"entries"`
}
type Summary struct {
	Slots          []map[string]any  `json:"slots"`
	Stats          map[string]uint64 `json:"stats"`
	MaxConcurrency int               `json:"max_concurrency"`
	Instance       string            `json:"instance"`
	Online         bool              `json:"online"`
	Error          string            `json:"error,omitempty"`
	Status         string            `json:"status"`
	Managed        bool              `json:"managed"`
	Container      string            `json:"container"`
	ContainerID    string            `json:"container_id,omitempty"`
	ZenKeyMasked   string            `json:"zen_api_key_masked"`
	ZenKeySet      bool              `json:"zen_api_key_configured"`
	ZenAuthMode    string            `json:"zen_auth_mode"`
	ProxyURLs      []string          `json:"proxy_urls"`
	QueueSize      int               `json:"queue_size"`
	InTrafficPool  bool              `json:"in_traffic_pool"`
	InstanceURL    string            `json:"-"`
}

type instanceRequest struct {
	Name           string   `json:"name"`
	ZenAPIKey      string   `json:"zen_api_key"`
	ZenAuthMode    string   `json:"auth_mode"`
	ProxyURLs      []string `json:"proxy_urls"`
	MaxConcurrency int      `json:"max_concurrency"`
	QueueSize      int      `json:"queue_size"`
}

func LoadConfig() (Config, error) {
	c := Config{ListenAddr: env("CONTROL_LISTEN_ADDR", "0.0.0.0:13338"), APIListenAddr: env("API_LISTEN_ADDR", "0.0.0.0:13337"), AdminToken: os.Getenv("ADMIN_TOKEN"), InstanceToken: os.Getenv("INSTANCE_ADMIN_TOKEN"), DataDir: env("CONTROL_DATA_DIR", "/control-data"), DockerSocket: env("DOCKER_SOCKET", "/var/run/docker.sock"), DockerNetwork: env("GATEWAY_NETWORK", "opencode-gateway-next_default"), GatewayImage: env("GATEWAY_IMAGE", "opencode-gateway-next-gateway:latest"), DirectFallback: compatibleBoolEnv("DIRECT_FALLBACK", "NGINX_DIRECT_FALLBACK", false), MihomoContainer: env("MIHOMO_CONTAINER", "opencode-gateway-mihomo"), MihomoConfigDir: env("MIHOMO_CONFIG_DIR", "/mihomo-config"), MihomoAPIURL: strings.TrimRight(env("MIHOMO_API_URL", "http://mihomo:9090"), "/"), MihomoMaxSlots: positiveIntEnv("MIHOMO_MAX_SLOTS", 64), MaxInstances: positiveIntEnv("MAX_INSTANCES", 16), BootstrapKeys: split(os.Getenv("GATEWAY_KEYS"))}
	if c.MihomoMaxSlots > 128 {
		c.MihomoMaxSlots = 128
	}
	if c.InstanceToken == "" {
		c.InstanceToken = c.AdminToken
	}
	for i, raw := range split(os.Getenv("GATEWAY_INSTANCES")) {
		parts := strings.SplitN(raw, "=", 2)
		instance := Instance{Name: fmt.Sprintf("gateway-%d", i+1), URL: raw}
		if len(parts) == 2 {
			instance.Name, instance.URL = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
		u, err := url.Parse(instance.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return c, fmt.Errorf("invalid GATEWAY_INSTANCES entry %q", raw)
		}
		instance.URL = strings.TrimRight(instance.URL, "/")
		c.Instances = append(c.Instances, instance)
	}
	if c.AdminToken == "" || c.InstanceToken == "" {
		return c, errors.New("ADMIN_TOKEN and INSTANCE_ADMIN_TOKEN are required")
	}
	return c, nil
}

func New(cfg Config) *Server {
	s := &Server{cfg: cfg, client: &http.Client{Timeout: 15 * time.Second}, docker: newDockerClient(cfg.DockerSocket), apiInflight: make(map[string]int), zenKeys: make(map[string]string), rotationFailures: make(map[string]string), lastUpstream429: make(map[string]uint64)}
	s.loadKeys()
	s.loadZenKeys()
	if len(s.keys) == 0 {
		s.keys = append([]string(nil), cfg.BootstrapKeys...)
		s.persistLocked()
	}
	s.loadInstances()
	if len(s.instances) == 0 {
		s.instances = cfg.Instances
	}
	return s
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.page)
	mux.HandleFunc("/static/", s.static)
	mux.HandleFunc("/api/", s.api)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, `{"status":"ok"}`) })
	return mux
}

// APIHandler is the public data-plane listener. It validates the client key
// before routing so instance-specific keys cannot reach another instance.
func (s *Server) APIHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.proxyAPI)
	for _, prefix := range []string{"/v1/", "/openai/", "/anthropic/", "/codex/"} {
		mux.HandleFunc(prefix, s.proxyAPI)
	}
	return mux
}

func (s *Server) proxyAPI(w http.ResponseWriter, r *http.Request) {
	apiKey := requestBearer(r)
	s.mu.RLock()
	instances := append([]Instance(nil), s.instances...)
	shared := containsKey(s.keys, apiKey)
	s.mu.RUnlock()
	if apiKey == "" || !shared {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	selected := selectTrafficPool(instances, s.cfg.DirectFallback)
	if len(selected) == 0 {
		http.Error(w, `{"error":"no_gateway_instances"}`, http.StatusServiceUnavailable)
		return
	}
	instance := s.acquireAPIInstance(selected)
	target, err := url.Parse(instance.URL)
	if err != nil || target.Host == "" {
		http.Error(w, `{"error":"invalid_gateway_instance"}`, http.StatusBadGateway)
		return
	}
	defer s.releaseAPIInstance(instance.Name)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
		http.Error(w, `{"error":"gateway_unavailable"}`, http.StatusBadGateway)
	}
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = target.Host
	}
	proxy.ServeHTTP(w, r)
}

func (s *Server) acquireAPIInstance(instances []Instance) Instance {
	s.apiMu.Lock()
	defer s.apiMu.Unlock()
	var selected Instance
	selectedLoad := int(^uint(0) >> 1)
	start := int(s.apiCursor.Add(1)-1) % len(instances)
	for i := range instances {
		instance := instances[(start+i)%len(instances)]
		load := s.apiInflight[instance.Name]
		if load < selectedLoad {
			selected, selectedLoad = instance, load
		}
	}
	s.apiInflight[selected.Name]++
	return selected
}

func (s *Server) releaseAPIInstance(name string) {
	s.apiMu.Lock()
	defer s.apiMu.Unlock()
	if s.apiInflight[name] > 1 {
		s.apiInflight[name]--
	} else {
		delete(s.apiInflight, name)
	}
}
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data, _ := assets.ReadFile("web/index.html")
	w.Header().Set("content-type", "text/html; charset=utf-8")
	w.Write(data)
}
func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/static/")
	data, err := assets.ReadFile("web/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	if strings.HasSuffix(name, ".js") {
		w.Header().Set("content-type", "application/javascript; charset=utf-8")
	} else if strings.HasSuffix(name, ".css") {
		w.Header().Set("content-type", "text/css; charset=utf-8")
	}
	// Asset URLs are versioned in the page; prevent an intermediary from
	// keeping an unversioned stylesheet or script stale after an upgrade.
	w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	w.Write(data)
}
func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	if !bearer(r, s.cfg.AdminToken) {
		http.Error(w, `{"error":"unauthorized"}`, 401)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api")
	switch {
	case path == "/overview" && r.Method == "GET":
		s.overview(w)
	case path == "/keys" && r.Method == "GET":
		s.mu.RLock()
		keys := append([]string(nil), s.keys...)
		s.mu.RUnlock()
		writeJSON(w, 200, map[string]any{"keys": keys})
	case path == "/keys" && r.Method == "POST":
		s.createKey(w)
	case path == "/mihomo" && r.Method == "GET":
		s.mihomoStatus(w)
	case path == "/mihomo/probe" && r.Method == "POST":
		s.mihomoProbe(w)
	case path == "/mihomo" && r.Method == "PUT":
		s.mihomoUpdate(w, r)
	case strings.HasPrefix(path, "/keys/") && r.Method == "DELETE":
		s.deleteKey(w, strings.TrimPrefix(path, "/keys/"))
	case path == "/slots" && r.Method == "POST":
		s.broadcast(w, r, "/admin/slots")
	case path == "/refresh" && r.Method == "POST":
		s.broadcast(w, r, "/admin/refresh")
	case path == "/instances" && r.Method == "GET":
		s.instancesList(w)
	case path == "/instances" && r.Method == "POST":
		s.instanceCreate(w, r)
	case strings.HasPrefix(path, "/instances/") && r.Method == "PUT":
		s.instanceUpdate(w, r, strings.TrimPrefix(path, "/instances/"))
	case strings.HasPrefix(path, "/instances/") && r.Method == "POST":
		s.instanceAction(w, r, strings.TrimPrefix(path, "/instances/"))
	case strings.HasPrefix(path, "/instances/") && r.Method == "DELETE":
		s.instanceDelete(w, strings.TrimPrefix(path, "/instances/"))
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) instancesList(w http.ResponseWriter) {
	instances, err := s.discoverInstances()
	if err != nil {
		http.Error(w, `{"error":"docker_unavailable"}`, http.StatusBadGateway)
		return
	}
	s.mu.Lock()
	s.instances = instances
	s.persistInstancesLocked()
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"instances": instances})
}

func (s *Server) instanceCreate(w http.ResponseWriter, r *http.Request) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	var request instanceRequest
	if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	if code, message := validateInstanceRequest(&request, true); code != 0 {
		http.Error(w, message, code)
		return
	}
	instances, err := s.discoverInstances()
	if err != nil {
		http.Error(w, `{"error":"docker_unavailable"}`, http.StatusBadGateway)
		return
	}
	request.Name = normalizeInstanceName(request.Name)
	if request.Name == "" {
		request.Name = nextInstanceName(instances)
	}
	if !instanceNamePattern.MatchString(request.Name) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	for _, instance := range instances {
		if instance.Name == request.Name {
			http.Error(w, `{"error":"instance_exists"}`, http.StatusConflict)
			return
		}
	}
	if len(instances) >= s.cfg.MaxInstances {
		http.Error(w, `{"error":"max_instances_reached"}`, http.StatusConflict)
		return
	}
	instance := Instance{Name: request.Name, Container: request.Name, URL: "http://" + request.Name + ":13339", Managed: true, Status: "created", ProxyURLs: request.ProxyURLs, MaxConcurrency: request.MaxConcurrency, QueueSize: request.QueueSize}
	s.mu.RLock()
	keys := append([]string(nil), s.keys...)
	s.mu.RUnlock()
	if err := s.docker.create(s.cfg, instance, keys, request.ZenAPIKey); err != nil {
		writeAPIError(w, http.StatusBadGateway, "create_failed", err)
		return
	}
	if err := s.docker.action(request.Name, "start"); err != nil {
		_ = s.docker.remove(request.Name)
		writeAPIError(w, http.StatusBadGateway, "start_failed", err)
		return
	}
	if err := s.docker.waitHealthy(request.Name); err != nil {
		_ = s.docker.remove(request.Name)
		writeAPIError(w, http.StatusBadGateway, "health_check_failed", err)
		return
	}
	if err := s.refreshInstances(); err != nil {
		_ = s.docker.remove(request.Name)
		_ = s.refreshInstances()
		writeAPIError(w, http.StatusBadGateway, "route_refresh_failed", err)
		return
	}
	instances, err = s.discoverInstances()
	if err != nil {
		_ = s.docker.remove(request.Name)
		_ = s.refreshInstances()
		writeAPIError(w, http.StatusBadGateway, "docker_verify_failed", err)
		return
	}
	var created Instance
	for _, current := range instances {
		if current.Name == request.Name {
			created = current
			break
		}
	}
	if created.ContainerID == "" || created.Status != "running" {
		_ = s.docker.remove(request.Name)
		_ = s.refreshInstances()
		writeAPIError(w, http.StatusBadGateway, "container_not_running", fmt.Errorf("container %q has status %q", request.Name, created.Status))
		return
	}
	s.mu.Lock()
	s.instances = instances
	s.zenKeys[request.Name] = request.ZenAPIKey
	s.persistInstancesLocked()
	s.persistZenKeysLocked()
	s.mu.Unlock()
	s.addRotationLog("info", "instance created", request.Name, map[string]any{"proxy_slots": len(request.ProxyURLs), "max_concurrency": request.MaxConcurrency})
	go func() { _ = s.ReconcileEgresses() }()
	writeJSON(w, http.StatusCreated, map[string]any{"instance": created})
}

func (s *Server) instanceUpdate(w http.ResponseWriter, r *http.Request, name string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if !instanceNamePattern.MatchString(name) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	var request instanceRequest
	if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&request) != nil {
		http.Error(w, `{"error":"invalid_body"}`, http.StatusBadRequest)
		return
	}
	request.Name = name
	s.mu.RLock()
	currentKey := s.zenKeys[name]
	s.mu.RUnlock()
	if strings.TrimSpace(request.ZenAuthMode) == "" && strings.TrimSpace(request.ZenAPIKey) == "" {
		request.ZenAuthMode, request.ZenAPIKey = zenAuthModeForKey(currentKey), currentKey
		if request.ZenAuthMode == "" {
			request.ZenAuthMode = zenAuthModePublic
		}
	}
	if request.ZenAuthMode == zenAuthModeCustom && strings.TrimSpace(request.ZenAPIKey) == "" && currentKey != "" && currentKey != zenAuthModePublic {
		request.ZenAPIKey = currentKey
	}
	if code, message := validateInstanceRequest(&request, false); code != 0 {
		http.Error(w, message, code)
		return
	}
	if request.ZenAuthMode == zenAuthModePublic {
		request.ZenAPIKey = zenAuthModePublic
	}
	s.mu.RLock()
	keys := append([]string(nil), s.keys...)
	s.mu.RUnlock()
	instance := Instance{Name: name, Container: name, URL: "http://" + name + ":13339", Managed: true, ProxyURLs: request.ProxyURLs, MaxConcurrency: request.MaxConcurrency, QueueSize: request.QueueSize}
	if err := s.docker.replace(s.cfg, instance, keys, request.ZenAPIKey); err != nil {
		http.Error(w, `{"error":"update_failed"}`, http.StatusBadGateway)
		return
	}
	if err := s.refreshInstances(); err != nil {
		http.Error(w, `{"error":"route_refresh_failed"}`, http.StatusBadGateway)
		return
	}
	s.mu.Lock()
	s.zenKeys[name] = request.ZenAPIKey
	s.persistZenKeysLocked()
	s.mu.Unlock()
	s.addRotationLog("info", "instance settings updated", name, map[string]any{"proxy_slots": len(request.ProxyURLs), "max_concurrency": request.MaxConcurrency})
	go func() { _ = s.ReconcileEgresses() }()
	writeJSON(w, http.StatusOK, map[string]any{"instance": instance})
}

func validateInstanceRequest(request *instanceRequest, defaultLimits bool) (int, string) {
	request.ZenAuthMode = strings.ToLower(strings.TrimSpace(request.ZenAuthMode))
	request.ZenAPIKey = strings.TrimSpace(request.ZenAPIKey)
	if request.ZenAuthMode == "" {
		if request.ZenAPIKey != "" {
			request.ZenAuthMode = zenAuthModeCustom
		} else if defaultLimits {
			request.ZenAuthMode = zenAuthModePublic
		}
	}
	if request.ZenAuthMode != "" && request.ZenAuthMode != zenAuthModePublic && request.ZenAuthMode != zenAuthModeCustom {
		return http.StatusBadRequest, `{"error":"invalid_auth_mode"}`
	}
	if request.ZenAuthMode == zenAuthModePublic {
		request.ZenAPIKey = zenAuthModePublic
	}
	if request.ZenAuthMode == zenAuthModeCustom && request.ZenAPIKey == "" {
		return http.StatusBadRequest, `{"error":"zen_api_key_required"}`
	}
	if request.ZenAPIKey != "" && (len(request.ZenAPIKey) > 512 || strings.IndexFunc(request.ZenAPIKey, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0) {
		return http.StatusBadRequest, `{"error":"invalid_zen_api_key"}`
	}
	if request.ZenAuthMode == zenAuthModeCustom && request.ZenAPIKey == zenAuthModePublic {
		return http.StatusBadRequest, `{"error":"custom_key_cannot_be_public"}`
	}
	if defaultLimits && request.MaxConcurrency == 0 {
		request.MaxConcurrency = 4
	}
	if defaultLimits && request.QueueSize == 0 {
		request.QueueSize = 8
	}
	if request.MaxConcurrency < 1 || request.MaxConcurrency > 64 || request.QueueSize < 0 || request.QueueSize > 256 {
		return http.StatusBadRequest, `{"error":"invalid_limits"}`
	}
	for i, raw := range request.ProxyURLs {
		request.ProxyURLs[i] = strings.TrimSpace(raw)
		parsed, err := url.Parse(request.ProxyURLs[i])
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "socks5" && parsed.Scheme != "socks5h") {
			return http.StatusBadRequest, `{"error":"proxy_urls_must_be_http_or_socks5"}`
		}
	}
	return 0, ""
}

func normalizeInstanceName(raw string) string {
	name := strings.ToLower(strings.TrimSpace(raw))
	name = strings.NewReplacer("_", "-", " ", "-").Replace(name)
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	name = strings.Trim(name, "-")
	if name == "" {
		return ""
	}
	if !strings.HasPrefix(name, "gateway-") {
		name = "gateway-" + name
	}
	return name
}

func nextInstanceName(instances []Instance) string {
	used := make(map[string]struct{}, len(instances))
	for _, instance := range instances {
		used[instance.Name] = struct{}{}
	}
	for suffix := 'a'; suffix <= 'z'; suffix++ {
		candidate := "gateway-" + string(suffix)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
	for number := 2; ; number++ {
		candidate := "gateway-" + strconv.Itoa(number)
		if _, exists := used[candidate]; !exists {
			return candidate
		}
	}
}

func (s *Server) instanceAction(w http.ResponseWriter, r *http.Request, name string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	parts := strings.Split(strings.Trim(name, "/"), "/")
	if len(parts) != 2 || !instanceNamePattern.MatchString(parts[0]) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	name, action := parts[0], parts[1]
	if action != "start" && action != "stop" && action != "restart" && action != "rotate" {
		http.NotFound(w, r)
		return
	}
	if action == "rotate" {
		result, err := s.RotateInstanceEgress(name)
		if err != nil {
			writeAPIError(w, http.StatusConflict, "egress_rotation_failed", err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if err := s.docker.action(name, action); err != nil {
		http.Error(w, `{"error":"action_failed"}`, http.StatusBadGateway)
		return
	}
	if action == "start" || action == "restart" {
		s.mu.RLock()
		keys := append([]string(nil), s.keys...)
		s.mu.RUnlock()
		payload, _ := json.Marshal(map[string]any{"keys": keys})
		instance := Instance{Name: name, URL: "http://" + name + ":13339"}
		var syncErr error
		for attempt := 0; attempt < 20; attempt++ {
			syncErr = s.call(instance, http.MethodPut, "/admin/keys", strings.NewReader(string(payload)), nil)
			if syncErr == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if syncErr != nil {
			http.Error(w, `{"error":"key_resync_failed"}`, http.StatusBadGateway)
			return
		}
	}
	if err := s.refreshInstances(); err != nil {
		http.Error(w, `{"error":"route_refresh_failed"}`, http.StatusBadGateway)
		return
	}
	if action == "start" || action == "restart" {
		go func() { _ = s.ReconcileEgresses() }()
	}
	s.addRotationLog("info", "instance action completed", name, map[string]any{"action": action})
	writeJSON(w, http.StatusOK, map[string]string{"name": name, "action": action})
}

func (s *Server) instanceDelete(w http.ResponseWriter, name string) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if !instanceNamePattern.MatchString(name) {
		http.Error(w, `{"error":"invalid_instance_name"}`, http.StatusBadRequest)
		return
	}
	if err := s.docker.remove(name); err != nil {
		http.Error(w, `{"error":"delete_failed"}`, http.StatusBadGateway)
		return
	}
	_ = s.docker.removeDataVolume(name)
	s.mu.Lock()
	delete(s.zenKeys, name)
	s.persistZenKeysLocked()
	s.mu.Unlock()
	if err := s.refreshInstances(); err != nil {
		http.Error(w, `{"error":"route_refresh_failed"}`, http.StatusBadGateway)
		return
	}
	s.addRotationLog("info", "instance deleted", name, nil)
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) overview(w http.ResponseWriter) {
	instances, err := s.discoverInstances()
	if err == nil {
		s.mu.Lock()
		s.instances = instances
		s.persistInstancesLocked()
		s.mu.Unlock()
	} else {
		s.mu.RLock()
		instances = append([]Instance(nil), s.instances...)
		s.mu.RUnlock()
	}
	var wg sync.WaitGroup
	summaries := make([]Summary, len(instances))
	audits := make([][]AuditRecord, len(instances))
	logs := make([][]SystemLog, len(instances))
	containerLogs := make([][]SystemLog, len(instances))
	for i, instance := range instances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			summaries[i] = s.summary(instance)
			if instance.Status != "" && instance.Status != "running" {
				return
			}
			audits[i] = s.audit(instance)
			logs[i] = s.logs(instance)
			containerLogs[i], _ = s.docker.logs(instance.Container, instance.Name, 100)
		}()
	}
	wg.Wait()
	pooled := make(map[string]struct{})
	for _, instance := range selectTrafficPool(instances, s.cfg.DirectFallback) {
		pooled[instance.Name] = struct{}{}
	}
	for i := range summaries {
		_, summaries[i].InTrafficPool = pooled[summaries[i].Instance]
	}
	totals := map[string]uint64{}
	totalConcurrency := 0
	var records []AuditRecord
	var systemLogs []SystemLog
	for _, sum := range summaries {
		if sum.Online && sum.InTrafficPool {
			totalConcurrency += sum.MaxConcurrency
			for k, v := range sum.Stats {
				totals[k] += v
			}
		}
	}
	for _, list := range audits {
		records = append(records, list...)
	}
	for _, list := range logs {
		systemLogs = append(systemLogs, list...)
	}
	s.mu.RLock()
	controlLogs := append([]SystemLog(nil), s.rotationLogs...)
	s.mu.RUnlock()
	systemLogs = append(systemLogs, controlLogs...)
	sort.Slice(systemLogs, func(i, j int) bool { return systemLogs[i].At.Before(systemLogs[j].At) })
	if len(systemLogs) > 500 {
		systemLogs = systemLogs[len(systemLogs)-500:]
	}
	mihomoLogs, _ := s.docker.logs(s.cfg.MihomoContainer, "mihomo", 200)
	logSources := buildLogSources(instances, audits, logs, containerLogs, controlLogs, mihomoLogs)
	sort.Slice(records, func(i, j int) bool { return records[i].At.Before(records[j].At) })
	if len(records) > 500 {
		records = records[len(records)-500:]
	}
	writeJSON(w, 200, map[string]any{"instances": summaries, "stats": totals, "max_concurrency": totalConcurrency, "records": records, "logs": systemLogs, "log_sources": logSources})
}

func buildLogSources(instances []Instance, audits [][]AuditRecord, gatewayLogs, containerLogs [][]SystemLog, controlLogs, mihomoLogs []SystemLog) []LogSource {
	sources := []LogSource{
		{ID: "control", Label: "控制面板", Entries: systemLogEntries(controlLogs, "control")},
		{ID: "mihomo", Label: "Mihomo", Entries: systemLogEntries(mihomoLogs, "container")},
	}
	for index, instance := range instances {
		entries := make([]map[string]any, 0, len(audits[index])+len(gatewayLogs[index])+len(containerLogs[index]))
		for _, record := range audits[index] {
			entries = append(entries, map[string]any{
				"kind": "audit", "at": record.At, "status": record.Status, "method": record.Method,
				"path": record.Path, "model": record.Model, "egress": record.Egress, "source": record.Source,
				"attempts": record.Attempts, "latency_ms": record.LatencyMS, "message": record.Path,
			})
		}
		entries = append(entries, systemLogEntries(gatewayLogs[index], "gateway")...)
		entries = append(entries, systemLogEntries(containerLogs[index], "container")...)
		sort.Slice(entries, func(i, j int) bool { return logEntryTime(entries[i]).Before(logEntryTime(entries[j])) })
		if len(entries) > 300 {
			entries = entries[len(entries)-300:]
		}
		sources = append(sources, LogSource{ID: instance.Name, Label: instance.Name, Entries: entries})
	}
	return sources
}

func systemLogEntries(logs []SystemLog, kind string) []map[string]any {
	entries := make([]map[string]any, 0, len(logs))
	for _, log := range logs {
		entries = append(entries, map[string]any{"kind": kind, "at": log.At, "level": log.Level, "message": log.Message, "fields": log.Fields})
	}
	return entries
}

func logEntryTime(entry map[string]any) time.Time {
	switch value := entry["at"].(type) {
	case time.Time:
		return value
	case string:
		parsed, _ := time.Parse(time.RFC3339Nano, value)
		return parsed
	default:
		return time.Time{}
	}
}
func (s *Server) summary(instance Instance) Summary {
	var out Summary
	out.Instance = instance.Name
	out.InstanceURL = instance.URL
	out.Status = instance.Status
	out.Managed = instance.Managed
	out.Container = instance.Container
	out.ContainerID = instance.ContainerID
	s.mu.RLock()
	zenKey := s.zenKeys[instance.Name]
	out.ZenKeyMasked = maskAPIKey(zenKey)
	out.ZenKeySet = zenKey != ""
	out.ZenAuthMode = zenAuthModeForKey(zenKey)
	if out.ZenAuthMode == zenAuthModePublic {
		out.ZenKeyMasked = "public（公共 Key）"
	}
	s.mu.RUnlock()
	out.MaxConcurrency = instance.MaxConcurrency
	out.ProxyURLs = append([]string(nil), instance.ProxyURLs...)
	out.QueueSize = instance.QueueSize
	if instance.Status != "" && instance.Status != "running" {
		return out
	}
	if err := s.call(instance, http.MethodGet, "/admin/summary", nil, &out); err != nil {
		out.Error = err.Error()
		return out
	}
	out.Online = true
	for _, slot := range out.Slots {
		slot["instance"] = instance.Name
		if groupName, ok := mihomoGroupForProxyURL(fmt.Sprint(slot["url"])); ok {
			slot["mihomo_group"] = groupName
			if group, err := s.getMihomoProxyGroup(groupName); err == nil {
				slot["mihomo_node"] = group.Now
			}
		}
	}
	return out
}
func (s *Server) audit(instance Instance) []AuditRecord {
	var out struct {
		Records []AuditRecord `json:"records"`
	}
	if s.call(instance, "GET", "/admin/audit", nil, &out) != nil {
		return nil
	}
	for i := range out.Records {
		out.Records[i].Instance = instance.Name
	}
	return out.Records
}
func (s *Server) logs(instance Instance) []SystemLog {
	var out struct {
		Logs []SystemLog `json:"logs"`
	}
	if s.call(instance, "GET", "/admin/logs", nil, &out) != nil {
		return nil
	}
	for i := range out.Logs {
		out.Logs[i].Instance = instance.Name
	}
	return out.Logs
}
func (s *Server) call(instance Instance, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequest(method, instance.URL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.InstanceToken)
	if body != nil {
		req.Header.Set("content-type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (s *Server) createKey(w http.ResponseWriter) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		http.Error(w, `{"error":"key_generation_failed"}`, 500)
		return
	}
	key := "gw_" + hex.EncodeToString(buf)
	s.mu.Lock()
	s.keys = append(s.keys, key)
	result := s.syncKeysLocked()
	s.persistLocked()
	s.mu.Unlock()
	writeJSON(w, result.Status, map[string]any{"key": key, "sync": result})
}
func (s *Server) deleteKey(w http.ResponseWriter, key string) {
	s.mu.Lock()
	next := s.keys[:0]
	for _, item := range s.keys {
		if item != key {
			next = append(next, item)
		}
	}
	if len(next) == 0 {
		s.mu.Unlock()
		http.Error(w, `{"error":"at_least_one_key_required"}`, 409)
		return
	}
	s.keys = next
	result := s.syncKeysLocked()
	s.persistLocked()
	s.mu.Unlock()
	writeJSON(w, result.Status, map[string]any{"deleted": true, "sync": result})
}

type syncResult struct {
	Status  int               `json:"-"`
	Updated []string          `json:"updated"`
	Failed  map[string]string `json:"failed"`
}

func (s *Server) syncKeysLocked() syncResult {
	result := syncResult{Status: 200, Failed: map[string]string{}}
	instances, err := s.discoverInstances()
	if err != nil {
		instances = append([]Instance(nil), s.instances...)
	}
	for _, instance := range instances {
		if instance.Status != "" && instance.Status != "running" {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"keys": append([]string(nil), s.keys...)})
		if err := s.call(instance, "PUT", "/admin/keys", strings.NewReader(string(payload)), nil); err != nil {
			result.Failed[instance.Name] = err.Error()
		} else {
			result.Updated = append(result.Updated, instance.Name)
		}
	}
	if len(result.Failed) > 0 {
		result.Status = 207
	}
	return result
}
func (s *Server) broadcast(w http.ResponseWriter, r *http.Request, path string) {
	data, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	result := syncResult{Status: 200, Failed: map[string]string{}}
	for _, instance := range s.currentInstances() {
		if instance.Status != "" && instance.Status != "running" {
			continue
		}
		if err := s.call(instance, "POST", path, strings.NewReader(string(data)), nil); err != nil {
			result.Failed[instance.Name] = err.Error()
		} else {
			result.Updated = append(result.Updated, instance.Name)
		}
	}
	if len(result.Failed) > 0 {
		result.Status = 207
	}
	writeJSON(w, result.Status, result)
}
func (s *Server) currentInstances() []Instance {
	instances, err := s.discoverInstances()
	if err == nil {
		return instances
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]Instance(nil), s.instances...)
}
func (s *Server) loadKeys() {
	data, err := os.ReadFile(s.cfg.DataDir + "/keys.json")
	if err == nil {
		json.Unmarshal(data, &s.keys)
	}
}

func (s *Server) loadInstances() {
	data, err := os.ReadFile(s.cfg.DataDir + "/instances.json")
	if err == nil {
		_ = json.Unmarshal(data, &s.instances)
	}
}
func (s *Server) loadZenKeys() {
	data, err := os.ReadFile(s.cfg.DataDir + "/zen-keys.json")
	if err == nil {
		_ = json.Unmarshal(data, &s.zenKeys)
	}
	if s.zenKeys == nil {
		s.zenKeys = make(map[string]string)
	}
}
func (s *Server) persistZenKeysLocked() {
	_ = os.MkdirAll(s.cfg.DataDir, 0o700)
	data, _ := json.MarshalIndent(s.zenKeys, "", "  ")
	_ = os.WriteFile(s.cfg.DataDir+"/zen-keys.json", data, 0o600)
}
func (s *Server) persistInstancesLocked() {
	_ = os.MkdirAll(s.cfg.DataDir, 0o700)
	data, _ := json.MarshalIndent(s.instances, "", "  ")
	_ = os.WriteFile(s.cfg.DataDir+"/instances.json", data, 0o600)
}
func (s *Server) persistLocked() {
	os.MkdirAll(s.cfg.DataDir, 0o700)
	data, _ := json.MarshalIndent(s.keys, "", "  ")
	os.WriteFile(s.cfg.DataDir+"/keys.json", data, 0o600)
}
func bearer(r *http.Request, expected string) bool {
	got := requestBearer(r)
	return len(got) == len(expected) && subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}
func requestBearer(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(header) < 7 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}
func containsKey(keys []string, expected string) bool {
	for _, key := range keys {
		if len(key) == len(expected) && subtle.ConstantTimeCompare([]byte(key), []byte(expected)) == 1 {
			return true
		}
	}
	return false
}
func maskAPIKey(key string) string {
	if len(key) <= 8 {
		return strings.Repeat("*", len(key))
	}
	return key[:4] + strings.Repeat("*", len(key)-8) + key[len(key)-4:]
}
func zenAuthModeForKey(key string) string {
	if strings.TrimSpace(key) == zenAuthModePublic {
		return zenAuthModePublic
	}
	if strings.TrimSpace(key) != "" {
		return zenAuthModeCustom
	}
	return ""
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
func writeAPIError(w http.ResponseWriter, status int, code string, err error) {
	writeJSON(w, status, map[string]string{"error": code, "detail": err.Error()})
}
func env(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}
func boolEnv(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
func compatibleBoolEnv(primary, legacy string, fallback bool) bool {
	if strings.TrimSpace(os.Getenv(primary)) != "" {
		return boolEnv(primary, fallback)
	}
	return boolEnv(legacy, fallback)
}
func split(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
