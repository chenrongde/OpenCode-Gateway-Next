package controlplane

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAPIHandlerReturnsUnavailableWithoutRunningInstances(t *testing.T) {
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), BootstrapKeys: []string{"gateway-key"}})
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
	request.Header.Set("Authorization", "Bearer gateway-key")
	response := httptest.NewRecorder()
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestAPIHandlerProxiesPathQueryHeadersAndResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/v1/chat/completions" || r.URL.RawQuery != "trace=1" {
			t.Errorf("request URL = %s", r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer gateway-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"model":"deepseek-v4-flash-free"}` {
			t.Errorf("body = %s", body)
		}
		w.Header().Set("X-Upstream", "gateway-a")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":"limited"}`)
	}))
	defer upstream.Close()
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), BootstrapKeys: []string{"gateway-key"}})
	s.instances = []Instance{{Name: "gateway-a", URL: upstream.URL, Status: "running"}}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?trace=1", strings.NewReader(`{"model":"deepseek-v4-flash-free"}`))
	request.Header.Set("Authorization", "Bearer gateway-key")
	response := httptest.NewRecorder()
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests || response.Header().Get("X-Upstream") != "gateway-a" || response.Body.String() != `{"error":"limited"}` {
		t.Fatalf("response = %d, headers=%v, body=%s", response.Code, response.Header(), response.Body.String())
	}
}

func TestAPIHandlerUsesProxyTrafficPool(t *testing.T) {
	directCalls, proxyCalls := 0, 0
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		directCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer direct.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		proxyCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), DirectFallback: false, BootstrapKeys: []string{"gateway-key"}})
	s.instances = []Instance{
		{Name: "gateway-a", URL: direct.URL, Status: "running"},
		{Name: "gateway-b", URL: proxy.URL, Status: "running", ProxyURLs: []string{"socks5h://mihomo:10801"}},
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer gateway-key")
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || directCalls != 0 || proxyCalls != 1 {
		t.Fatalf("status=%d direct=%d proxy=%d", response.Code, directCalls, proxyCalls)
	}
}

func TestAPIHandlerReturnsServiceUnavailableWithoutInstances(t *testing.T) {
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), BootstrapKeys: []string{"gateway-key"}})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer gateway-key")
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "no_healthy_gateway_instances") {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestAPIHandlerRejectsUnknownKey(t *testing.T) {
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), BootstrapKeys: []string{"gateway-key"}})
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer wrong-key")
	response := httptest.NewRecorder()
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestAPIHandlerRejectsZenKeyAsClientCredential(t *testing.T) {
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), BootstrapKeys: []string{"gateway-key"}})
	s.zenKeys["gateway-a"] = "zen-secret-key"
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer zen-secret-key")
	response := httptest.NewRecorder()
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestZenKeysPersistReloadAndMask(t *testing.T) {
	dataDir := t.TempDir()
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: dataDir})
	s.zenKeys["gateway-a"] = "zen-instance-secret-key"
	s.persistZenKeysLocked()

	reloaded := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: dataDir})
	if reloaded.zenKeys["gateway-a"] != "zen-instance-secret-key" {
		t.Fatalf("reloaded keys = %#v", reloaded.zenKeys)
	}
	if got := maskAPIKey(reloaded.zenKeys["gateway-a"]); got != "zen-***************-key" {
		t.Fatalf("masked key = %q", got)
	}
	data, err := json.Marshal(Summary{ZenKeyMasked: maskAPIKey(reloaded.zenKeys["gateway-a"]), ZenKeySet: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "zen-instance-secret-key") {
		t.Fatalf("summary leaked key: %s", data)
	}
}

func TestAcquireAPIInstanceUsesLeastConnectionsAndRoundRobinTies(t *testing.T) {
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir()})
	instances := []Instance{{Name: "gateway-a"}, {Name: "gateway-b"}}
	s.apiInflight["gateway-a"] = 2
	if got := s.acquireAPIInstance(instances); got.Name != "gateway-b" {
		t.Fatalf("least-connections selected %q", got.Name)
	}
	s.apiInflight = map[string]int{}
	first := s.acquireAPIInstance(instances).Name
	second := s.acquireAPIInstance(instances).Name
	if first == second {
		t.Fatalf("round-robin tie selected %q twice", first)
	}
}

func TestAcquireAPIInstanceSkipsTemporarilyFailedInstance(t *testing.T) {
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir()})
	s.apiCircuits["gateway-a"] = apiCircuit{Failures: 1, Until: time.Now().Add(time.Minute)}
	instances := []Instance{{Name: "gateway-a"}, {Name: "gateway-b"}}
	if got := s.acquireAPIInstance(instances); got.Name != "gateway-b" {
		t.Fatalf("selected %q, want gateway-b", got.Name)
	}
}

func TestAPIHandlerTemporarilySkipsGatewayAfter5xx(t *testing.T) {
	var failedCalls, healthyCalls int
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		failedCalls++
		http.Error(w, `{"error":"upstream_unavailable"}`, http.StatusBadGateway)
	}))
	defer failed.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		healthyCalls++
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer healthy.Close()
	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), DirectFallback: true, BootstrapKeys: []string{"gateway-key"}})
	s.instances = []Instance{{Name: "gateway-a", URL: failed.URL, Status: "running"}, {Name: "gateway-b", URL: healthy.URL, Status: "running"}}
	request := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		req.Header.Set("Authorization", "Bearer gateway-key")
		s.APIHandler().ServeHTTP(response, req)
		return response
	}
	if response := request(); response.Code != http.StatusBadGateway {
		t.Fatalf("first status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(); response.Code != http.StatusOK || response.Body.String() != `{"ok":true}` {
		t.Fatalf("second status=%d body=%s", response.Code, response.Body.String())
	}
	if failedCalls != 1 || healthyCalls != 1 {
		t.Fatalf("failed=%d healthy=%d", failedCalls, healthyCalls)
	}
}

func TestAPIHandlerSkipsUnhealthyInstanceBeforeFirstRequest(t *testing.T) {
	var unhealthyRequests, healthyRequests int
	unhealthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		unhealthyRequests++
		http.Error(w, "should not receive traffic", http.StatusBadGateway)
	}))
	defer unhealthy.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		healthyRequests++
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer healthy.Close()

	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir(), DirectFallback: true, BootstrapKeys: []string{"gateway-key"}})
	s.instances = []Instance{{Name: "gateway-starting", URL: unhealthy.URL, Status: "running"}, {Name: "gateway-ready", URL: healthy.URL, Status: "running"}}
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer gateway-key")
	response := httptest.NewRecorder()
	s.APIHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || healthyRequests != 1 || unhealthyRequests != 0 {
		t.Fatalf("status=%d unhealthy=%d healthy=%d body=%s", response.Code, unhealthyRequests, healthyRequests, response.Body.String())
	}
}

func TestReadyTrafficPoolProbesInstancesConcurrently(t *testing.T) {
	var inflight atomic.Int32
	var maximum atomic.Int32
	newGateway := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/healthz" {
				http.NotFound(w, r)
				return
			}
			current := inflight.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			time.Sleep(25 * time.Millisecond)
			inflight.Add(-1)
			w.WriteHeader(http.StatusOK)
		}))
	}
	servers := []*httptest.Server{newGateway(), newGateway(), newGateway(), newGateway()}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()

	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir()})
	instances := make([]Instance, 0, len(servers))
	for index, server := range servers {
		instances = append(instances, Instance{Name: "gateway-" + string(rune('a'+index)), URL: server.URL, Status: "running"})
	}
	if ready := s.readyTrafficPool(instances); len(ready) != len(instances) {
		t.Fatalf("ready instances = %d, want %d", len(ready), len(instances))
	}
	if maximum.Load() < 2 {
		t.Fatalf("health checks ran sequentially, maximum inflight = %d", maximum.Load())
	}
}

func TestControlPagesAndTokenStats(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/audit" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"records":[{"at":"2026-08-16T00:00:01Z","request_id":"request-1","method":"POST","path":"/v1/chat/completions","model":"deepseek-v4-flash-free","status":502,"source":"gateway","client_key":"gw_...cdef","stream":true,"attempts":1,"latency_ms":1000},{"at":"2026-08-16T00:00:03Z","request_id":"request-1","method":"POST","path":"/v1/chat/completions","model":"deepseek-v4-flash-free","status":200,"source":"upstream","client_key":"gw_...cdef","stream":true,"attempts":2,"latency_ms":1500,"prompt_tokens":12,"completion_tokens":8,"total_tokens":20,"first_token_ms":250},{"at":"2026-08-16T00:01:00Z","request_id":"request-2","method":"POST","path":"/v1/responses","model":"deepseek-v4-flash-free","status":200,"source":"upstream","client_key":"gw_...cdef","stream":false,"attempts":1,"prompt_tokens":15,"completion_tokens":9,"total_tokens":24,"cached_tokens":6}]}`)
	}))
	defer upstream.Close()

	s := New(Config{AdminToken: "admin", InstanceToken: "internal", DataDir: t.TempDir()})
	s.instances = []Instance{{Name: "gateway-a", URL: upstream.URL, Status: "running"}}
	for _, path := range []string{"/", "/instances", "/mihomo", "/keys", "/logs", "/tokens"} {
		response := httptest.NewRecorder()
		s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "OpenCode Gateway Next") {
			t.Fatalf("page %s: status=%d body=%s", path, response.Code, response.Body.String())
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/api/tokens", nil)
	request.Header.Set("Authorization", "Bearer admin")
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("token status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Summary struct {
			Requests     int64   `json:"requests"`
			Errors       int64   `json:"errors"`
			TotalTokens  int64   `json:"total_tokens"`
			PromptTokens int64   `json:"prompt_tokens"`
			TotalCostUSD float64 `json:"total_cost_usd"`
		} `json:"summary"`
		Records []AuditRecord `json:"records"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Summary.Requests != 2 || result.Summary.Errors != 0 || result.Summary.TotalTokens != 44 || result.Summary.PromptTokens != 27 || result.Summary.TotalCostUSD < 7.716e-06 || result.Summary.TotalCostUSD > 7.718e-06 || len(result.Records) != 2 || result.Records[0].Path != "/v1/responses" || result.Records[1].ClientKey != "gw_...cdef" || result.Records[1].FirstTokenMS != 250 || !result.Records[1].Recovered || len(result.Records[1].AttemptHistory) != 2 || result.Records[1].LatencyMS != 3000 {
		t.Fatalf("token response = %#v", result)
	}

	request = httptest.NewRequest(http.MethodGet, "/api/tokens?path=%2Fv1%2Fresponses", nil)
	request.Header.Set("Authorization", "Bearer admin")
	response = httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("filtered token status=%d body=%s", response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Summary.Requests != 1 || result.Summary.TotalTokens != 24 || len(result.Records) != 1 || result.Records[0].Path != "/v1/responses" {
		t.Fatalf("filtered token response = %#v", result)
	}
}
