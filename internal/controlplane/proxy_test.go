package controlplane

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	direct := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { directCalls++; w.WriteHeader(http.StatusOK) }))
	defer direct.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { proxyCalls++; w.WriteHeader(http.StatusOK) }))
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
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "no_gateway_instances") {
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
