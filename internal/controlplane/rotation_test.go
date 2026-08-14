package controlplane

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestMihomoGroupForProxyURL(t *testing.T) {
	for raw, want := range map[string]string{
		"socks5h://mihomo:10801":                 "GATEWAY-SLOT-1",
		"socks5://opencode-gateway-mihomo:10808": "GATEWAY-SLOT-8",
	} {
		got, ok := mihomoGroupForProxyURL(raw)
		if !ok || got != want {
			t.Errorf("mihomoGroupForProxyURL(%q) = (%q, %v), want %q", raw, got, ok, want)
		}
	}
	if _, ok := mihomoGroupForProxyURL("socks5h://other:10801"); ok {
		t.Fatal("non-Mihomo proxy was treated as a managed slot")
	}
}

func TestRotateInstanceMihomoSlotSkipsOccupiedEgress(t *testing.T) {
	var mu sync.Mutex
	current := "node-a"
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/proxies/GATEWAY-SLOT-1" {
			t.Fatalf("controller path = %s", r.URL.Path)
		}
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"name": "GATEWAY-SLOT-1", "now": current, "all": []string{"node-a", "node-b", "node-c"}})
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		current = body["name"]
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()

	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/probe" || r.Method != http.MethodPost {
			t.Fatalf("gateway request = %s %s", r.Method, r.URL.Path)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		node := current
		mu.Unlock()
		ip := map[string]string{"node-a": "192.0.2.1", "node-b": "192.0.2.2", "node-c": "192.0.2.3"}[node]
		_ = json.NewEncoder(w).Encode(map[string]any{"slots": []map[string]string{{"url": "socks5h://mihomo:10801", "egress": ip}}})
	}))
	defer gateway.Close()

	s := New(Config{AdminToken: "admin", InstanceToken: "internal", MihomoAPIURL: controller.URL, DataDir: t.TempDir()})
	instance := Instance{Name: "gateway-a", URL: gateway.URL, Container: "gateway-a", Status: "running"}
	result, err := s.rotateInstanceMihomoSlot(instance, "socks5h://mihomo:10801", "192.0.2.1", "duplicate_egress", map[string]struct{}{"192.0.2.1": {}, "192.0.2.2": {}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Node != "node-c" || result.Egress != "192.0.2.3" || result.Attempts != 2 {
		t.Fatalf("result = %#v", result)
	}
}

func TestReconcileEgressesRotatesDuplicateAcrossInstances(t *testing.T) {
	var mu sync.Mutex
	selected := map[string]string{"GATEWAY-SLOT-1": "node-a", "GATEWAY-SLOT-2": "node-a"}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		group := strings.TrimPrefix(r.URL.Path, "/proxies/")
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"name": group, "now": selected[group], "all": []string{"node-a", "node-b"}})
			return
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		selected[group] = body["name"]
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()

	gateway := func(proxyURL, group string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			node := selected[group]
			mu.Unlock()
			ip := "192.0.2.1"
			if group == "GATEWAY-SLOT-2" && node == "node-b" {
				ip = "192.0.2.2"
			}
			switch r.URL.Path {
			case "/admin/summary":
				_ = json.NewEncoder(w).Encode(map[string]any{"slots": []map[string]any{{"url": proxyURL, "egress": ip, "direct": false, "enabled": true}}, "stats": map[string]uint64{"upstream429": 0}, "max_concurrency": 4})
			case "/admin/probe":
				_ = json.NewEncoder(w).Encode(map[string]any{"slots": []map[string]string{{"url": proxyURL, "egress": ip}}})
			default:
				http.NotFound(w, r)
			}
		}))
	}
	first := gateway("socks5h://mihomo:10801", "GATEWAY-SLOT-1")
	defer first.Close()
	second := gateway("socks5h://mihomo:10802", "GATEWAY-SLOT-2")
	defer second.Close()

	s := New(Config{AdminToken: "admin", InstanceToken: "internal", MihomoAPIURL: controller.URL, DataDir: t.TempDir()})
	s.instances = []Instance{
		{Name: "gateway-a", URL: first.URL, Container: "gateway-a", Status: "running", ProxyURLs: []string{"socks5h://mihomo:10801"}},
		{Name: "gateway-b", URL: second.URL, Container: "gateway-b", Status: "running", ProxyURLs: []string{"socks5h://mihomo:10802"}},
	}
	if err := s.ReconcileEgresses(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := selected["GATEWAY-SLOT-2"]
	mu.Unlock()
	if got != "node-b" {
		t.Fatalf("gateway-b selector = %q", got)
	}
	if len(s.rotationLogs) != 1 || s.rotationLogs[0].Message != "duplicate egress rotated" {
		t.Fatalf("rotation logs = %#v", s.rotationLogs)
	}
}
