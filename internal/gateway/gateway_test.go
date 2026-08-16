package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(upstream string) Config {
	c := DefaultConfig()
	c.UpstreamURL = upstream
	c.OpenCodeAPIKey = "zen-test-key"
	c.GatewayKeys = []string{"test-key"}
	c.MaxConcurrency = 1
	c.QueueSize = 1
	c.MaxRetries = 2
	c.CooldownBase = time.Millisecond
	c.CooldownMax = 5 * time.Millisecond
	c.ProxyProbeURLs = nil
	return c
}

func TestLoadConfigRequiresOpenCodeAPIKey(t *testing.T) {
	t.Setenv("GATEWAY_KEYS", "gateway-key")
	t.Setenv("OPENCODE_API_KEY", "")
	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "OPENCODE_API_KEY is required") {
		t.Fatalf("expected missing OpenCode key error, got %v", err)
	}
}

func TestForwardReplacesClientAuthorizationWithOpenCodeKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer zen-test-key" {
			t.Errorf("authorization = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestMethodAllowedRejectsGetChatCompletions(t *testing.T) {
	for _, path := range []string{
		"/v1/chat/completions",
		"/openai/v1/chat/completions",
		"/anthropic/v1/chat/completions",
		"/codex/v1/chat/completions",
	} {
		if allowed, allow := methodAllowed(path, http.MethodGet); allowed || allow != http.MethodPost {
			t.Errorf("methodAllowed(%q, GET) = (%v, %q), want (false, POST)", path, allowed, allow)
		}
	}
}

func TestMethodAllowedAcceptsModelsGetAndChatPost(t *testing.T) {
	if allowed, _ := methodAllowed("/v1/models", http.MethodGet); !allowed {
		t.Fatal("GET /v1/models should be allowed")
	}
	if allowed, _ := methodAllowed("/v1/chat/completions", http.MethodPost); !allowed {
		t.Fatal("POST /v1/chat/completions should be allowed")
	}
}

func TestHandlerReturns405ForGetChatCompletions(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if got := res.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow = %q, want POST", got)
	}
	var body map[string]string
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["error"] != "method_not_allowed" || body["method"] != http.MethodGet || body["path"] != "/v1/chat/completions" || body["allow"] != http.MethodPost {
		t.Fatalf("body = %#v", body)
	}
}

func TestForwardDoesNotRetrySingleEgress429(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer zen-test-key" {
			t.Errorf("authorization = %q", got)
		}
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestForwardRetriesOnlyDistinctEgressAndSetsOpenCodeHeaders(t *testing.T) {
	var calls atomic.Int32
	requestIDs := make(map[string]struct{})
	var requestIDsMu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != opencodeUserAgent {
			t.Errorf("user-agent = %q", got)
		}
		if got := r.Header.Get("X-OpenCode-Client"); got != opencodeClient {
			t.Errorf("x-opencode-client = %q", got)
		}
		if got := r.Header.Get("HTTP-Referer"); got != opencodeReferer {
			t.Errorf("http-referer = %q", got)
		}
		if got := r.Header.Get("X-Title"); got != opencodeTitle {
			t.Errorf("x-title = %q", got)
		}
		requestIDsMu.Lock()
		requestIDs[r.Header.Get("X-OpenCode-Request")] = struct{}{}
		requestIDsMu.Unlock()
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"messages":[],"model":"x-free"}` {
			t.Errorf("body = %q", body)
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
		{client: g.client, url: "socks5h://proxy:10803", egress: "192.0.2.3"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || calls.Load() != 3 {
		t.Fatalf("status = %d, calls = %d, body = %s", res.Code, calls.Load(), res.Body.String())
	}
	if len(requestIDs) != 1 {
		t.Fatalf("request IDs = %#v", requestIDs)
	}
}

func TestForwardOverridesClientOpenCodeHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != opencodeUserAgent {
			t.Errorf("user-agent = %q", got)
		}
		if got := r.Header.Get("X-OpenCode-Client"); got != opencodeClient {
			t.Errorf("x-opencode-client = %q", got)
		}
		if got := r.Header.Get("HTTP-Referer"); got != opencodeReferer {
			t.Errorf("http-referer = %q", got)
		}
		if got := r.Header.Get("X-Title"); got != opencodeTitle {
			t.Errorf("x-title = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	g, err := New(testConfig(upstream.URL), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("User-Agent", "client-spoof")
	req.Header.Set("X-OpenCode-Client", "desktop")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestForwardUsesConfiguredZenCPAHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "opencode/1.4.3" {
			t.Errorf("user-agent = %q", got)
		}
		if got := r.Header.Get("X-OpenCode-Client"); got != "desktop" {
			t.Errorf("x-opencode-client = %q", got)
		}
		if got := r.Header.Get("HTTP-Referer"); got != "https://opencode.ai/" {
			t.Errorf("http-referer = %q", got)
		}
		if got := r.Header.Get("X-Title"); got != "opencode" {
			t.Errorf("x-title = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.OpenCodeClient = "desktop"
	c.OpenCodeVersion = "1.4.3"
	c.OpenCodeReferer = "https://opencode.ai/"
	c.OpenCodeTitle = "opencode"
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestForwardAddsContextToUpstreamResponseEOF(t *testing.T) {
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF}),
		}, nil
	})}
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	failing := &proxySlot{client: client, egress: "192.0.2.1"}
	healthy := &proxySlot{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"}
	g.slots = []*proxySlot{failing, healthy}
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if g.stats.Success.Load() != 0 || g.stats.Errors.Load() != 1 {
		t.Fatalf("stats success=%d errors=%d", g.stats.Success.Load(), g.stats.Errors.Load())
	}
	if failing.fails == 0 || g.active.Load() != 1 {
		t.Fatalf("failing slot=%#v active=%d", failing, g.active.Load())
	}
	g.logsMu.RLock()
	logs := append([]systemLog(nil), g.logs...)
	g.logsMu.RUnlock()
	if len(logs) == 0 || !strings.Contains(logs[len(logs)-1].Message, "forward failed") || !strings.Contains(logs[len(logs)-1].Fields["error"].(string), "upstream response body") {
		t.Fatalf("logs = %#v", logs)
	}
}

func TestForwardRetriesNonStreamingTruncatedResponseOnNextEgress(t *testing.T) {
	firstClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF})}, nil
	})}
	secondClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
	})}
	c := testConfig("https://example.com")
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: firstClient, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: secondClient, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[],"stream":false}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || res.Body.String() != `{"ok":true}` {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	if g.active.Load() != 1 || g.stats.Success.Load() != 1 || g.stats.Errors.Load() != 0 {
		t.Fatalf("active=%d success=%d errors=%d", g.active.Load(), g.stats.Success.Load(), g.stats.Errors.Load())
	}
	g.auditMu.RLock()
	audits := append([]auditRecord(nil), g.audits...)
	g.auditMu.RUnlock()
	if len(audits) != 2 || audits[0].Status != http.StatusBadGateway || audits[1].Status != http.StatusOK || audits[1].Attempts != 2 {
		t.Fatalf("audits=%#v", audits)
	}
}

func TestForwardRetriesStreamingResponseBeforeFirstOutput(t *testing.T) {
	firstCalls, secondCalls := 0, 0
	firstClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		firstCalls++
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(&errorReader{err: io.ErrUnexpectedEOF})}, nil
	})}
	secondClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		secondCalls++
		body := "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\ndata: [DONE]\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	c := testConfig("https://example.com")
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: firstClient, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: secondClient, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[],"stream":true}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"content":"OK"`) || firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("status=%d first=%d second=%d body=%s", res.Code, firstCalls, secondCalls, res.Body.String())
	}
	g.auditMu.RLock()
	audits := append([]auditRecord(nil), g.audits...)
	g.auditMu.RUnlock()
	if len(audits) != 2 || audits[0].Status != http.StatusBadGateway || audits[1].Status != http.StatusOK || audits[1].Attempts != 2 {
		t.Fatalf("audits = %#v", audits)
	}
}

type errorReader struct{ err error }

func (r *errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestModelCooldownDoesNotBlockOtherModelsOnSameEgress(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"model":"model-a-free"`) {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxRetries = 0
	c.CooldownBase = time.Minute
	c.CooldownMax = time.Minute
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}

	request := func(model string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","messages":[]}`))
		req.Header.Set("Authorization", "Bearer test-key")
		res := httptest.NewRecorder()
		g.Handler().ServeHTTP(res, req)
		return res
	}
	if res := request("model-a"); res.Code != http.StatusTooManyRequests {
		t.Fatalf("model-a status = %d", res.Code)
	}
	if res := request("model-b"); res.Code != http.StatusOK {
		t.Fatalf("model-b status = %d, body = %s", res.Code, res.Body.String())
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d", calls.Load())
	}
}

func TestModel429FailsOverOnceToDistinctEgress(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("status = %d, calls = %d, body = %s", res.Code, calls.Load(), res.Body.String())
	}
	g.auditMu.RLock()
	defer g.auditMu.RUnlock()
	if len(g.audits) != 2 || g.audits[0].Status != http.StatusTooManyRequests || g.audits[1].Status != http.StatusOK {
		t.Fatalf("audits = %#v", g.audits)
	}
	if g.audits[0].Model != "model-a-free" || g.audits[1].Model != "model-a-free" {
		t.Fatalf("audit models = %q, %q", g.audits[0].Model, g.audits[1].Model)
	}
}

func Test429FailoverKeepsTheNewActiveSlotForFollowingRequests(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxRetries = 1
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
	}
	request := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"model-a","messages":[]}`))
		req.Header.Set("Authorization", "Bearer test-key")
		res := httptest.NewRecorder()
		g.Handler().ServeHTTP(res, req)
		return res
	}
	if res := request(); res.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s", res.Code, res.Body.String())
	}
	if got := g.active.Load(); got != 1 {
		t.Fatalf("active after 429 failover = %d, want 1", got)
	}
	if res := request(); res.Code != http.StatusOK {
		t.Fatalf("following status = %d, body = %s", res.Code, res.Body.String())
	}
	if got := g.active.Load(); got != 1 {
		t.Fatalf("active changed during ordinary request = %d, want 1", got)
	}
}

func TestSlotSelectionSticksUntilCurrentEgressIsExcluded(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "socks5h://proxy:10801", egress: "192.0.2.1"},
		{client: g.client, url: "socks5h://proxy:10802", egress: "192.0.2.2"},
		{client: g.client, url: "socks5h://proxy:10803", egress: "192.0.2.3"},
	}
	for i := 0; i < 3; i++ {
		slot, _ := g.selectSlotExcluding("model-free", nil)
		if slot != g.slots[0] {
			t.Fatalf("ordinary request %d selected %q", i+1, slot.url)
		}
	}
	excluded := map[string]struct{}{g.slots[0].identity(): {}}
	slot, _ := g.selectSlotExcluding("model-free", excluded)
	if slot != g.slots[1] {
		t.Fatalf("failover selected %q", slot.url)
	}
	slot, _ = g.selectSlotExcluding("model-free", nil)
	if slot != g.slots[1] {
		t.Fatalf("selection did not remain on failover slot: %q", slot.url)
	}
}

func TestSummaryMarksOnlyCurrentSlotActive(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{{client: g.client, url: "proxy-a"}, {client: g.client, url: "proxy-b"}}
	g.active.Store(1)
	encoded, _ := json.Marshal(g.summary())
	var summary struct {
		Slots []struct {
			Active bool `json:"active"`
		} `json:"slots"`
	}
	if err := json.Unmarshal(encoded, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Slots[0].Active || !summary.Slots[1].Active {
		t.Fatalf("active slots = %#v", summary.Slots)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStaticProxyProbeRecordsAndDeduplicatesEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	config.InstanceAdminToken = "internal"
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	slot := func(raw, ip string) *proxySlot {
		client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(ip))}, nil
		})}
		return &proxySlot{client: client, url: raw}
	}
	g.slots = []*proxySlot{slot("socks5h://mihomo:10801", "192.0.2.1"), slot("socks5h://mihomo:10802", "192.0.2.1"), slot("socks5h://mihomo:10803", "192.0.2.2")}
	g.probeAndDeduplicateStaticSlots()
	if len(g.slots) != 3 || g.slots[0].egress != "192.0.2.1" || !g.slots[1].disabled || g.slots[2].egress != "192.0.2.2" {
		t.Fatalf("slots = %#v", g.slots)
	}
}

func TestStaticProxyDedupPrefersExplicitProxyOverDirect(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	slot := func(raw string) *proxySlot {
		client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.8"))}, nil
		})}
		return &proxySlot{client: client, url: raw}
	}
	g.slots = []*proxySlot{slot(""), slot("socks5h://mihomo:10801")}

	g.probeAndDeduplicateStaticSlots()

	if len(g.slots) != 2 || !g.slots[0].disabled || g.slots[1].url != "socks5h://mihomo:10801" || g.slots[1].egress != "192.0.2.8" {
		t.Fatalf("slots = %#v", g.slots)
	}
}

func TestStaticProxyProbeMarksFailuresUnavailableAndHealthNeedsOneHealthySlot(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	failed := &proxySlot{url: "socks5h://mihomo:10801", disabled: true, client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("certificate mismatch")
	})}}
	healthy := &proxySlot{url: "socks5h://mihomo:10802", disabled: true, client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.20"))}, nil
	})}}
	g := &Gateway{cfg: config, slots: []*proxySlot{failed, healthy}}
	g.probeAndDeduplicateStaticSlots()
	if !failed.disabled || failed.probeError == "" {
		t.Fatalf("failed slot = %#v", failed)
	}
	if healthy.disabled || healthy.egress != "192.0.2.20" {
		t.Fatalf("healthy slot = %#v", healthy)
	}
	res := httptest.NewRecorder()
	g.health(res, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if res.Code != http.StatusOK {
		t.Fatalf("health status = %d, body = %s", res.Code, res.Body.String())
	}
}

func TestHealthReportsUnavailableWhileAllSlotsAreCoolingDown(t *testing.T) {
	g := &Gateway{slots: []*proxySlot{{
		url:    "socks5h://mihomo:10801",
		egress: "192.0.2.10",
		until:  time.Now().Add(time.Minute),
	}}}
	response := httptest.NewRecorder()
	g.health(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("cooling slot health status = %d, body = %s", response.Code, response.Body.String())
	}

	g.slots[0].until = time.Time{}
	response = httptest.NewRecorder()
	g.health(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("ready slot health status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestProbeDirectSlotRecordsEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	direct := &proxySlot{client: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.9"))}, nil
	})}}
	g.slots = []*proxySlot{direct}

	g.probeDirectSlot(direct)

	if direct.egress != "192.0.2.9" {
		t.Fatalf("egress = %q", direct.egress)
	}
}

func TestAdminProbeRefreshesSelectedSlotEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.ProxyProbeURLs = []string{"https://probe.example"}
	config.InstanceAdminToken = "internal"
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("192.0.2.44"))}, nil
	})}
	g.slots = []*proxySlot{{client: client, url: "socks5h://mihomo:10801", egress: "192.0.2.10", fails: 2, until: time.Now().Add(time.Minute), modelCooldowns: map[string]cooldownState{"model-free": {Fails: 1, Until: time.Now().Add(time.Minute)}}}}
	req := httptest.NewRequest(http.MethodPost, "/admin/probe", strings.NewReader(`{"url":"socks5h://mihomo:10801"}`))
	req.Header.Set("Authorization", "Bearer internal")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body.String())
	}
	if g.slots[0].egress != "192.0.2.44" || !strings.Contains(res.Body.String(), "192.0.2.44") {
		t.Fatalf("slot = %#v, body = %s", g.slots[0], res.Body.String())
	}
	if g.slots[0].fails != 0 || !g.slots[0].until.IsZero() || len(g.slots[0].modelCooldowns) != 0 {
		t.Fatalf("old cooldown was retained after egress change: %#v", g.slots[0])
	}
}

func TestAdminRotateAdvancesStickySlotAndSkipsForbiddenEgress(t *testing.T) {
	config := testConfig("https://example.com")
	config.InstanceAdminToken = "internal"
	g, err := New(config, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	g.slots = []*proxySlot{
		{client: g.client, url: "proxy-a", egress: "192.0.2.1"},
		{client: g.client, url: "proxy-b", egress: "192.0.2.2"},
		{client: g.client, url: "proxy-c", egress: "192.0.2.3"},
	}
	req := httptest.NewRequest(http.MethodPost, "/admin/rotate", strings.NewReader(`{"forbidden":["192.0.2.2"]}`))
	req.Header.Set("Authorization", "Bearer internal")
	res := httptest.NewRecorder()
	g.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK || g.active.Load() != 2 || !strings.Contains(res.Body.String(), "192.0.2.3") {
		t.Fatalf("status = %d, active = %d, body = %s", res.Code, g.active.Load(), res.Body.String())
	}
}

func TestParseProbeIPSupportsPlainAndCloudflareTrace(t *testing.T) {
	for input, want := range map[string]string{
		"192.0.2.9\n":                    "192.0.2.9",
		"fl=1\nip=2001:db8::9\nloc=US\n": "2001:db8::9",
		"not an ip":                      "",
	} {
		if got := parseProbeIP([]byte(input)); got != want {
			t.Errorf("parseProbeIP(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestProbeSlotFallsBackToNextURL(t *testing.T) {
	var calls atomic.Int32
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("first probe unavailable")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ip=192.0.2.22\nloc=US\n")),
		}, nil
	})}
	g := &Gateway{cfg: Config{
		ProxyProbeURLs: []string{"https://first.example", "https://second.example"},
		ProxyProbeWait: time.Second,
	}}
	ip, err := g.probeSlot(&proxySlot{client: client, url: "socks5h://proxy:1080"})
	if err != nil {
		t.Fatal(err)
	}
	if ip != "192.0.2.22" || calls.Load() != 2 {
		t.Fatalf("ip = %q, calls = %d", ip, calls.Load())
	}
}

func TestUnauthorizedAndQueueLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.MaxConcurrency = 1
	c.QueueSize = 0
	g, err := New(c, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	unauth := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	got := httptest.NewRecorder()
	g.Handler().ServeHTTP(got, unauth)
	if got.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", got.Code)
	}
	first := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	first.Header.Set("Authorization", "Bearer test-key")
	done := make(chan struct{})
	go func() { r := httptest.NewRecorder(); g.Handler().ServeHTTP(r, first); close(done) }()
	time.Sleep(5 * time.Millisecond)
	second := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	second.Header.Set("Authorization", "Bearer test-key")
	r2 := httptest.NewRecorder()
	g.Handler().ServeHTTP(r2, second)
	if r2.Code != http.StatusTooManyRequests {
		t.Fatalf("queue status = %d", r2.Code)
	}
	<-done
}

func TestBuildProxySlotsAcceptsSOCKS5(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	slots, err := g.buildProxySlots([]string{"socks5h://user:pass@127.0.0.1:1080"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 1 || slots[0].url != "socks5h://user:pass@127.0.0.1:1080" {
		t.Fatalf("unexpected slots: %#v", slots)
	}
}

func TestBuildProxySlotsRejectsVLESS(t *testing.T) {
	g, err := New(testConfig("https://example.com"), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.buildProxySlots([]string{"vless://uuid@example.com:443"}); err == nil {
		t.Fatal("expected vless URL rejection")
	}
}

func TestDialSOCKS5PassesHostnameToProxy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	targets := make(chan string, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		_, _ = io.ReadFull(conn, greeting)
		_, _ = conn.Write([]byte{0x05, 0x00})
		header := make([]byte, 5)
		_, _ = io.ReadFull(conn, header)
		host := make([]byte, int(header[4]))
		_, _ = io.ReadFull(conn, host)
		port := make([]byte, 2)
		_, _ = io.ReadFull(conn, port)
		targets <- net.JoinHostPort(string(host), strconv.Itoa(int(binary.BigEndian.Uint16(port))))
		_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0})
	}()

	conn, err := dialSOCKS5(context.Background(), "tcp", listener.Addr().String(), "opencode.ai:443", "", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if target := <-targets; target != "opencode.ai:443" {
		t.Fatalf("target = %q", target)
	}
}

func TestTokenUsageParsesRegularAndStreamingResponses(t *testing.T) {
	regular := parseTokenUsage([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20,"prompt_tokens_details":{"cached_tokens":4}}}`))
	if regular != (tokenUsage{PromptTokens: 12, CompletionTokens: 8, TotalTokens: 20, CachedTokens: 4}) {
		t.Fatalf("regular usage = %#v", regular)
	}
	responses := parseTokenUsage([]byte(`{"response":{"usage":{"input_tokens":15,"output_tokens":9,"total_tokens":24,"input_tokens_details":{"cached_tokens":6}}}}`))
	if responses != (tokenUsage{PromptTokens: 15, CompletionTokens: 9, TotalTokens: 24, CachedTokens: 6}) {
		t.Fatalf("responses usage = %#v", responses)
	}
	if model := requestModel([]byte(`{"model":"deepseek-v4-flash-free","input":"hello"}`)); model != "deepseek-v4-flash-free" {
		t.Fatalf("responses model = %q", model)
	}
	if !streamingRequest([]byte(`{"model":"deepseek-v4-flash-free","input":"hello","stream":true}`)) {
		t.Fatal("responses stream was not detected")
	}
	if !sseLineHasOutput(`data: {"type":"response.output_text.delta","delta":"OK"}`) {
		t.Fatal("responses output event was not detected")
	}

	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":15,\"completion_tokens\":9,\"total_tokens\":24}}\n\ndata: [DONE]\n\n")
	usage, firstTokenMS, committed, err := copyResponse(response, stream, time.Now().Add(-20*time.Millisecond), true)
	if err != nil {
		t.Fatal(err)
	}
	if usage != (tokenUsage{PromptTokens: 15, CompletionTokens: 9, TotalTokens: 24}) {
		t.Fatalf("stream usage = %#v", usage)
	}
	if firstTokenMS < 10 {
		t.Fatalf("first token latency = %dms", firstTokenMS)
	}
	if !committed {
		t.Fatal("stream response was not committed after its first output token")
	}
	if !strings.Contains(response.Body.String(), "data: [DONE]") {
		t.Fatalf("stream was not forwarded: %q", response.Body.String())
	}

	response = httptest.NewRecorder()
	responsesStream := strings.NewReader("event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"OK\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n")
	usage, _, committed, err = copyResponse(response, responsesStream, time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if usage != (tokenUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}) {
		t.Fatalf("responses stream usage = %#v", usage)
	}
	if !committed || !strings.Contains(response.Body.String(), `"response.output_text.delta"`) {
		t.Fatalf("responses stream was not committed: %q", response.Body.String())
	}
}

func TestCopyResponseAddsFinishReasonBeforeDoneWhenUpstreamOmitsIt(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"}}]}\n\ndata: [DONE]\n\n")

	_, _, committed, err := copyResponse(response, stream, time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("stream response was not committed")
	}
	body := response.Body.String()
	finishAt := strings.Index(body, `"finish_reason":"stop"`)
	doneAt := strings.Index(body, "data: [DONE]")
	if finishAt < 0 || doneAt < 0 || finishAt > doneAt {
		t.Fatalf("terminal finish chunk must precede [DONE], body = %q", body)
	}
}

func TestCopyResponseDoesNotDuplicateUpstreamFinishReason(t *testing.T) {
	response := httptest.NewRecorder()
	stream := strings.NewReader("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")

	_, _, committed, err := copyResponse(response, stream, time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("stream response was not committed")
	}
	if count := strings.Count(response.Body.String(), `"finish_reason":"stop"`); count != 1 {
		t.Fatalf("finish reason count = %d, body = %q", count, response.Body.String())
	}
}

func TestMaskGatewayKeyDoesNotExposePlaintext(t *testing.T) {
	masked := maskGatewayKey("gw_1234567890abcdef")
	if masked != "gw_...cdef" || strings.Contains(masked, "1234567890") {
		t.Fatalf("masked key = %q", masked)
	}
}
