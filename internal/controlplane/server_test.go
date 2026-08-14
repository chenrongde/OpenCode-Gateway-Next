package controlplane

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStaticAssetsDisableStaleCaching(t *testing.T) {
	server := &Server{}
	request := httptest.NewRequest(http.MethodGet, "/static/style.css", nil)
	response := httptest.NewRecorder()

	server.static(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("static stylesheet status = %d, want %d", response.Code, http.StatusOK)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-cache, must-revalidate" {
		t.Fatalf("Cache-Control = %q, want no-cache, must-revalidate", got)
	}
	if !strings.Contains(response.Body.String(), "--bg") {
		t.Fatal("static stylesheet body does not contain the embedded application CSS")
	}
}
