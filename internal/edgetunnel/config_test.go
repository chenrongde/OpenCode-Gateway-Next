package edgetunnel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratePreservesSNIAndUsesCandidateIPs(t *testing.T) {
	dir := t.TempDir()
	nodes := filepath.Join(dir, "nodes.txt")
	ips := filepath.Join(dir, "ips.txt")
	config := filepath.Join(dir, "sing-box.json")
	proxies := filepath.Join(dir, "proxies.txt")
	if err := os.WriteFile(nodes, []byte("vless://test-uuid@example.pages.dev:443?security=tls&type=ws&sni=example.pages.dev&host=example.pages.dev&path=%2Fsecret#node\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ips, []byte("# list\n172.67.1.1:443#ca latency=10\n104.20.1.1:443#us latency=20\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	count, err := Generate(Options{NodesFile: nodes, IPsFile: ips, ConfigFile: config, ProxyFile: proxies, ProxyHost: "sing-box", PortBase: 18080, MaxOutbounds: 2})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count = %d", count)
	}
	raw, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	outbounds := got["outbounds"].([]any)
	first := outbounds[0].(map[string]any)
	if first["server"] != "172.67.1.1" {
		t.Fatalf("server = %v", first["server"])
	}
	tls := first["tls"].(map[string]any)
	if tls["server_name"] != "example.pages.dev" {
		t.Fatalf("server_name = %v", tls["server_name"])
	}
	proxyData, _ := os.ReadFile(proxies)
	if strings.TrimSpace(string(proxyData)) != "http://sing-box:18081\nhttp://sing-box:18082" {
		t.Fatalf("proxies = %q", proxyData)
	}
}
