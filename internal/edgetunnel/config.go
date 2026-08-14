package edgetunnel

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Options struct {
	NodesFile    string
	IPsFile      string
	ConfigFile   string
	ProxyFile    string
	ProxyHost    string
	PortBase     int
	MaxOutbounds int
}

type node struct {
	UUID        string
	Server      string
	Port        int
	ServerName  string
	Host        string
	Path        string
	TLS         bool
	Fingerprint string
}

func Generate(opts Options) (int, error) {
	nodes, err := loadNodes(opts.NodesFile)
	if err != nil {
		return 0, err
	}
	addresses, err := loadAddresses(opts.IPsFile)
	if err != nil {
		return 0, err
	}
	if len(addresses) == 0 {
		for _, n := range nodes {
			addresses = append(addresses, n.Server)
		}
	}
	if opts.MaxOutbounds <= 0 {
		opts.MaxOutbounds = 16
	}
	if opts.PortBase <= 0 {
		opts.PortBase = 18080
	}
	if opts.ProxyHost == "" {
		opts.ProxyHost = "sing-box"
	}

	config := map[string]any{
		"log": map[string]any{"level": "warn", "timestamp": true},
	}
	var inbounds []any
	var outbounds []any
	var rules []any
	var proxies []string
	count := 0
	for _, address := range addresses {
		for _, template := range nodes {
			if count >= opts.MaxOutbounds {
				break
			}
			count++
			tag := fmt.Sprintf("edge-%d", count)
			inboundTag := fmt.Sprintf("http-%d", count)
			port := opts.PortBase + count
			inbounds = append(inbounds, map[string]any{
				"type": "http", "tag": inboundTag, "listen": "0.0.0.0", "listen_port": port,
			})
			outbound := map[string]any{
				"type": "vless", "tag": tag, "server": address, "server_port": template.Port, "uuid": template.UUID,
				"transport": map[string]any{"type": "ws", "path": template.Path, "headers": map[string]string{"Host": template.Host}},
			}
			if template.TLS {
				outbound["tls"] = map[string]any{
					"enabled": true, "server_name": template.ServerName,
					"utls": map[string]any{"enabled": true, "fingerprint": template.Fingerprint},
				}
			}
			outbounds = append(outbounds, outbound)
			rules = append(rules, map[string]any{"inbound": []string{inboundTag}, "outbound": tag})
			proxies = append(proxies, fmt.Sprintf("http://%s:%d", opts.ProxyHost, port))
		}
		if count >= opts.MaxOutbounds {
			break
		}
	}
	if count == 0 {
		return 0, fmt.Errorf("no EdgeTunnel outbounds generated")
	}
	outbounds = append(outbounds, map[string]any{"type": "direct", "tag": "direct"})
	config["inbounds"] = inbounds
	config["outbounds"] = outbounds
	config["route"] = map[string]any{"rules": rules, "final": "direct"}

	encoded, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(parentDir(opts.ConfigFile), 0o755); err != nil {
		return 0, err
	}
	if err := os.WriteFile(opts.ConfigFile, append(encoded, '\n'), 0o600); err != nil {
		return 0, err
	}
	if err := os.WriteFile(opts.ProxyFile, []byte(strings.Join(proxies, "\n")+"\n"), 0o600); err != nil {
		return 0, err
	}
	return count, nil
}

func loadNodes(path string) ([]node, error) {
	lines, err := readLines(path)
	if err != nil {
		return nil, err
	}
	var nodes []node
	for _, line := range lines {
		if !strings.HasPrefix(line, "vless://") {
			continue
		}
		u, err := url.Parse(line)
		if err != nil || u.User == nil {
			continue
		}
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			continue
		}
		q := u.Query()
		if transport := q.Get("type"); transport != "" && transport != "ws" {
			continue
		}
		host := q.Get("host")
		if host == "" {
			host = u.Hostname()
		}
		serverName := q.Get("sni")
		if serverName == "" {
			serverName = host
		}
		path := q.Get("path")
		if path == "" {
			path = "/"
		}
		fingerprint := q.Get("fp")
		if fingerprint == "" {
			fingerprint = "chrome"
		}
		nodes = append(nodes, node{
			UUID: u.User.Username(), Server: u.Hostname(), Port: port, ServerName: serverName,
			Host: host, Path: path, TLS: q.Get("security") == "tls", Fingerprint: fingerprint,
		})
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("%s contains no supported VLESS WebSocket nodes", path)
	}
	return nodes, nil
}

func loadAddresses(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	lines, err := readLines(path)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var addresses []string
	for _, line := range lines {
		value := strings.TrimSpace(strings.SplitN(line, "#", 2)[0])
		host, _, err := net.SplitHostPort(value)
		if err != nil {
			host = value
		}
		if net.ParseIP(host) == nil {
			continue
		}
		if _, ok := seen[host]; !ok {
			seen[host] = struct{}{}
			addresses = append(addresses, host)
		}
	}
	return addresses, nil
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}

func parentDir(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[:i]
	}
	return "."
}
