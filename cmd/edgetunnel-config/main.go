package main

import (
	"log"
	"opencode-gateway-next/internal/edgetunnel"
	"os"
	"strconv"
)

func main() {
	count, err := edgetunnel.Generate(edgetunnel.Options{
		NodesFile:    env("EDGETUNNEL_NODES_FILE", "/config/nodes.txt"),
		IPsFile:      env("EDGETUNNEL_IPS_FILE", "/config/500ip.txt"),
		ConfigFile:   env("SINGBOX_CONFIG_FILE", "/generated/sing-box.json"),
		ProxyFile:    env("PROXY_LIST_FILE", "/generated/proxies.txt"),
		ProxyHost:    env("SINGBOX_HOST", "sing-box"),
		PortBase:     envInt("SINGBOX_HTTP_PORT_BASE", 18080),
		MaxOutbounds: envInt("EDGETUNNEL_MAX_CANDIDATES", 16),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("generated %d EdgeTunnel outbounds", count)
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
