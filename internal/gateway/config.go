package gateway

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	opencodeVersion   = "1.18.16"
	opencodeUserAgent = "opencode/" + opencodeVersion
	opencodeClient    = "cli"
	opencodeReferer   = "https://opencode.ai/"
	opencodeTitle     = "opencode"
)

type Config struct {
	ListenAddr         string
	UpstreamURL        string
	OpenCodeAPIKey     string
	GatewayKeys        []string
	ProxyURLs          []string
	ProxyListFile      string
	ProxyRefresh       time.Duration
	ProxyProbeURLs     []string
	ProxyProbeWait     time.Duration
	ProxyProbeJobs     int
	TargetEgress       int
	DirectEnabled      bool
	MaxConcurrency     int
	QueueSize          int
	MaxRetries         int
	RequestTimeout     time.Duration
	CooldownBase       time.Duration
	CooldownMax        time.Duration
	OpenCodeClient     string
	OpenCodeVersion    string
	OpenCodeReferer    string
	OpenCodeTitle      string
	FreeModelsOnly     bool
	AdminToken         string
	InstanceAdminToken string
	DataDir            string
}

func DefaultConfig() Config {
	return Config{
		ListenAddr: "0.0.0.0:13339", UpstreamURL: "https://opencode.ai/zen",
		MaxConcurrency: 4, QueueSize: 16, MaxRetries: 1,
		RequestTimeout: 5 * time.Minute, CooldownBase: 5 * time.Second,
		CooldownMax: 60 * time.Second, ProxyRefresh: 30 * time.Second,
		ProxyProbeURLs: []string{"https://api.ipify.org", "https://ifconfig.me/ip", "https://www.cloudflare.com/cdn-cgi/trace"}, ProxyProbeWait: 10 * time.Second,
		ProxyProbeJobs: 8, DirectEnabled: true,
		OpenCodeClient: opencodeClient, OpenCodeVersion: opencodeVersion, OpenCodeReferer: opencodeReferer, OpenCodeTitle: opencodeTitle, FreeModelsOnly: true,
		DataDir: "/data",
	}
}

func LoadConfig() (Config, error) {
	c := DefaultConfig()
	if v := os.Getenv("LISTEN_ADDR"); v != "" {
		c.ListenAddr = v
	}
	if v := os.Getenv("UPSTREAM_URL"); v != "" {
		c.UpstreamURL = strings.TrimRight(v, "/")
	}
	c.OpenCodeAPIKey = strings.TrimSpace(os.Getenv("OPENCODE_API_KEY"))
	if v := os.Getenv("GATEWAY_KEYS"); v != "" {
		c.GatewayKeys = split(v)
	}
	if v := os.Getenv("PROXY_URLS"); v != "" {
		c.ProxyURLs = split(v)
	}
	c.ProxyListFile = strings.TrimSpace(os.Getenv("PROXY_LIST_FILE"))
	if v := os.Getenv("PROXY_REFRESH_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("PROXY_REFRESH_INTERVAL: %w", err)
		}
		c.ProxyRefresh = d
	}
	if v := os.Getenv("PROXY_PROBE_URL"); v != "" {
		c.ProxyProbeURLs = split(v)
	}
	if v := os.Getenv("PROXY_PROBE_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("PROXY_PROBE_TIMEOUT: %w", err)
		}
		c.ProxyProbeWait = d
	}
	if v := os.Getenv("PROXY_PROBE_CONCURRENCY"); v != "" {
		c.ProxyProbeJobs = positiveInt(v, c.ProxyProbeJobs)
	}
	if v := os.Getenv("TARGET_EGRESS_SLOTS"); v != "" {
		c.TargetEgress = nonNegativeInt(v, c.TargetEgress)
	}
	if v := os.Getenv("DIRECT_ENABLED"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("DIRECT_ENABLED: %w", err)
		}
		c.DirectEnabled = b
	}
	if v := os.Getenv("MAX_CONCURRENCY"); v != "" {
		c.MaxConcurrency = positiveInt(v, c.MaxConcurrency)
	}
	if v := os.Getenv("QUEUE_SIZE"); v != "" {
		c.QueueSize = nonNegativeInt(v, c.QueueSize)
	}
	if v := os.Getenv("MAX_RETRIES"); v != "" {
		c.MaxRetries = nonNegativeInt(v, c.MaxRetries)
	}
	if v := os.Getenv("REQUEST_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("REQUEST_TIMEOUT: %w", err)
		}
		c.RequestTimeout = d
	}
	if v := os.Getenv("COOLDOWN_BASE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("COOLDOWN_BASE: %w", err)
		}
		c.CooldownBase = d
	}
	if v := os.Getenv("COOLDOWN_MAX"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("COOLDOWN_MAX: %w", err)
		}
		c.CooldownMax = d
	}
	if v := strings.TrimSpace(os.Getenv("OPENCODE_CLIENT")); v != "" {
		c.OpenCodeClient = v
	}
	if v := strings.TrimSpace(os.Getenv("OPENCODE_VERSION")); v != "" {
		c.OpenCodeVersion = v
	}
	if v := strings.TrimSpace(os.Getenv("OPENCODE_REFERER")); v != "" {
		c.OpenCodeReferer = v
	}
	if v := strings.TrimSpace(os.Getenv("OPENCODE_TITLE")); v != "" {
		c.OpenCodeTitle = v
	}
	if v := os.Getenv("FREE_MODELS_ONLY"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, fmt.Errorf("FREE_MODELS_ONLY: %w", err)
		}
		c.FreeModelsOnly = b
	}
	c.AdminToken = os.Getenv("ADMIN_TOKEN")
	c.InstanceAdminToken = os.Getenv("INSTANCE_ADMIN_TOKEN")
	if c.InstanceAdminToken == "" {
		c.InstanceAdminToken = c.AdminToken
	}
	if v := strings.TrimSpace(os.Getenv("DATA_DIR")); v != "" {
		c.DataDir = v
	}
	if c.UpstreamURL == "" {
		return c, fmt.Errorf("UPSTREAM_URL is required")
	}
	if u, err := url.Parse(c.UpstreamURL); err != nil || u.Scheme == "" || u.Host == "" {
		return c, fmt.Errorf("UPSTREAM_URL must be an absolute URL")
	}
	if len(c.GatewayKeys) == 0 {
		return c, fmt.Errorf("GATEWAY_KEYS is required")
	}
	if c.OpenCodeAPIKey == "" {
		return c, fmt.Errorf("OPENCODE_API_KEY is required")
	}
	for name, value := range map[string]string{"OPENCODE_VERSION": c.OpenCodeVersion, "OPENCODE_REFERER": c.OpenCodeReferer, "OPENCODE_TITLE": c.OpenCodeTitle} {
		if value == "" || strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return c, fmt.Errorf("%s is invalid", name)
		}
	}
	if len(c.OpenCodeAPIKey) > 512 || strings.IndexFunc(c.OpenCodeAPIKey, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return c, fmt.Errorf("OPENCODE_API_KEY is invalid")
	}
	if c.MaxConcurrency < 1 || c.QueueSize < 0 || c.MaxRetries < 0 || c.CooldownBase <= 0 || c.CooldownMax < c.CooldownBase {
		return c, fmt.Errorf("invalid concurrency/retry/cooldown settings")
	}
	if c.ProxyRefresh <= 0 || c.ProxyProbeWait <= 0 || c.ProxyProbeJobs <= 0 {
		return c, fmt.Errorf("proxy refresh/probe durations must be positive")
	}
	if len(c.ProxyProbeURLs) == 0 {
		return c, fmt.Errorf("PROXY_PROBE_URL must contain at least one URL")
	}
	for _, raw := range c.ProxyProbeURLs {
		if u, err := url.Parse(raw); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return c, fmt.Errorf("PROXY_PROBE_URL must contain absolute HTTP(S) URLs")
		}
	}
	if !c.DirectEnabled && len(c.ProxyURLs) == 0 && c.ProxyListFile == "" {
		return c, fmt.Errorf("at least one proxy source is required when DIRECT_ENABLED=false")
	}
	return c, nil
}

func split(v string) []string {
	var out []string
	for _, item := range strings.Split(v, ",") {
		if s := strings.TrimSpace(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}
func positiveInt(v string, fallback int) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}
func nonNegativeInt(v string, fallback int) int {
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return fallback
	}
	return n
}
