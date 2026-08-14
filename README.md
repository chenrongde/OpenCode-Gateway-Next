# OpenCode Gateway Next

OpenCode Gateway Next 是一个面向自托管场景的 OpenCode API 反向代理网关。它提供统一的 OpenAI 兼容入口、多个受控网关实例、Mihomo 协议转换、出口健康探测、有限重试、429 冷却、请求审计和控制台管理。

当前版本：**1.0.0**

> 本项目用于技术研究和个人测试。上游服务的访问策略、模型可用性、速率限制和账户条款以 OpenCode 官方规则为准。增加容器或代理出口不保证提高上游额度，也不应被用于规避服务商的访问限制。

## 功能概览

- OpenAI 兼容接口：`/v1/*`，同时兼容 `/openai/v1/*`、`/anthropic/v1/*`、`/codex/v1/*`。
- 控制台统一创建、启动、停止、重启、设置和删除网关实例。
- 每个实例独立配置 Zen API Key、并发上限、队列容量和代理候选。
- 支持 HTTP、HTTPS、SOCKS5 和 SOCKS5H 代理；实例平时固定当前出口。
- 只有发生上游 429、网络故障、出口 IP 冲突或手动换线时才推进到下一个健康出口。
- Mihomo 接收 Clash/Mihomo 订阅，将 VLESS、Trojan、Shadowsocks、VMess、Hysteria2 等节点转换为多个本地 SOCKS5 端口。
- 异步探测出口公网 IP，失败槽位标记为不可用，重复公网 IP 不进入流量池。
- 按控制面、Mihomo 和实例分类查看系统日志与请求审计。
- 控制台支持浅色/深色模式，左下角显示当前版本并检查 GitHub 最新版本。

## 架构

默认 Compose 只启动两个容器：

```text
浏览器 ──> control-plane :13338  （控制台、实例生命周期、审计）
客户端 ──> control-plane :13337  （统一 API 入口、负载均衡）
                         │
                         ├── 动态 gateway-* 容器 :13339
                         └── mihomo :10801 ...    （可选出口）
```

宿主机 Nginx 应将业务路径反代到 `13337`，将控制台和 `/api/*` 反代到 `13338`。不要直接反代某个实例的 `13339`，否则会绕过控制面负载均衡和审计。

## 快速开始

### 1. 准备配置

```bash
cp config.example.env .env
```

至少设置以下变量：

```env
GATEWAY_KEYS=replace-with-client-key
ADMIN_TOKEN=replace-with-admin-token
INSTANCE_ADMIN_TOKEN=replace-with-instance-token
GATEWAY_IMAGE=opencode-gateway-next-gateway:latest
MAX_INSTANCES=16
```

如需从宿主机修改端口：

```env
API_HOST_PORT=13337
CONTROL_HOST_PORT=13338
```

### 2. 生成网关 Key

网关 Key 用于客户端访问 `13337`，可使用 OpenSSL 生成随机值：

```bash
openssl rand -hex 32
```

将结果写入 `.env` 的 `GATEWAY_KEYS`。多个 Key 使用逗号分隔。 `ADMIN_TOKEN` 和 `INSTANCE_ADMIN_TOKEN` 也应使用独立的强随机值，不能与客户端 Key 混用。

### 3. 启动

```bash
docker compose up -d --build
docker compose ps
```

打开 `http://127.0.0.1:13338/`，输入 `ADMIN_TOKEN`。默认不会创建网关实例；在控制台新建实例并选择公共 Key 或自定义 Zen API Key 后，控制面会动态创建并健康检查容器。

## 控制台使用

1. 在“Mihomo 协议转换”中输入 Clash/Mihomo 订阅并保存。
2. 等待状态显示运行中，点击“检测健康”确认绿色出口。
3. 新建实例，填写 Zen API Key、并发和队列；代理可手工逐行输入，也可以从健康的 Mihomo 端口多选。
4. 保存后在实例表格查看当前节点、真实公网出口 IP、候选数量和模型冷却状态。
5. 发生 429 或网络故障时，网关只在健康槽位之间有限切换；普通请求不会无条件轮询。

Mihomo 面板每页显示 10 个出口，端口按 `10801`、`10802`、`10803` 的顺序编号。控制台不会回显完整订阅 URL，也不会在审计中记录 Zen API Key。

## API 调用

统一入口：

```bash
curl http://127.0.0.1:13337/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hello"}]}'
```

流式请求使用标准 SSE：

```json
{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hello"}],"stream":true}
```

API 端口没有实例时返回 `503 no_gateway_instances`；网关自身队列或并发已满时返回 `429 gateway_overloaded`。上游返回的 429 会记录为 `upstream429`，并按“实例 + 出口 + 模型”进入冷却。

## 出口与健康检查

网关支持：

```text
http://proxy:8080
https://proxy:8443
socks5://proxy:1080
socks5h://mihomo:10801
```

Mihomo 节点先由桥接容器转换，再提供本地 SOCKS5 端口。不要把 `vless://`、`trojan://`、`ss://` 分享链接或 Cloudflare `IP:443` 直接填写到网关代理框；它们不是 HTTP/SOCKS5 代理入口。

启动后网关会先监听端口，再异步探测候选出口。探测失败的槽位会保留在状态中但不参与请求；只要存在至少一个健康出口，实例即可通过健康检查。多个实例使用同一公网 IP 时，控制面会协调后续实例切换到其他健康槽位。

## 宿主机 Nginx 反代

建议路径分流：

```text
/v1/、/openai/、/anthropic/、/codex/  -> 127.0.0.1:13337
/、/api/、/static/                    -> 127.0.0.1:13338
```

外部客户端只需要一个域名，例如：

```text
https://gateway.example.com/v1/chat/completions
```

生产环境请启用 TLS、外部身份认证和请求限流，并限制控制台只对管理员网络开放。

## 并发测试结果

以下结果来自本地测试截图和当时的配置，仅用于说明网关侧容量变化，不代表 OpenCode 上游的固定限制。实际结果会受到 VPS 网络、出口 IP、Zen Key、模型可用性、请求体大小和上游限流影响。

### 初始容量

| 并发 | 成功率 | 平均延迟 | 现象 |
|---:|---:|---:|---|
| 5 | 100% | 4.3 s | 稳定 |
| 10 | 100% → 24% | 12.8 s | 波动较大，网关容量不足 |
| 20 | 10%（加重试 69%） | 13.5 s | 大量 `429 gateway_overloaded` |
| 30 | 40% | 9.2 s | 主要受限于网关排队 |

当并发超过 20 时，主要瓶颈是网关自身队列和并发上限。P50 延迟约 9–13 秒，尾部延迟约 20–66 秒，有效吞吐约 0.2–1.2 req/s。

### 调整网关容量后

| 并发 | 调整前 | 调整后 | 平均延迟变化 |
|---:|---:|---:|---:|
| 10 | 100%，但波动大 | 100% | 12.8 s → 4.3 s |
| 20 | 10% | 100% | 13.5 s → 4.4 s |
| 30 | 40% | 100% | 9.2 s → 8.5 s |
| 40 | — | 90%（4 次 429） | 12.2 s |

本轮测试观察到的稳定区间约为并发 20–30；并发 40 开始出现 429，尾部延迟扩大。测试中的 429 需要结合控制台字段判断来源：`gateway429` 表示网关自身过载，`upstream429` 表示上游返回限流。

## 常见问题

### 订阅保存后仍显示旧节点

使用“清除订阅”会删除旧 provider 缓存；输入新地址后重新保存并等待 Mihomo 重启完成。控制台会在失败时恢复上一版可用配置。

### 出口显示直连

检查实例的 `proxy_urls` 是否包含 `socks5h://mihomo:10801` 等地址，并确认 Mihomo 面板对应端口为绿色健康。若代理最终与 VPS 使用相同公网 IP，控制面会将其识别为重复出口并禁用重复槽位。

### 上游模型返回 429

先查看审计中的出口、模型和 `upstream429` 计数。更换出口只能改变网络路径，不能绕过 Zen Key 或模型本身的额度限制；应降低并发、等待冷却或使用有权限的 Zen API Key。

### 容器创建失败

控制面需要挂载 Docker Socket。使用以下命令检查动态实例：

```bash
docker ps -a --filter label=opencode.gateway.managed=true
docker inspect gateway-a
docker logs --tail=100 gateway-a
```

## 安全说明

- Docker Socket 等同于较高的宿主机管理权限，只应对可信管理员开放控制台。
- 不要把 `.env`、`data/`、`zen-keys.json`、订阅 URL 或真实 Key 提交到 Git。
- 生产环境为控制台配置 TLS、身份认证、IP 白名单和外部限流。
- API Key、Cookie、订阅 Token 不应出现在截图、日志、工单或公开仓库中。
- 项目不会通过无限重试或无条件轮询掩盖上游错误。

## 验证与开发

```bash
gofmt -w ./cmd ./internal
go test -count=1 ./...
go vet ./...
go build ./...
node --check internal/controlplane/web/app.js
docker compose config --quiet
```

## 致谢

本项目在架构调研、协议转换和兼容性测试中参考了以下开源项目，感谢维护者的工作：

- [spfnas/opencode2api-free](https://github.com/spfnas/opencode2api-free)
- [GuJi08233/opencode-free-gate](https://github.com/GuJi08233/opencode-free-gate)
- [ouqiting/ds2api](https://github.com/ouqiting/ds2api)
- [cmliu/edgetunnel](https://github.com/cmliu/edgetunnel)

请在使用这些项目或其衍生代码时分别遵守对应仓库的许可证和使用条款。

## 许可证与免责声明

本仓库的许可证以仓库中的 LICENSE（如有）为准。项目作者不对上游服务可用性、第三方代理质量、账户限制、数据丢失或因配置不当造成的损失负责。
