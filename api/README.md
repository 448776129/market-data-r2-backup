# 行情K线动态接口（Cloudflare Worker）

免费、无需服务器。数据直接读取本仓库 `data/` 下的 CSV，在 Cloudflare 边缘节点解析并转成 JSON 返回。

> 已部署（自定义域名）：**https://stockapi.365200.xyz**
> 首页（项目介绍 + API 文档）：https://stockapi.365200.xyz/

## 接口介绍

本接口是一个 **Cloudflare Worker**，免费托管、无需服务器。它直接读取本仓库（公开仓库）里由 GitHub Actions 生成的 CSV 数据文件，在 Cloudflare 边缘节点解析并转成 JSON 返回，供量化系统调用。

**工作原理：**

```
你的量化系统 ──GET──▶ https://stockapi.365200.xyz/kline
                          │ 根据 symbol 推断市场区域（us/hk/cn）
                          │ 定位 data/{region}/{interval}/{symbol}.csv
                          ▼
                    Cloudflare 边缘节点解析 CSV ──▶ JSON 返回
```

**特点：**

- **免费**：Worker 免费计划（10 万次请求/天）+ 公开仓库，无服务器成本
- **无需 Key**：直接 GET 即可，无需注册、无需 API Key
- **开放 CORS**：支持浏览器跨域直接调用
- **实时映射**：数据由 GitHub Actions 定时增量更新，接口始终返回最新数据
- **多市场**：自动识别美股 / 港股 / A股

## 调用示例

```bash
# 最新 5 条日K（limit 默认返回最新数据）
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&limit=5"

# AAPL 一年日K（2024-01-01 ~ 2024-12-31）
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&interval=1d&start=2024-01-01&end=2024-12-31"

# 港股 0700.HK 最新 100 条 1 小时K，倒序（最新在前）
curl "https://stockapi.365200.xyz/kline?symbol=0700.HK&interval=1h&limit=100&order=desc"

# 5分钟K线（由采集端用 1m 重采样计算）
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&interval=5m&limit=100"

# 15分钟K线（由采集端用 1m 重采样计算）
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&interval=15m&limit=100"

# 半小时K线
curl "https://stockapi.365200.xyz/kline?symbol=600519.SS&interval=30m&limit=100"

# A股 600519.SS 日线
curl "https://stockapi.365200.xyz/kline?symbol=600519.SS&interval=1d"

# 原始CSV（format=csv）
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&format=csv"
```

## 参数

| 参数 | 必填 | 默认 | 说明 |
|------|------|------|------|
| `symbol` | 是 | — | 股票代码，如 `AAPL` / `0700.HK` / `600519.SS` / `000001.SZ` |
| `interval` | 否 | `1d` | `1d`(日线) / `1m`(1分钟) / `5m`(5分钟) / `15m`(15分钟) / `30m`(半小时) / `1h`(1小时) |
| `start` | 否 | — | 起始日期 `YYYY-MM-DD`（含） |
| `end` | 否 | — | 结束日期 `YYYY-MM-DD`（含） |
| `limit` | 否 | 全部 | 最多返回行数；默认返回最新 N 条 |
| `order` | 否 | `asc` | `asc`(时间升序) / `desc`(最新在前) |
| `format` | 否 | `json` | `json`(默认) / `csv` |

> **`limit` 行为**：默认返回时间上最晚的 N 条（最新数据）。配合 `order=desc` 时最新日期排在最前。若省略 `limit`，则返回日期范围内的全部数据。

## 返回格式（JSON）

```json
{
  "symbol": "AAPL",
  "region": "us",
  "interval": "1d",
  "count": 5,
  "order": "asc",
  "data": [
    { "Date": "2026-08-11", "Open": "217.90", "High": "219.70", "Low": "216.30", "Close": "218.70", "Adj Close": "218.70", "Volume": "41000000" }
  ]
}
```

返回的 `data` 元素字段与入库 CSV 列一致：日线含 `Date`，分钟线含 `Datetime`，其余为 `Open / High / Low / Close / Adj Close / Volume`。`interval=5m/15m/30m` 为采集端由 1m 数据重采样计算（`Open`=首根开盘、`High`=区间最高、`Low`=区间最低、`Close`=末根收盘、`Volume`=求和）；`interval=1h` 为雅虎原生小时K线。

> **美股延长时段说明**：`interval=1m/5m/15m/30m` 包含美股盘前/盘后（4:00–20:00 美东）；`1h` 为雅虎原生小时K线，仅含盘中（9:30–16:00），含 `15:30–16:00` 收盘bar（该bar的 `Close` 即 16:00 官方收盘价）。详见仓库根目录 [README.md](../README.md)「数据口径与注意事项」。

## 区域自动识别

代码后缀自动判断市场，无需传 `region`：
- 裸代码（如 `AAPL`）→ US
- `.HK` → 港股
- `.SS` / `.SZ` → A股

## 配额

Cloudflare Workers 免费计划：**10 万次请求/天**，对量化系统个人使用完全够用。

## 部署（约 2 分钟）

1. 注册 [Cloudflare](https://dash.cloudflare.com) 账号（免费）。
2. 安装 [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/)：
   ```bash
   npm install -g wrangler
   ```
3. 登录并部署：
   ```bash
   cd api
   npm install
   wrangler login        # 浏览器授权
   wrangler deploy
   ```
4. 绑定自定义域名 `stockapi.365200.xyz`：
   - 在 [wrangler.toml](wrangler.toml) 已配置 `routes = [{ pattern = "stockapi.365200.xyz", custom_domain = true }]`。
   - 部署后到 Cloudflare Dashboard → Workers，把该 Worker 绑定到域名 `stockapi.365200.xyz`（或确认 DNS 已通过 Cloudflare 托管并指向该 Worker）。
5. 后续更新代码后重新部署：
   ```bash
   cd api
   wrangler deploy
   ```

> **重要**：接口通过 GitHub 公开仓库读取数据，请确保 [market-data-pipeline](https://github.com/448776129/market-data-pipeline) 仓库为 **Public**（否则 404）。数据由你已经配置好的 GitHub Actions 自动更新，接口无需改动。

## 本地开发

```bash
cd api
npm install
wrangler dev          # 本地 http://localhost:8787
```