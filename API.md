# 行情数据 API 文档

本项目提供免费、无需 Key 的行情 K 线数据接口，数据由 **GitHub Actions 自动采集**并存入 **Cloudflare R2**，经 **Cloudflare Workers** 在边缘节点转成 JSON 返回。

- **在线接口**：https://stockapi.365200.xyz
- **文档首页**：https://stockapi.365200.xyz/
- **数据仓库**：[448776129/market-data-r2](https://github.com/448776129/market-data-r2)（公开）

## 数据架构

```
GitHub Actions（采集）
  ├─ Fetch History (Full)    手动触发 · 一次性全量历史（日K 5y / 1m 5d / 1h 6mo / 延长时段）
  ├─ Sync Data (Incremental) 每 30 分钟 · 增量同步（新增数据）
  ├─ Fetch Meta              手动触发 · 基本面 meta
  └─ Fetch News              手动触发 · 新闻
        │  经 Yahoo chart/search API + 反代访问，gzip 压缩
        ▼
Cloudflare R2（存储）
        │
        ▼
Cloudflare Worker（stockapi.365200.xyz）
  ├─ GET /kline       K线数据（日K/1m/5m/15m/30m/1h，走 R2）
  ├─ GET /price       实时价格（当场调取 Yahoo，非缓存）
  ├─ GET /download    下载 gzip 压缩的原始 CSV
  ├─ GET /quote       个股元数据（基本面 meta）
  ├─ GET /news        个股新闻
  ├─ GET /universe    指数/区域股票清单
  ├─ GET /indices     可用清单及成分数量
  ├─ GET /symbols     按区域列出股票代码
  └─ GET /status      服务配置信息
```

## 支持的股票范围

| 市场 | 代码 | 范围 | 数量 |
| ---- | ---- | ---- | ---- |
| 美股 | `us` | Russell 1000（IWB 持仓） | 1022 只 |
| 沪深A股 | `cn` | 沪深全市场（沪+深） | 4595 只 |
| 港股 | `hk` | 恒生指数成分股 | 87 只 |
| 韩股 | `kr` | KOSPI 200 核心成分股 | 48 只 |
| 美股ETF | `etf` | 市值前 500 + 用户主题 ETF | 831 只 |
| 中国ETF | `cn_etf` | 用户指定 A 股 ETF | 211 只 |

## K 线数据

### 请求

```
GET https://stockapi.365200.xyz/kline?symbol=AAPL&interval=1d&limit=5
```

### 参数

| 参数 | 必填 | 默认 | 说明 |
| ---- | ---- | ---- | ---- |
| `symbol` | 是 | — | 股票代码：`AAPL` / `0700.HK` / `600519.SS` / `000001.SZ` / `005930.KS` |
| `interval` | 否 | `1d` | `1d`(日线) / `1m`(1分钟) / `5m`(5分钟) / `15m`(15分钟) / `30m`(半小时) / `1h`(1小时) |
| `start` | 否 | — | 起始日期 `YYYY-MM-DD`（含） |
| `end` | 否 | — | 结束日期 `YYYY-MM-DD`（含） |
| `limit` | 否 | 全部 | 最多返回行数（返回最新 N 条） |
| `order` | 否 | `asc` | `asc`(时间升序) / `desc`(最新在前) |
| `format` | 否 | `json` | `json` / `csv` |

### 响应示例

```json
{
  "symbol": "AAPL",
  "region": "us",
  "interval": "1d",
  "count": 5,
  "order": "asc",
  "data": [
    { "Date": "2026-08-11", "Open": 305.10, "High": 305.66, "Low": 300.57, "Close": 302.25, "Adj Close": 302.25, "Volume": 41657800 }
  ]
}
```

### 字段说明

| 字段 | 说明 |
| ---- | ---- |
| `Date` / `Datetime` | 时间索引。日线为交易日期；分钟线为带时分秒的时间戳（K线起始时间） |
| `Open` / `High` / `Low` / `Close` | OHLC 价格 |
| `Adj Close` | 复权收盘价（分钟K线由 1m 重采样时取区间末根 Close） |
| `Volume` | 成交量（股） |

### 周期与数据深度

| 周期 | 历史深度 | 美股延长时段 |
| ---- | ---- | ---- |
| `1d` | 近 5 年 | ❌ 不含（盘中聚合） |
| `1h` | 近 6 个月 | ✅ 含（4:00–20:00 美东） |
| `1m` | 近 5 天 | ✅ 含（4:00–20:00 美东） |
| `5m` / `15m` / `30m` | 与 1m 一致 | ✅ 含（由 1m 重采样派生） |

> 5m/15m/30m 由采集端用 1m 数据重采样计算：`Open`=首根开盘、`High`=区间最高、`Low`=区间最低、`Close`=末根收盘、`Volume`=求和、`Adj Close`=末根 Close。时间桶对齐到 5/15/30 分钟边界。

## 其它接口

### 实时价格（当场调取 Yahoo）

```
GET https://stockapi.365200.xyz/price?symbol=AAPL
```

返回实时价格、涨跌幅、52周高低、名称、币种等。**不走 R2 数据库**，当场调取 Yahoo chart API，保证最新。

### 个股元数据（基本面 meta）

```
GET https://stockapi.365200.xyz/quote?symbol=600519.SS
```

返回名称、行业、板块、最新价、52周高低、交易所、币种等（采集端经 chart + search 接口采集）。

### 个股新闻

```
GET https://stockapi.365200.xyz/news?symbol=AAPL
```

返回新闻列表（标题、来源、发布时间、链接），采集端经 Yahoo search 接口入库。

### 下载 gzip CSV

```
GET https://stockapi.365200.xyz/download?symbol=AAPL&interval=1h
```

返回 R2 中存储的原始 gzip 压缩 CSV（`Content-Disposition: attachment`），体积小，适合离线分析。

### 股票清单

```
GET https://stockapi.365200.xyz/universe?index=us
GET https://stockapi.365200.xyz/indices
GET https://stockapi.365200.xyz/symbols?region=cn&limit=10
```

## 区域自动识别

代码后缀自动判断市场，无需传 `region`：
- 裸代码（`AAPL`）→ US
- `.HK` → 港股
- `.SS` / `.SZ` → A股
- `.KS` → 韩股

## 配额与限制

- Cloudflare Workers 免费计划：**10 万次请求/天**，个人量化足够。
- 数据在 GitHub Actions 每小时自动增量更新，接口始终返回最新数据。
- 数据仅供学习研究，来自 Yahoo Finance。

## 部署说明

见仓库内 `api/README.md`（Cloudflare Worker 部署）与 `API.md` 的数据采集部分。
