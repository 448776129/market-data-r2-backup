# Market Data Pipeline

通过 **GitHub Actions 采集行情数据 → Cloudflare R2 存储 → Cloudflare Worker 提供 API** 的自动数据管道。

数据经 **Yahoo chart API + 反代** 拉取，gzip 压缩存入 **Cloudflare R2**，由 Worker 边缘分发为 JSON 接口，供量化系统调用。

## 核心特性

- **全自动**：GitHub Actions 定时增量同步，无需人工干预
- **全量 + 增量分离**：
  - `Fetch History (Full)`：手动触发，全量历史入库（日K 5y / 1m 5d / 1h 6mo / 延长时段）
  - `Sync Data (Incremental)`：**唯一定时 action**，每 30 分钟增量同步（含查重）
- **四套数据独立采集**（K 线 / 基本面 meta / 新闻 / 清单，互不混合）
- **多周期**：日K、1m、5m、15m、30m、1h（5m/15m/30m 由 1m 派生）
- **延长时段**：美股 1m/5m/15m/30m/1h 含盘前盘后（4:00–20:00 美东）
- **增量查重**：休市/数据新鲜自动跳过请求，减少 Actions 浪费
- **R2 高效入库**：gzip 压缩 + 多线程并发上传
- **动态接口**：Worker 免费提供 JSON API，无需 Key
- **选股器**：技术指标预计算 → KV 快照 → Worker 毫秒级选股过滤（MA/MACD/RSI/KDJ/布林带等）

## 线上接口

Worker 部署在 Cloudflare Workers，可通过自定义域名访问：
```
https://stocks-api2.365200.xyz
```
备用地址：`https://stocks-api2.wangfugui.workers.dev`

### 接口一览

| 接口 | 功能 | 示例 |
| ---- | ---- | ---- |
| `GET /` | 项目首页 + API 文档 | `/` |
| `GET /kline` | K 线数据（日K/1m/5m/15m/30m/1h） | `/kline?symbol=AAPL&interval=1d&limit=5` |
| `GET /price` | 实时价格（当场调取 Yahoo） | `/price?symbol=AAPL` |
| `GET /screener` | 选股器（读 KV 快照过滤） | `/screener?scope=daily:us&ma5_gt_ma10=true&rsi14_lt=30` |
| `GET /news` | 聚合新闻（雅虎+东方财富） | `/news?limit=50` |
| `GET /news-yh` | 雅虎香港头条（缓存） | `/news-yh?limit=20` |
| `GET /news-em` | 东方财富 7x24h（缓存） | `/news-em?limit=80` |
| `GET /news-yh/live` | 实时拉取雅虎香港头条 | `/news-yh/live?limit=20` |
| `GET /news-em/live` | 实时拉取东方财富 7x24h | `/news-em/live?limit=80` |
| `GET /quote` | 个股元数据 | `/quote?symbol=0700.HK` |
| `GET /download` | 下载 gzip 原始 CSV | `/download?symbol=AAPL&interval=1h` |
| `GET /universe` | 指数成分股清单 | `/universe?index=csi300` |
| `GET /indices` | 全部可用指数 | `/indices` |
| `GET /symbols` | 按区域列出股票代码 | `/symbols?region=us&limit=5` |
| `GET /status` | 服务配置信息 | `/status` |

### 选股器用法

```bash
# 查看选股器参数说明
curl "https://stocks-api2.365200.xyz/screener"

# 日K选股：MA5>MA10（多头）且 RSI14<30（超卖）
curl "https://stocks-api2.365200.xyz/screener?scope=daily:us&ma5_gt_ma10=true&rsi14_lt=30&sort=change_1d&limit=20"

# 数值条件过滤
curl "https://stocks-api2.365200.xyz/screener?scope=daily:us&ma5_gt=300&change_1d_gt=2"
```

选股器支持的过滤条件：
- **数值**：`ma5_gt=300` / `rsi14_lt=30` / `change_1d_gt=2`（支持 gt/gte/lt/lte/eq）
- **布尔**：`ma5_gt_ma10=true` / `macd_gt_signal=true` / `rsi_oversold=true` / `volume_surge=true`
- **排序**：`sort=change_1d` / `order=desc` / `limit=20`（最大 500）

> 选股快照由 GitHub Actions 每 30 分钟预计算写入 KV。首次使用需等 Actions 跑完全量采集 + `screener-precompute` job。

## 股票范围

| 市场 | 代码 | 来源 | 数量 |
| ---- | ---- | ---- | ---- |
| 美股 | `us` | iShares Russell 1000（IWB 持仓） | 1022 只 |
| 沪深A股 | `cn` | 沪市 + 深市全市场 | 4595 只 |
| 港股 | `hk` | 恒生指数成分股 | 87 只 |
| 韩股 | `kr` | KOSPI 200 核心成分股 | 48 只 |
| 美股ETF | `etf` | 市值前 500 + 用户主题 ETF | 831 只 |
| 中国ETF | `cn_etf` | 用户指定 A 股 ETF | 211 只 |

> 清单文件位于 `data/universe/{region}.csv`，由 `scripts/build_universe.py` 生成后提交到仓库。

## 数据流架构

```
GitHub Actions（采集）
  ├─ Fetch History (Full)       ← 手动 · 全量历史
  ├─ Sync Data (Incremental)    ← 每 30 分钟 · 增量
  └─ Fetch Meta / Fetch News    ← 手动 · 基本面 meta / 新闻
        │ Yahoo chart + search API（经反代）→ gzip → 并发上传
        ▼
Cloudflare R2（5G 存储）
  ├─ universe/{region}.csv                # 股票清单（不压缩）
  ├─ {region}/kline*/{symbol}.csv.gz      # K线（6 周期分目录）
  ├─ {region}/meta/{symbol}.json          # 基本面 meta
  ├─ {region}/news/{symbol}.json          # 新闻
  └─ _status.json                         # 采集状态
        │
        ▼
Cloudflare Worker（stockapi.365200.xyz）
  /kline /price /download /quote /news /universe /indices /symbols /status
```

## 目录结构

```
.
├── config.py                     # 区域、股票范围、反代、交易时段配置
├── requirements.txt              # Python 依赖
├── scripts/
│   ├── fetch_history.py          # 批量历史全量入库 R2（手动）
│   ├── sync_incremental.py       # 增量同步入库 R2（定时）
│   ├── fetch_meta.py             # 基本面 meta 采集（独立于 K 线）
│   ├── fetch_news.py             # 新闻采集（独立于 K 线 / meta）
│   ├── r2store.py                # Cloudflare R2 存储客户端（gzip + 并发）
│   ├── yahoo_chart.py            # Yahoo chart API 客户端（K线，经反代）
│   ├── yahoo_meta.py             # Yahoo meta 采集（名称/行情快照 + 板块行业）
│   ├── yahoo_news.py             # Yahoo search 新闻采集（经反代）
│   ├── build_universe.py         # 从本地文件生成各区域股票清单
│   ├── marketlib.py              # 共享工具（列表解析 + 分批 + 交易时段）
│   ├── indicators.py             # 技术指标计算引擎（纯计算：MA/EMA/MACD/RSI/KDJ/布林带等）
│   └── test_indicators.py        # 指标交叉验证测试（与 pandas_ta 逐项比对）
├── api/                          # Cloudflare Worker 动态接口
│   ├── src/index.js              # 从 R2 读取（fallback GitHub raw）
│   └── wrangler.toml             # R2 binding 配置
├── API.md                        # API 使用文档
├── SECRETS.md                    # 部署凭据说明（勿提交真实密钥）
└── .github/workflows/
    ├── fetch_history.yml         # 全量历史（手动）
    ├── sync_data.yml             # 增量同步（每 30 分钟）
    └── fetch_meta.yml            # meta 采集（手动）
```

## 技术指标引擎（新增）

`scripts/indicators.py` 提供纯计算技术指标，供选股器预计算使用：

| 指标 | 函数 | 默认参数 |
| ---- | ---- | ---- |
| 简单移动平均 | `ma(close, period)` | 5/10/20/60 |
| 指数移动平均 | `ema(close, period)` | 12/26（TA Lib 风格，SMA 种子） |
| MACD | `macd(close)` | 12/26/9 |
| RSI | `rsi(close, period)` | 14（Wilder 平滑） |
| KDJ | `kdj(high, low, close)` | 9/3/3 |
| 布林带 | `bollinger(close)` | 20, 2.0 (ddof=1) |
| 成交量均线 | `volume_ma(volume, period)` | 5/20 |
| 涨跌幅 | `price_change(close, periods)` | 1日/5日/20日 |
| 全套指标 | `compute_all(df)` | MA/EMA/MACD/RSI/KDJ/BB/成交量/涨跌幅 |

**正确性保证**：`scripts/test_indicators.py` 用 AAPL 真实日K + 合成分钟数据，
与 `pandas_ta` 逐项交叉验证（35/35 通过，max_diff ≤ 1.1e-13），
并覆盖边界条件（数据不足返回全 NaN、单元素/空数据不崩溃）。

```bash
# 运行指标验证
pip install -r requirements.txt
python scripts/test_indicators.py
```

> 计算口径与 pandas_ta 完全对齐（EMA 用 SMA 种子 + adjust=False；
> RSI 用 `ewm(alpha=1/n, adjust=False)`；KDJ 用 `pd_rma`；布林带 ddof=1）。

## 选股器架构（设计稿）

面向"减少 Workers 额度 + 日K/分钟K选股"的 KV 快照方案：

```
┌─ GH Actions 每日 ─────────────────────────────┐
│  全量日K/小时K同步 → 计算指标                  │
│  → 写 KV: screener:daily:{region}             │
├───────────────────────────────────────────────┤
│  额度：约 150 分钟/月                          │
└───────────────────────────────────────────────┘

┌─ GH Actions 每 10 分钟 ───────────────────────┐
│  候选池分钟K同步 → 计算指标                    │
│  → 写 KV: screener:watchlist:{interval}       │
├───────────────────────────────────────────────┤
│  额度：约 720 分钟/月                          │
└───────────────────────────────────────────────┘

┌─ Worker ──────────────────────────────────────┐
│  /screener → 读 KV 快照 → 内存过滤 → 返回      │
│  1 次 KV 读，<1ms CPU，0 次 R2 读              │
└───────────────────────────────────────────────┘
```

要点：
- **KV 存快照**（全量读 + 内存过滤），不逐只查 R2（免费 KV 写入 1000 次/天足够）
- **日K选股**：每日更新一次，覆盖全市场
- **分钟K选股**：候选池 + 高频更新，实时监控
- 存储优化规划：分钟K按时间分区（`{date}.csv.gz` 一文件一市场一天），文件数从 3 万降至 ~120

## 配置（首次部署）

### 1. GitHub Secrets

仓库 **Settings → Secrets and variables → Actions** 配置：

| Secret | 值 |
| ---- | ---- |
| `R2_ACCOUNT_ID` | Cloudflare 账户 ID |
| `R2_ACCESS_KEY_ID` | R2 S3 API Access Key ID |
| `R2_SECRET_ACCESS_KEY` | R2 S3 API Secret Access Key |
| `R2_BUCKET` | R2 bucket 名（如 `stocksmarkets`） |
| `ALERT_WEBHOOK_URL` | （可选）失败告警 webhook |

### 2. 首次全量入库

Actions 页手动触发 `Fetch History (Full)`，`region=all` 拉取全部市场（大区域自动分批）。

> 首次运行拉取全部历史写入 R2，之后由增量 action 自动更新。

### 3. 增量 / meta / 新闻

- 增量同步已配置**每 30 分钟**定时运行。
- `Fetch Meta`：手动触发采集基本面 meta。
- 新闻采集：运行 `python scripts/fetch_news.py --region <区域>`。

## 本地运行

```bash
pip install -r requirements.txt

# 配置 R2 凭据
export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=stocksmarkets

# 全量历史入库 R2
python scripts/fetch_history.py --region us

# 增量同步
python scripts/sync_incremental.py --region us

# 基本面 meta 采集
python scripts/fetch_meta.py --region us

# 新闻采集
python scripts/fetch_news.py --region us
```

## 数据口径说明

- **时间戳按 K 线起始时间标注**：1h 最后一根标为 `15:30`（覆盖 15:30–16:00），`Close` 即 16:00 官方收盘价。
- **美股延长时段**：1m/5m/15m/30m/1h 含盘前盘后（4:00–20:00 美东）；1d 不含。
- **派生周期**：5m/15m/30m 由 1m 重采样（Open 首 / High 最高 / Low 最低 / Close 末 / Volume 求和）。
- **反代访问**：国内直连 Yahoo 被 403，所有请求经 `config.YAHOO_CHART_PROXY`（`https://img2.365200.xyz`）转发。
- **基本面 meta 说明**：Yahoo quoteSummary（市值/PE/财务等）对免费通道系统性 429 限流，meta 采集使用 chart API（名称/行情快照）+ search 接口（板块/行业/证券类型），不含受限的财务明细字段。
- **新闻数据**：来自 Yahoo `/v1/finance/search`（标题/来源/发布时间），独立存于 `{region}/news/`。

## API 使用

完整接口文档见 [API.md](API.md)。快速开始：

```bash
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&interval=1d&limit=5"    # 历史K线（R2）
curl "https://stockapi.365200.xyz/price?symbol=AAPL"                         # 实时价格（当场调Yahoo）
curl "https://stockapi.365200.xyz/quote?symbol=600519.SS"                    # 基本面 meta
curl "https://stockapi.365200.xyz/news?symbol=AAPL"                          # 新闻
curl "https://stockapi.365200.xyz/download?symbol=AAPL&interval=1h"          # 下载 gzip CSV
curl "https://stockapi.365200.xyz/universe?index=etf"                        # 成分股清单
```

## 说明

- 数据仅供学习研究，来自 Yahoo Finance。
- R2 免费 10GB 存储（gzip 后约 1~2GB），Worker 免费 10 万次/天。
- 全量跑完后增量只拉新增，重复运行不会重复写入。
