# 服务器自部署版

用你自己的云服务器 + crontab 替代 **GitHub Actions** 完成全部行情采集，替代/补充 **Cloudflare Worker** 提供接口。

## 为什么自部署

| 维度 | GitHub Actions | 服务器 crontab |
|:----|:----:|:----:|
| 分钟K频率 | 最低 5 分钟（硬限制）| **1 分钟** |
| 配额 | 2000 分钟/月（分钟K超 20 倍）| **无配额** |
| Yahoo 访问 | 出口 IP 可能被限流（429）| 固定 IP 直连稳定 |
| 反代依赖 | 需 img2.365200.xyz | **直连 Yahoo（海外）** |

## 目录结构

```
服务器自部署版/
├── README.md              # 本说明
├── deploy.sh              # 一键部署脚本（装依赖+拉代码+配cron）
├── crontab.conf           # 全部采集任务（cron 表达式 + 脚本）
├── .env.example           # 环境变量模板（复制为 /opt/market-data-r2/.env）
└── requirements-server.txt # 服务器依赖（仅 pandas；r2s3 用标准库 SigV4）
```

## 快速开始

```bash
# 0. 前置：机器不在此项目仓库（本目录文件需自行拷贝到服务器 /opt/market-data-r2）
#    或服务器上 git clone https://github.com/448776129/market-data-r2-backup.git

# 1. 在服务器上执行
cd /opt/market-data-r2
cp 服务器自部署版/.env.example .env
vim .env          # 填入 R2/KV 凭据（stocks-api2 / stocksAPI2）
bash 服务器自部署版/deploy.sh

# 2. 首次全量历史入库（服务器直连，很快）
export $(grep -v '^#' .env | xargs)
python3 scripts/fetch_history.py --region all

# 3. 确认 cron 生效
crontab -l
```

## 部署清单

| 任务 | 频率 | 脚本 | 说明 |
|:----|:----|:----|:----|
| 沪深300 分钟K | 每 1 分钟 | `sync_minute_realtime_pure.py --index csi300 --region cn` | 直连 Yahoo |
| 纳指100 分钟K | 每 1 分钟 | `sync_minute_realtime_pure.py --index nasdaq100 --region us` | 直连 Yahoo |
| 聚合新闻 | 每 30 分钟 | `fetch_agg_news.py` | 直连 Yahoo HK/东财 |
| A股日K | 每天 07:10 UTC | `sync_incremental.py --region cn` | 收盘后 |
| 美股日K 盘前 | 每天 09:00 UTC | `sync_incremental.py --region us` | 美东 04:00 |
| 美股日K 盘中 | 每天 13:00 UTC | `sync_incremental.py --region us` | 美东 08:00 |
| 美股日K 盘后 | 每天 21:40 UTC | `sync_incremental.py --region us` | 美东 16:40 |
| 港股/韩股/ETF | 每天 21:40 UTC | `sync_incremental.py --region hk/kr/etf/cn_etf` | 随美股盘后 |

> 分钟K脚本内已自动跳过非交易时段（`_is_market_session`），cron 每 1 分钟触发不会浪费请求。

## 接口服务（可选）

采集完成后数据落在 R2 `stocks-api2` / KV `stocksAPI2`，有两种对外方式：

- **保留 Cloudflare Worker**：`stocks-api2.365200.xyz` 已绑定，读 R2/KV（免费额度充足）
- **自部署 FastAPI**：`api_server.py` 用 r2s3/kvstore/indicators_pure 提供同接口

## 环境变量（.env）

参见 `.env.example`。关键项：

| 变量 | 值 |
|:----|:----|
| `R2_ACCOUNT_ID` | `8e43ef2043266e0898cf9e02ca53df2f` |
| `R2_ACCESS_KEY_ID` | 新账号 Access Key |
| `R2_SECRET_ACCESS_KEY` | 新账号 Secret |
| `R2_BUCKET` | `stocks-api2` |
| `CLOUDFLARE_API_TOKEN` | 新账号 API Token（KV 写入用）|
| `CLOUDFLARE_ACCOUNT_ID` | `8e43ef2043266e0898cf9e02ca53df2f` |
| `KV_NAMESPACE_ID` | `6ec220d973654f7981364ac1340863df` |
| `YAHOO_DIRECT` | `1`（分钟K直连 Yahoo）|
| `YAHOO_USE_PROXY` | 空（其他脚本直连；国内填 `1` 走反代）|