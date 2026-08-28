# 服务器版 TradingView 服务部署

常驻进程采集 TradingView 数据 + FastAPI 接口，**分钟级实时**（比 GitHub Actions 的 30 分钟快得多）。

## 文件说明

| 文件 | 用途 |
|:----|:----|
| `server_tv.py` | 主服务：后台采集线程 + FastAPI 接口 |
| `server_tv_requirements.txt` | Python 依赖 |
| `deploy_server_tv.sh` | 一键部署脚本（venv + 启动）|
| `Dockerfile.server_tv` | Docker 镜像（参考 stocks-API 方式）|

## 接口

| 接口 | 功能 | 示例 |
|:----|:----|:----|
| `GET /kline` | K 线数据（日K/1m/5m/15m/30m/1h/周K/月K）| `/kline?symbol=AAPL&interval=1d&limit=5` |
| `GET /kline` | CSV 格式 | `/kline?symbol=AAPL&interval=1h&format=csv` |
| `GET /status` | 服务信息与采集状态 | `/status` |
| `GET /` | 服务首页 | `/` |

## 配置环境变量

| 变量 | 默认 | 说明 |
|:----|:----|:----|
| `R2_ACCOUNT_ID` | 必填 | Cloudflare 账户 ID |
| `R2_ACCESS_KEY_ID` | 必填 | R2 Access Key |
| `R2_SECRET_ACCESS_KEY` | 必填 | R2 Secret |
| `R2_BUCKET` | `stocks-tv` | R2 bucket（TradingView 独立）|
| `TV_SYNC_INTERVAL` | `1` | 采集间隔（分钟）|
| `TV_REGIONS` | `us` | 采集区域（逗号分隔）|
| `PORT` | `3216` | 服务端口 |

## 部署方式 A：脚本部署（推荐）

```bash
# 在服务器上
cd /opt/market-data-r2
bash deploy_server_tv.sh
# 首次会生成 .env 模板，填 R2 凭据后重跑
vim .env
bash deploy_server_tv.sh
```

## 部署方式 B：Docker

```bash
# 在服务器上
cd /opt/market-data-r2
docker build -f Dockerfile.server_tv -t stocks-tv .
docker run -d --name stocks-tv \
  -p 3216:3216 \
  -e R2_ACCOUNT_ID=... -e R2_ACCESS_KEY_ID=... -e R2_SECRET_ACCESS_KEY=... \
  -e R2_BUCKET=stocks-tv -e TV_SYNC_INTERVAL=1 \
  --restart unless-stopped \
  stocks-tv
```

## 部署方式 C：1Panel Python 项目（参考 stocks-API）

在 1Panel 面板创建 Python 项目：
1. 源码目录: `/opt/market-data-r2`
2. 启动命令: `bash deploy_server_tv.sh`（或直接 `uvicorn server_tv:app --host 0.0.0.0 --port 3216`）
3. 环境变量: 按上面表格配置
4. 端口: 3216

## 与 GitHub Actions 的关系

- **GitHub Actions**（`sync_tv.yml`）：保留，作为兜底（每 30 分钟）
- **服务器常驻**：主力，分钟级实时
- 两者写同一个 R2 `stocks-tv`，**不冲突**（merge 去重）

## 数据流向

```
服务器常驻进程（每1分钟）
  └─ tvdatafeed 拉取 → 增量合并 → R2 stocks-tv
                                      ↓
FastAPI /kline → 读 R2 stocks-tv → 返回
```