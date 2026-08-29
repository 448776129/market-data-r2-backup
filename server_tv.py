"""TradingView 服务器版：常驻采集 + FastAPI 接口（单文件）。

部署在服务器（参考 stocks-API 方式），常驻进程实现分钟级实时采集：
  - 后台线程：每 N 分钟用 tvdatafeed 拉美股全周期 → R2 stocks-tv
  - FastAPI：/kline /status 接口读 R2 stocks-tv

用法（服务器）：
    pip install fastapi uvicorn tvdatafeed boto3  # 或:
    pip install -r server_tv_requirements.txt

    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=...
    export R2_BUCKET=stocks-tv
    export TV_SYNC_INTERVAL=1       # 采集间隔分钟（默认 1）
    export TV_REGIONS=us            # 采集区域（默认 us）

    # 启动（参考 stocks-API deploy.sh）
    uvicorn server_tv:app --host 0.0.0.0 --port 3216

Docker 部署参考 Dockerfile 部分（底部注释）。
"""

from __future__ import annotations

import gzip
import io
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# 项目根目录（server_tv.py 位于仓库根）
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import r2s3  # noqa: E402  项目内极简 R2 客户端

from tvDatafeed import Interval, TvDatafeed  # noqa: E402

# ── 双池配置 ────────────────────────────────────────────────────
# 高频池（行情池）：小范围、分钟级实时采集，用于实时选股/监控
# 低频池（存档池）：全市场、低频深采，用于历史/回测
HIGH_POOL = {
    "us": {
        "file": "nasdaq100.csv",   # 纳指100 ~101只（美股高频池）
        "intervals": ["1m", "5m", "15m", "30m", "1h"],
        "n_bars": 2000,            # 分钟级增量拉取
        "sync_min": 1,             # 每 1 分钟
    },
}
LOW_POOL = {
    "us": {
        "file": "us.csv",          # 全市场 1022 只（美股存档池）
        "intervals": ["1d", "1h", "1wk", "1mo"],
        "n_bars": 1500,            # 历史深度
        "sync_min": 30,            # 每 30 分钟
    },
}

# 池类型（环境变量）：TV_POOL_TYPE=high / low / both
POOL_TYPE = os.environ.get("TV_POOL_TYPE", "high")
SYNC_INTERVAL_MIN = int(os.environ.get("TV_SYNC_INTERVAL", "1"))
SYNC_REGIONS = os.environ.get("TV_REGIONS", "us").split(",")
R2_BUCKET = os.environ.get("R2_BUCKET", "stocks-tv")
# 每轮处理的股票数（分批轮询，防止 TV WS 断连）
BATCH_SIZE = int(os.environ.get("TV_BATCH_SIZE", "60"))
# 批次间隔秒（给 WS 喘息，防止 RateLimit）
BATCH_DELAY_SEC = float(os.environ.get("TV_BATCH_DELAY", "2"))

# TradingView 周期映射：名称 -> (tvdatafeed Interval, R2 子目录)
TV_INTERVALS = {
    "1d": (Interval.in_daily, "kline"),
    "1m": (Interval.in_1_minute, "kline_1m"),
    "5m": (Interval.in_5_minute, "kline_5m"),
    "15m": (Interval.in_15_minute, "kline_15m"),
    "30m": (Interval.in_30_minute, "kline_30m"),
    "1h": (Interval.in_1_hour, "kline_1h"),
    "1wk": (Interval.in_weekly, "kline_1wk"),
    "1mo": (Interval.in_monthly, "kline_1mo"),
}

DEFAULT_BARS = {
    "1d": 320,     # 完整日K（常驻后增量，首次拉 320 根）
    "1m": 1200,    # 近 20 小时 1m
    "5m": 1200,
    "15m": 1200,
    "30m": 1200,
    "1h": 1200,
    "1wk": 300,
    "1mo": 100,
}

# 美股交易所前缀（自动探测）
US_EXCHANGES = ["NYSE", "NASDAQ", "AMEX"]


# ── 工具函数 ────────────────────────────────────────────────────

def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as f:
        f.write(data)
    return buf.getvalue()


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.10g}"
    return str(v)


def df_to_csv(df) -> str:
    """tvdatafeed DataFrame → CSV 文本（兼容现有格式，带表头）。"""
    if df is None or len(df) == 0:
        return ""
    lines = ["Datetime,Open,High,Low,Close,Adj Close,Volume"]
    for idx, row in df.iterrows():
        dt_str = idx.strftime("%Y-%m-%d %H:%M:%S") if hasattr(idx, "strftime") else str(idx)
        lines.append(",".join([
            dt_str,
            _fmt(row["open"]), _fmt(row["high"]), _fmt(row["low"]), _fmt(row["close"]),
            _fmt(row["close"]),  # TradingView 无 adjclose，用 close 填充
            _fmt(row["volume"]),
        ]))
    return "\n".join(lines) + "\n"


def put_csv_gz(region: str, symbol: str, subdir: str, csv_text: str) -> None:
    key = f"{region}/{subdir}/{symbol}.csv"
    payload = ("\ufeff" + csv_text).encode("utf-8")
    r2s3.put_obj(key, gzip_bytes(payload),
                 content_type="text/csv; charset=utf-8", content_encoding="gzip")


def load_csv_gz(region: str, symbol: str, subdir: str) -> str | None:
    """读已有 CSV（用于增量合并）。返回文本或 None。"""
    key = f"{region}/{subdir}/{symbol}.csv"
    raw = r2s3.get_obj(key)
    if raw is None:
        return None
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def merge_csv(existing: str | None, new_csv: str) -> str:
    """合并新旧 CSV（按首列时间戳去重，保留新数据）。"""
    if not existing:
        return new_csv
    ex_lines = [l for l in existing.strip().splitlines() if l.strip()]
    new_lines = [l for l in new_csv.strip().splitlines() if l.strip()]
    if len(ex_lines) < 2:
        return new_csv

    merged = {}  # 时间戳 -> 行
    for line in ex_lines[1:]:
        merged[line.split(",")[0]] = line
    for line in new_lines[1:]:
        merged[line.split(",")[0]] = line

    header = new_lines[0] if new_lines else ex_lines[0]
    out = [header] + [merged[k] for k in sorted(merged.keys())]
    return "\n".join(out) + "\n"


# ── 采集 ────────────────────────────────────────────────────────

def _pool_symbols(region: str, file: str) -> list[str]:
    """从指定 universe 文件加载股票代码。"""
    uni_file = ROOT / "data" / "universe" / file
    if not uni_file.exists():
        print(f"[WARN] universe 文件不存在: {uni_file}")
        return []
    return [line.strip() for line in uni_file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")]


def fetch_one(tv, region: str, symbol: str, interval: str, exchanges: list[str],
              n_bars: int | None = None) -> dict:
    """拉取单只股票单周期并增量合并写入 R2。"""
    tv_interval, subdir = TV_INTERVALS[interval]
    if n_bars is None:
        n_bars = DEFAULT_BARS[interval]
    result = {"symbol": symbol, "interval": interval, "status": "ok", "bars": 0}

    for exchange in exchanges:
        try:
            df = tv.get_hist(symbol=symbol, exchange=exchange,
                             interval=tv_interval, n_bars=n_bars)
            if df is not None and len(df) > 0:
                new_csv = df_to_csv(df)
                existing = load_csv_gz(region, symbol, subdir)
                merged = merge_csv(existing, new_csv)
                put_csv_gz(region, symbol, subdir, merged)
                result["bars"] = len(df)
                return result
        except Exception:
            continue
    result["status"] = "no_data"
    return result


def sync_pool(tv, pool_name: str) -> dict:
    """同步一个池（高频或低频）。

    pool_name: "high" / "low"
    """
    pool_cfg = HIGH_POOL if pool_name == "high" else LOW_POOL
    ok = skip = 0
    total_symbols = 0
    for region in SYNC_REGIONS:
        cfg = pool_cfg.get(region)
        if not cfg:
            continue
        symbols = _pool_symbols(region, cfg["file"])
        if not symbols:
            continue
        total_symbols += len(symbols)
        intervals = cfg["intervals"]
        n_bars = cfg["n_bars"]
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
              f"{pool_name}池 {region} 同步 {len(symbols)} 只, 周期={intervals}",
              flush=True)
        # 分批轮询：防止 TV WS 断连
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            for interval in intervals:
                for sym in batch:
                    try:
                        r = fetch_one(tv, region, sym, interval, US_EXCHANGES, n_bars)
                        if r["status"] == "ok":
                            ok += 1
                        else:
                            skip += 1
                    except Exception:  # noqa: BLE001
                        skip += 1
            if i + BATCH_SIZE < len(symbols):
                time.sleep(BATCH_DELAY_SEC)
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
              f"{pool_name}池 {region} 完成: ok={ok} skip={skip}",
              flush=True)
    return {"pool": pool_name, "symbols": total_symbols, "ok": ok, "skip": skip}


def sync_loop_once(pool_type: str) -> None:
    """执行一轮同步（按池类型）。"""
    try:
        tv = TvDatafeed()
        if pool_type == "both":
            sync_pool(tv, "low")
            sync_pool(tv, "high")
        else:
            sync_pool(tv, pool_type)
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
              f"{pool_type}池 本轮同步完成", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] 同步异常: {exc}", flush=True)


# ── 后台采集线程 ────────────────────────────────────────────────

_sync_thread: threading.Thread | None = None
_sync_running = False

# 各池的同步间隔（分钟）：高频 1 分钟，低频 30 分钟
_POOL_INTERVAL_MIN = {"high": 1, "low": 30}


def _worker():
    global _sync_running
    _sync_running = True
    last_sync: dict[str, float] = {}
    # 启动先各同步一次
    for pool in (["low", "high"] if POOL_TYPE == "both" else [POOL_TYPE]):
        sync_loop_once(pool)
        last_sync[pool] = time.time()
    while _sync_running:
        time.sleep(5)  # 轻量轮询
        for pool in (["low", "high"] if POOL_TYPE == "both" else [POOL_TYPE]):
            interval = _POOL_INTERVAL_MIN.get(pool, 30) * 60
            if time.time() - last_sync.get(pool, 0) >= interval:
                sync_loop_once(pool)
                last_sync[pool] = time.time()


def start_background_sync() -> None:
    """启动后台采集线程（幂等）。"""
    global _sync_thread
    if _sync_thread and _sync_thread.is_alive():
        return
    print(f"启动后台采集线程: 池类型={POOL_TYPE} "
          f"(高频1分钟/低频30分钟)", flush=True)
    _sync_thread = threading.Thread(target=_worker, daemon=True)
    _sync_thread.start()


# ── FastAPI 接口 ────────────────────────────────────────────────

def create_app():
    from fastapi import FastAPI, Query
    from fastapi.responses import JSONResponse, PlainTextResponse

    app = FastAPI(title="TradingView 行情 API", description="服务器版常驻采集 + 接口")

    WEB_HTML = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradingView 行情</title>
<style>
body{font-family:system-ui,-apple-system,sans-serif;margin:0;background:#0f1420;color:#e8ecf4}
.header{padding:16px 24px;background:#1a2234;border-bottom:1px solid #2a3550;display:flex;align-items:center;gap:12px}
.header h1{font-size:18px;margin:0;color:#4d9fff}
.header .sub{font-size:12px;color:#8b96ad}
.wrap{max-width:1200px;margin:0 auto;padding:20px 24px}
.card{background:#161e30;border:1px solid #26324a;border-radius:10px;padding:16px;margin-bottom:16px}
.card h3{margin:0 0 12px;font-size:14px;color:#9fb4d8}
.grid{display:grid;grid-template-columns:2fr 1fr;gap:16px}
@media(max-width:900px){.grid{grid-template-columns:1fr}}
input,select,button{padding:8px 10px;border-radius:6px;border:1px solid #33405e;background:#1d2740;color:#e8ecf4;font-size:13px}
button{cursor:pointer;background:#2563eb;border-color:#2563eb;font-weight:600}
button:hover{background:#3b82f6}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:8px}
canvas{width:100%;height:340px;background:#111827;border-radius:8px;margin-top:8px}
#quote{font-size:22px;font-weight:700}
#quote .chg-up{color:#22c55e}#quote .chg-down{color:#ef4444}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #222c44}
th{color:#7d8db0;font-weight:500}
.ch-up{color:#22c55e}.ch-down{color:#ef4444}
.news-item{padding:8px 0;border-bottom:1px solid #222c44;font-size:13px}
.news-item .t{color:#e8ecf4}.news-item .src{color:#6b7ba0;font-size:11px;margin-left:8px}
.news-item .time{color:#556383;font-size:11px}
.badge{display:inline-block;padding:1px 6px;border-radius:4px;font-size:11px;background:#1d2740;color:#9fb4d8;margin-left:6px}
</style></head><body>
<div class="header">
  <h1>📈 TradingView 行情</h1>
  <span class="sub">服务器常驻采集 · 分钟级实时 · 数据源 TradingView</span>
</div>
<div class="wrap">
<div class="grid">
<div>
  <div class="card">
    <h3>K线查询</h3>
    <div class="row">
      <input id="sym" placeholder="股票代码 e.g. AAPL" value="AAPL">
      <select id="itv">
        <option value="1m">1分钟</option><option value="5m">5分钟</option>
        <option value="15m">15分钟</option><option value="30m">30分钟</option>
        <option value="1h" selected>1小时</option><option value="1d">日K</option>
        <option value="1wk">周K</option><option value="1mo">月K</option>
      </select>
      <input id="bars" placeholder="根数" value="120" style="width:60px">
      <button onclick="loadKline()">查询</button>
    </div>
    <div id="quote" style="margin:8px 0"></div>
    <canvas id="cv"></canvas>
  </div>
  <div class="card">
    <h3>最近行情</h3>
    <table id="screener-tbl"><thead><tr>
      <th>代码</th><th>价格</th><th>涨跌%</th><th>成交量</th>
    </tr></thead><tbody></tbody></table>
  </div>
</div>
<div>
  <div class="card">
    <h3>实时快讯</h3>
    <div class="row">
      <select id="news-src">
        <option value="all">全部</option><option value="yh">雅虎</option>
        <option value="em">东财</option><option value="ths">同花顺</option>
        <option value="sina">新浪</option><option value="jin10">金十</option>
      </select>
      <button onclick="loadNews()">刷新</button>
    </div>
    <div id="news"></div>
  </div>
</div>
</div>
</div>
<script>
const API='/';
function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}
function drawChart(rows){
  const cv=document.getElementById('cv'),ctx=cv.getContext('2d');
  const W=cv.width=Math.floor(cv.clientWidth*2),H=cv.height=680;
  ctx.clearRect(0,0,W,H);
  if(!rows||rows.length<2){ctx.fillStyle='#8b96ad';ctx.fillText('暂无数据',20,30);return}
  const closes=rows.map(r=>parseFloat(r.Close)), highs=rows.map(r=>parseFloat(r.High)), lows=rows.map(r=>parseFloat(r.Low));
  const min=Math.min(...lows),max=Math.max(...highs),range=max-min||1;
  const pad=40,xs=W/(rows.length),y=v=>H-((v-min)/range)*(H-pad*2)-pad;
  // 网格
  ctx.strokeStyle='#222c44';ctx.fillStyle='#6b7ba0';ctx.font='12px sans-serif';
  for(let i=0;i<5;i++){const gy=pad+(H-2*pad)*i/4;ctx.beginPath();ctx.moveTo(0,gy);ctx.lineTo(W,gy);ctx.stroke();
    const v=max-range*i/4;ctx.fillText(v.toFixed(2),5,gy-4)}
  // 蜡烛
  rows.forEach((r,i)=>{
    const o=parseFloat(r.Open),c=parseFloat(r.Close),h=parseFloat(r.High),l=parseFloat(r.Low);
    const up=c>=o,x=(i+0.5)*xs,upc='#22c55e',dnc='#ef4444',col=up?upc:dnc;
    ctx.strokeStyle=col;ctx.fillStyle=col;
    ctx.beginPath();ctx.moveTo(x,y(h));ctx.lineTo(x,y(l));ctx.stroke();
    const bw=Math.max(2,xs*0.6),top=Math.min(y(o),y(c)),bh=Math.max(1,Math.abs(y(o)-y(c)));
    ctx.fillRect(x-bw/2,top,bw,bh);
  });
}
async function loadKline(){
  const sym=document.getElementById('sym').value.trim().toUpperCase();
  const itv=document.getElementById('itv').value;
  const bars=document.getElementById('bars').value||100;
  if(!sym)return;
  try{
    const r=await fetch(API+'kline?symbol='+encodeURIComponent(sym)+'&interval='+itv+'&limit='+bars);
    const d=await r.json();
    if(d.error){document.getElementById('quote').innerHTML=esc(d.error);return}
    const rows=d.data||[];
    if(rows.length){
      const last=rows[rows.length-1],prev=rows.length>1?rows[rows.length-2]:null;
      const chg=prev?(parseFloat(last.Close)-parseFloat(prev.Close))/parseFloat(prev.Close)*100:0;
      const cls=chg>=0?'ch-up':'ch-down';
      document.getElementById('quote').innerHTML=
        '${esc(sym)} <span>'+parseFloat(last.Close).toFixed(2)+'</span> '+
        '<span class="'+cls+'">'+chg.toFixed(2)+'%</span>'+
        '<span class="badge">'+itv+'</span> <span class="badge">'+rows.length+'根</span>';
    }
    drawChart(rows);
  }catch(e){document.getElementById('quote').innerHTML=esc('查询失败: '+e.message)}
}
async function loadScreener(){
  try{
    // 用纳指100快照（scanner 不支持，这里用已采集的股票演示最近行情）
    const r=await fetch(API+'kline?symbol=AAPL&interval=1d&limit=2');
    const d=await r.json();
    // 简单展示：不额外请求，占位由其他接口补充
  }catch(e){}
}
async function loadNews(){
  const src=document.getElementById('news-src').value;
  try{
    // 新闻来自 stocks-api2（Yahoo 管道聚合5源）
    const r=await fetch('https://stocks-api2.365200.xyz/news?limit=30');
    const d=await r.json();
    const items=d.items||[];
    const el=document.getElementById('news');
    el.innerHTML=items.map(n=>
      '<div class="news-item"><span class="t">'+esc(n.title||n.digest||'')+'</span>'+
      '<span class="src">'+esc(n.publisher||n.channel||'')+'</span>'+
      '<span class="time">'+esc(n.pub_time||'')+'</span></div>'
    ).join('')||'<div class="news-item">暂无新闻</div>';
  }catch(e){document.getElementById('news').innerHTML=esc('加载失败: '+e.message)}
}
loadKline();loadNews();
setInterval(loadKline,30000);setInterval(loadNews,60000);
</script>
</body></html>"""

    @app.get("/")
    def root():
        from fastapi.responses import HTMLResponse
        return HTMLResponse(WEB_HTML)

    @app.get("/kline")
    def kline(symbol: str = Query(...), interval: str = Query("1d"),
              limit: int = Query(0), region: str = Query("us"),
              format: str = Query("json")):
        if interval not in TV_INTERVALS:
            return JSONResponse({"error": f"Invalid interval. Allowed: {list(TV_INTERVALS.keys())}"}, 400)
        subdir = TV_INTERVALS[interval][1]
        text = load_csv_gz(region, symbol, subdir)
        if text is None:
            return JSONResponse({"error": f"No data for {symbol} ({interval})"}, 404)
        if format == "csv":
            return PlainTextResponse(text, media_type="text/csv; charset=utf-8")
        # 解析 CSV 成 JSON
        lines = [l for l in text.strip().splitlines() if l.strip()]
        if len(lines) < 2:
            return {"symbol": symbol, "interval": interval, "count": 0, "data": []}
        header = lines[0].split(",")
        rows = []
        for line in lines[1:]:
            cells = line.split(",")
            rows.append(dict(zip(header, cells)))
        if limit > 0:
            rows = rows[-limit:]
        return {"symbol": symbol, "region": region, "interval": interval,
                "source": "tradingview", "count": len(rows), "data": rows}

    @app.get("/status")
    def status():
        return {
            "service": "TradingView API (server)",
            "bucket": R2_BUCKET,
            "sync_interval_minutes": SYNC_INTERVAL_MIN,
            "sync_running": _sync_running,
            "regions": SYNC_REGIONS,
            "intervals": list(TV_INTERVALS.keys()),
        }

    return app


app = create_app()


# ── 启动入口（uvicorn 运行） ────────────────────────────────────

if __name__ == "__main__":
    # 本地直接运行：python server_tv.py
    import uvicorn
    start_background_sync()
    port = int(os.environ.get("PORT", "3216"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")