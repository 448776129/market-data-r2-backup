/**
 * TradingView 数据 API（独立管道）
 *
 * 与 Yahoo 管道（stocks-api2）完全独立：
 *  - 数据源：R2 bucket stocks-tv（由 sync_tv.yml 采集）
 *  - 域名：stocks-tv.365200.xyz
 *
 * 路由：
 *   GET /                              → 文档主页
 *   GET /kline?symbol=&interval=&limit → K线（日K/1m/5m/15m/30m/1h/周K/月K）
 *   GET /status                         → 服务信息
 */

const API_BASE = "https://stocks-tv.365200.xyz";

const INTERVAL_DIR = {
  "1d": "kline",
  "1m": "kline_1m",
  "5m": "kline_5m",
  "15m": "kline_15m",
  "30m": "kline_30m",
  "1h": "kline_1h",
  "1wk": "kline_1wk",
  "1mo": "kline_1mo",
};

const REGION_LABEL = { us: "美股" };

function inferRegion(symbol) {
  const s = symbol.toUpperCase();
  if (s.endsWith(".HK")) return "hk";
  if (s.endsWith(".KS") || s.endsWith(".KQ")) return "kr";
  if (s.endsWith(".SS") || s.endsWith(".SZ")) return "cn";
  return "us"; // 目前仅美股
}

function splitLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; }
        else inQuotes = false;
      } else cur += ch;
    } else if (ch === '"') inQuotes = true;
    else if (ch === ",") { out.push(cur); cur = ""; }
    else cur += ch;
  }
  out.push(cur);
  return out;
}

function parseCSV(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length === 0) return [];
  const header = splitLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = splitLine(lines[i]);
    if (cells.length === 0) continue;
    const obj = {};
    for (let j = 0; j < header.length; j++) {
      obj[header[j]] = cells[j] !== undefined ? cells[j] : "";
    }
    rows.push(obj);
  }
  return rows;
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Cache-Control": "public, max-age=30",
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...corsHeaders() },
  });
}

function error(msg, status = 400) {
  return json({ error: msg }, status);
}

const HOME_HTML = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<title>TradingView 行情 API</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{font-family:system-ui,sans-serif;max-width:800px;margin:40px auto;padding:0 20px;color:#1a1a1a}
h1{font-size:1.6em}code{background:#f0f0f0;padding:2px 6px;border-radius:4px;font-size:.9em}
.ep{margin:8px 0}.m{font-weight:bold;color:#2563eb}</style></head><body>
<h1>TradingView 行情数据 API</h1>
<p>独立数据管道：GitHub Actions 用 tvdatafeed 采集 → R2 bucket <code>stocks-tv</code> → 本 Worker 分发。</p>
<p>与 <a href="https://stocks-api2.365200.xyz">stocks-api2（Yahoo 管道）</a> 互不干扰。</p>
<h3>接口</h3>
<div class="ep"><span class="m">GET /kline</span> K线数据（日K/1m/5m/15m/30m/1h/周K/月K）<br>
<code>/kline?symbol=AAPL&interval=1d&limit=5</code></div>
<div class="ep"><span class="m">GET /status</span> 服务信息<br>
<code>/status</code></div>
</body></html>`;

async function handleKline(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/kline`,
        description: "查询任意股票 K 线（TradingView 数据源）",
        params: {
          symbol: "股票代码（必填），如 AAPL",
          interval: `周期，默认 1d。可选：${Object.keys(INTERVAL_DIR).join(" / ")}`,
          limit: "最多返回行数（最新 N 条）",
          format: "json(默认) / csv",
        },
        example: `${API_BASE}/kline?symbol=AAPL&interval=1d&limit=5`,
      },
    });
  }
  const interval = (params.get("interval") || "1d").toLowerCase();
  if (!INTERVAL_DIR[interval]) {
    return error(`Invalid interval: ${interval}. Allowed: ${Object.keys(INTERVAL_DIR).join(", ")}`);
  }
  const region = (params.get("region") || inferRegion(symbol)).toLowerCase();
  const limit = parseInt(params.get("limit") || "0", 10);
  const format = (params.get("format") || "json").toLowerCase();

  const r2key = `${region}/${INTERVAL_DIR[interval]}/${symbol}.csv`;
  let text = null;
  try {
    const obj = await env.MARKET_DATA_R2.get(r2key);
    if (obj) {
      const bytes = new Uint8Array(await obj.arrayBuffer());
      const gzipMagic = bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b;
      if (gzipMagic || r2key.endsWith(".gz")) {
        const ds = new DecompressionStream("gzip");
        const stream = new Blob([bytes]).stream().pipeThrough(ds);
        text = new TextDecoder().decode(await new Response(stream).arrayBuffer());
      } else {
        text = new TextDecoder().decode(bytes);
      }
    }
  } catch (e) {
    return error(`R2 读取失败: ${e.message}`, 502);
  }
  if (text === null) {
    return error(`No data for ${symbol} (${interval}). File not found: ${r2key}`, 404);
  }

  if (format === "csv") {
    return new Response(text, { status: 200, headers: { "Content-Type": "text/csv; charset=utf-8", ...corsHeaders() } });
  }

  let rows = parseCSV(text);
  const indexCol = (interval === "1d" || interval === "1wk" || interval === "1mo") ? "Date" : "Datetime";
  // 兼容旧数据 Date/Datetime 列名
  if (rows.length > 0 && !rows[0][indexCol] && rows[0]["Date"] && indexCol === "Datetime") {
    // no-op, 用已有列
  }
  if (limit > 0) {
    rows = rows.slice(-limit);
  }
  return json({ symbol, region, interval, source: "tradingview", count: rows.length, order: "asc", data: rows });
}

function handleStatus(request) {
  const currentBase = request ? `https://${new URL(request.url).host}` : API_BASE;
  return json({
    service: "StockAPI-TradingView",
    base: API_BASE,
    current: currentBase,
    source: "TradingView (tvdatafeed)",
    bucket: "stocks-tv",
    endpoints: {
      kline: `${API_BASE}/kline`,
    },
    intervals: Object.keys(INTERVAL_DIR),
    regions: Object.keys(REGION_LABEL),
    note: "独立数据管道，与 Yahoo 管道 stocks-api2 互不干扰。",
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    if (path === "/" || path === "") {
      return new Response(HOME_HTML, { status: 200, headers: { "Content-Type": "text/html; charset=utf-8", ...corsHeaders() } });
    }
    const params = url.searchParams;
    if (path === "/kline") return await handleKline(params, env);
    if (path === "/status") return handleStatus(request);
    return error("Not found. Use /, /kline, /status", 404);
  },
};