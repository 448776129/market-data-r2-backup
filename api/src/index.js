/**
 * 行情数据动态接口（Cloudflare Worker）
 *
 * 免费托管在 Cloudflare Workers（无需服务器），数据由 GitHub Actions 自动采集
 * 并存入 Cloudflare R2（gzip 压缩），本 Worker 在边缘节点从 R2 读取、
 * 自动解压 gzip 并解析转成 JSON 返回。
 *
 * 部署：见 api/README.md
 *
 * 路由：
 *   GET /                                            → 项目介绍 + API 文档主页（HTML）
 *   GET /kline?symbol=AAPL&interval=1d&limit=5       → K线数据（日K/1m/5m/15m/30m/1h）
 *   GET /price?symbol=AAPL                           → 实时最新价格快照
 *   GET /download?symbol=AAPL&interval=1h            → 下载 gzip 压缩的原始 CSV
 *   GET /quote?symbol=0700.HK                        → 个股元数据（名称/行业/市值/最新价…）
 *   GET /universe?index=csi300                       → 指数成分股清单
 *   GET /indices                                     → 可用的指数/清单及其成分数量
 *   GET /symbols?region=cn&limit=10&offset=0         → 按区域列出股票代码
 *   GET /status                                      → 数据仓库配置信息
 */

// 数据仓库信息（与 git remote 一致）
const REPO_OWNER = "448776129";
const REPO_NAME = "market-data-r2";
const REPO_BRANCH = "main";

// 对外接口域名（自定义域）
const API_BASE = "https://stockapi.365200.xyz";

// Yahoo chart API 反代入口（与采集端 config.YAHOO_CHART_PROXY 一致）
// 国内访问 Yahoo 需经反代转发；用于 /price 实时行情。
const YAHOO_CHART_PROXY = "https://img2.365200.xyz";
const YAHOO_CHART_ORIGIN = "https://query1.finance.yahoo.com/v8/finance/chart/";

// interval -> data 子目录映射
const INTERVAL_DIR = {
  "1d": "kline",
  "1m": "kline_1m",
  "5m": "kline_5m",
  "15m": "kline_15m",
  "30m": "kline_30m",
  "1h": "kline_1h",
};

// 区域 -> 中文名（用于文档/展示）
const REGION_LABEL = {
  cn: "A股",
  us: "美股",
  hk: "港股",
  kr: "韩股",
  etf: "美股ETF",
  cn_etf: "中国ETF",
};

// 指数/清单 -> 中文名（universe/*.csv 文件名）
const INDEX_LABEL = {
  csi300: "沪深300",
  csi500: "中证500",
  nasdaq100: "纳斯达克100",
  sp500: "标普500",
  hsi: "恒生指数",
  cn: "A股全部",
  us: "美股全部",
  hk: "港股全部",
  kr: "韩股全部",
  etf: "美股ETF",
  cn_etf: "中国ETF",
};

// 从代码后缀推断区域；裸代码默认美股
function inferRegion(symbol) {
  const s = symbol.toUpperCase();
  if (s.endsWith(".HK")) return "hk";
  if (s.endsWith(".KS") || s.endsWith(".KQ")) return "kr";
  if (s.endsWith(".SS") || s.endsWith(".SZ")) return "cn";
  return "us";
}

// 解析 CSV 文本为对象数组（首行表头）
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

// 简易 CSV 行切分（处理带引号字段）
function splitLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

// 统一响应头（允许跨域调用）
function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "public, max-age=60",
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

function html(text, status = 200) {
  return new Response(text, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8", ...corsHeaders() },
  });
}

// 边缘缓存（Cloudflare Cache API）：把高频读的 JSON/CSV 接口结果缓存在边缘，
// 命中时直接短路返回，既省 Worker 的 CPU/请求额度，也大幅减少对 R2 的读操作
// （每次未命中才回源 R2 一次，命中则零 R2 成本）。
//
// key   ：显式缓存键，通常用请求 URL。会附加一个用户不可见的版本前缀便于整体失效。
// ttlSec：缓存有效期（秒）。过期后自动回源刷新。
// producer：生成新鲜响应的函数，仅首次/过期时才被调用。
async function edgeCache(key, ttlSec, env, ctx, producer) {
  const cache = caches.default;
  const cacheKey = new Request(key, { method: "GET" });

  // 命中缓存：直接返回，不计 Worker 业务逻辑、不读 R2
  const hit = await cache.match(cacheKey);
  if (hit) {
    // 刷新浏览器端缓存指令，保持对外表现一致
    const resp = new Response(hit.body, hit);
    resp.headers.set("Cache-Control", `public, max-age=${Math.min(ttlSec, 60)}`);
    resp.headers.set("CF-Cache-Status", "HIT");
    return resp;
  }

  const origin = await producer();
  if (origin.status >= 400) {
    return origin; // 错误响应不缓存，避免缓存住 404/错误
  }

  // 写缓存：clone 一份给 cache.put（body 只能消费一次）
  const forCache = origin.clone();
  forCache.headers.set("Cache-Control", `public, max-age=${ttlSec}`);
  ctx.waitUntil(cache.put(cacheKey, forCache));

  origin.headers.set("Cache-Control", `public, max-age=${Math.min(ttlSec, 60)}`);
  origin.headers.set("CF-Cache-Status", "MISS");
  return origin;
}

// 读取静态清单（universe）：优先 KV（毫秒级、不耗 R2 读额度），miss 时 fallback R2。
// name 对应 universe 文件名（不含 .csv 后缀），如 "csi300" / "us" / "hk"。
// KV key: "universe:{name}"；R2 key: "universe/{name}.csv"。
async function fetchUniverseText(name, env) {
  // 1) 优先 KV
  if (env && env.STATIC_KV) {
    try {
      const val = await env.STATIC_KV.get(`universe:${name}`, { type: "text" });
      if (val !== null && val !== undefined) return val;
    } catch (e) {
      console.warn(`KV read failed for universe:${name}: ${e.message}`);
    }
  }
  // 2) fallback R2 → GitHub raw
  return await fetchUpstream(`data/universe/${name}.csv`, env);
}

// 读取数据：优先从 R2（MARKET_DATA_R2 binding）读取，失败时 fallback 到 GitHub raw。
// path 形如 "data/{region}/kline/{symbol}.csv"，R2 中对象键为 "{region}/kline/{symbol}.csv"。
// R2 中的 K 线 CSV 为 gzip 压缩存储（.gz 或 Content-Encoding: gzip），自动解压。
async function fetchUpstream(path, env) {
  // 1) 优先 R2：去掉 "data/" 前缀即 R2 对象键
  if (env && env.MARKET_DATA_R2) {
    try {
      const r2key = path.replace(/^data\//, "");
      const obj = await env.MARKET_DATA_R2.get(r2key);
      if (obj) {
        const bytes = await obj.arrayBuffer();
        const data = new Uint8Array(bytes);
        const gzipMagic = data.length >= 2 && data[0] === 0x1f && data[1] === 0x8b;
        const enc = obj.httpMetadata && (obj.httpMetadata.contentEncoding || "");
        if (gzipMagic || enc === "gzip" || r2key.endsWith(".gz")) {
          // 解压 gzip
          const ds = new DecompressionStream("gzip");
          const stream = new Blob([data]).stream().pipeThrough(ds);
          const buf = await new Response(stream).arrayBuffer();
          return new TextDecoder().decode(buf);
        }
        return new TextDecoder().decode(data);
      }
    } catch (e) {
      // R2 读取失败则尝试 GitHub raw
      console.warn(`R2 read failed for ${path}: ${e.message}`);
    }
  }

  // 2) fallback：GitHub raw
  const url = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${REPO_BRANCH}/${path}`;
  let resp;
  try {
    resp = await fetch(url);
  } catch (e) {
    throw new Error(`upstream fetch failed: ${e.message}`);
  }
  if (resp.status === 404) {
    return null;
  }
  if (!resp.ok) {
    throw new Error(`upstream error: ${resp.status}`);
  }
  return await resp.text();
}

// 带区域回退的数据读取：
//  - 中国 A股(.SS/.SZ) 与 中国 ETF(.SS/.SZ) 后缀相同，inferRegion 识别为 cn。
//    若 cn 下找不到，回退尝试 cn_etf（中国 ETF 独立分类）。
//  - 美股裸代码（SPY/VOO/QQQ）inferRegion 识别为 us，但可能是 ETF，
//    若 us 下找不到，回退尝试 etf（美股 ETF 独立分类）。
// 返回文本；都找不到返回 null。
async function fetchWithFallback(path, env) {
  let text = await fetchUpstream(path, env);
  if (text !== null) return text;
  // 回退1：/cn/ -> /cn_etf/
  const altCn = path.replace("/cn/", "/cn_etf/");
  if (altCn !== path) {
    text = await fetchUpstream(altCn, env);
    if (text !== null) return text;
  }
  // 回退2：/us/ -> /etf/
  const altUs = path.replace("/us/", "/etf/");
  if (altUs !== path) {
    text = await fetchUpstream(altUs, env);
    if (text !== null) return text;
  }
  return null;
}

// 数值/日期过滤预备：返回比较用的时间戳
function tsOf(value) {
  if (value === undefined || value === null || value === "") return null;
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d.getTime();
}

// ============================================================
// K线数据
// ============================================================
async function handleKline(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/kline`,
        description: "查询任意股票K线数据（日K / 1m / 5m / 15m / 30m / 1h）",
        params: {
          symbol: "股票代码（必填），如 AAPL / 0700.HK / 600519.SS / 000001.SZ",
          interval: `周期，默认 1d。可选：${Object.keys(INTERVAL_DIR).join(" / ")}`,
          start: "起始日期 YYYY-MM-DD（含）",
          end: "结束日期 YYYY-MM-DD（含）",
          limit: "最多返回行数；默认返回时间上最新 N 条",
          order: "asc(默认，时间升序) / desc(最新在前)",
          format: "json(默认) / csv",
        },
        example: `${API_BASE}/kline?symbol=AAPL&interval=1d&start=2024-01-01&end=2024-12-31`,
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
  const startTs = tsOf(params.get("start"));
  const endTs = tsOf(params.get("end"));

  const dir = INTERVAL_DIR[interval];
  const text = await fetchWithFallback(`data/${region}/${dir}/${symbol}.csv`, env);
  if (text === null) {
    return error(
      `No data for ${symbol} (${interval}). File not found: data/${region}/${dir}/${symbol}.csv`,
      404
    );
  }

  if (format === "csv") {
    return new Response(text, {
      status: 200,
      headers: { "Content-Type": "text/csv; charset=utf-8", ...corsHeaders() },
    });
  }

  let rows = parseCSV(text);
  const indexCol = interval === "1d" ? "Date" : "Datetime";

  if (startTs !== null || endTs !== null) {
    rows = rows.filter((r) => {
      const t = tsOf(r[indexCol]);
      if (t === null) return true;
      if (startTs !== null && t < startTs) return false;
      if (endTs !== null && t > endTs) return false;
      return true;
    });
  }

  const order = (params.get("order") || "asc").toLowerCase();
  if (order === "desc") {
    rows.reverse();
  }

  if (limit > 0) {
    rows = order === "desc" ? rows.slice(0, limit) : rows.slice(-limit);
  }

  return json({ symbol, region, interval, count: rows.length, order, data: rows });
}

// ============================================================
// 个股元数据（quote）
// ============================================================
async function handleQuote(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/quote`,
        description: "查询个股元数据（公司名/行业/市值/最新价等）",
        params: { symbol: "股票代码（必填）" },
        example: `${API_BASE}/quote?symbol=AAPL`,
      },
    });
  }

  const region = (params.get("region") || inferRegion(symbol)).toLowerCase();
  const text = await fetchWithFallback(`data/${region}/meta/${symbol}.json`, env);
  if (text === null) {
    return error(`No meta for ${symbol}. File not found: data/${region}/meta/${symbol}.json`, 404);
  }

  let meta;
  try {
    meta = JSON.parse(text);
  } catch {
    return error(`Invalid meta JSON for ${symbol}`, 502);
  }

  // 兼容两种结构：扁平（当前采集的 chart meta + search）与嵌套（旧 yfinance info）
  const info = meta.info || meta;

  // 完整暴露已入库的基本面字段（不丢字段）
  const flatKeys = [
    // 标识
    "symbol", "longName", "shortName", "currency", "exchange",
    "instrumentType", "quoteType", "sector", "industry",
    "firstTradeDate", "timezone", "gmtoffset", "hasPrePostMarketData",
    // 行情快照
    "regularMarketPrice", "regularMarketDayHigh", "regularMarketDayLow",
    "regularMarketVolume", "regularMarketTime",
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "chartPreviousClose",
    "change", "changePercent",
  ];
  const flat = {};
  for (const k of flatKeys) {
    if (info[k] !== undefined && info[k] !== null) flat[k] = info[k];
  }

  // 嵌套结构（旧 yfinance info）的字段原样透出
  const nestedPick = [
    "country", "marketCap", "currentPrice", "open", "previousClose",
    "dayHigh", "dayLow", "regularMarketPreviousClose",
    "trailingPE", "forwardPE", "priceToBook", "dividendYield", "dividendRate",
    "trailingEps", "forwardEps", "beta", "volume", "averageVolume",
    "sharesOutstanding", "floatShares", "targetMeanPrice", "targetHighPrice",
    "targetLowPrice", "recommendationKey", "totalRevenue", "grossProfits",
    "freeCashflow", "totalDebt", "totalCash", "profitMargins",
    "returnOnEquity", "returnOnAssets", "earningsGrowth", "revenueGrowth",
    "fiftyDayAverage", "twoHundredDayAverage",
  ];
  const nested = {};
  for (const k of nestedPick) {
    if (info[k] !== undefined && info[k] !== null) nested[k] = info[k];
  }

  const result = {
    symbol: meta.symbol || symbol,
    region: meta.region || region,
    name: meta.longName || meta.shortName || meta.name,
    currency: meta.currency,
    exchange: meta.exchange || meta.fullExchangeName || info.exchange,
    isin: meta.isin || null,
    ...flat,
    ...nested,
  };
  // 若存在旧嵌套 info，一并透出完整 info + 财务/分红等（供参考）
  if (meta.info) result.info = meta.info;
  if (meta.financials) result.financials = meta.financials;
  if (meta.dividends) result.dividends = meta.dividends;
  if (meta.splits) result.splits = meta.splits;
  if (meta.recommendations_summary) result.recommendations_summary = meta.recommendations_summary;
  if (meta.earnings_dates) result.earnings_dates = meta.earnings_dates;
  if (meta.major_holders) result.major_holders = meta.major_holders;
  if (meta.institutional_holders) result.institutional_holders = meta.institutional_holders;
  if (meta.analyst_price_targets) result.analyst_price_targets = meta.analyst_price_targets;

  return json(result);
}

// ============================================================
// 实时价格（当场调取 Yahoo API，实时返回最新价）
// 逻辑：直接请求 Yahoo chart API（经反代）range=1d 数据，
// 从 meta 取实时价格 / 涨跌 / 当日高低 / 成交量 / 名称 / 币种。
// 注意：不读 R2 数据库，保证价格是最新的（含盘前盘后）。
// ============================================================
async function fetchRealtimeQuote(symbol) {
  // 经反代访问 Yahoo chart API（range=1d 足够拿 meta 实时价）
  const query = `interval=1d&range=1d`;
  const url = `${YAHOO_CHART_PROXY}/${YAHOO_CHART_ORIGIN}${encodeURIComponent(symbol)}?${query}`;
  const resp = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });
  if (resp.status === 404) return null;
  if (!resp.ok) throw new Error(`Yahoo chart API HTTP ${resp.status}`);
  const data = await resp.json();
  const result = (data && data.chart && data.chart.result) || [];
  if (result.length === 0) return null;
  return result[0];
}

async function handlePrice(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/price`,
        description: "实时返回股票最新价格（当场调取 Yahoo API，非数据库缓存）",
        params: { symbol: "股票代码（必填），如 AAPL / 0700.HK / 600519.SS" },
        example: `${API_BASE}/price?symbol=AAPL`,
      },
    });
  }
  const region = (params.get("region") || inferRegion(symbol)).toLowerCase();

  // 当场调取 Yahoo 实时行情
  let res;
  try {
    res = await fetchRealtimeQuote(symbol);
  } catch (e) {
    return error(`实时行情获取失败: ${e.message}`, 502);
  }
  if (res === null) {
    return error(`Yahoo 无此股票数据: ${symbol}`, 404);
  }

  const meta = res.meta || {};
  const ts = res.timestamp || [];
  const quote = ((res.indicators || {}).quote || [{}])[0] || {};
  const closeArr = quote.close || [];
  const openArr = quote.open || [];
  const highArr = quote.high || [];
  const lowArr = quote.low || [];
  const volArr = quote.volume || [];

  // 最新一根 bar
  const n = ts.length;
  const lastIdx = n > 0 ? n - 1 : -1;
  const price = meta.regularMarketPrice !== undefined ? meta.regularMarketPrice : (lastIdx >= 0 ? closeArr[lastIdx] : null);
  const open = lastIdx >= 0 ? openArr[lastIdx] : null;
  const high = lastIdx >= 0 ? highArr[lastIdx] : null;
  const low = lastIdx >= 0 ? lowArr[lastIdx] : null;
  const volume = lastIdx >= 0 ? volArr[lastIdx] : 0;
  const datetime = lastIdx >= 0 ? new Date(ts[lastIdx] * 1000).toISOString().slice(0, 19).replace("T", " ") : null;

  // 涨跌（meta 提供 regularMarketPrice / chartPreviousClose）
  const prevClose = meta.chartPreviousClose !== undefined ? meta.chartPreviousClose : (n >= 2 ? closeArr[n - 2] : null);
  let change = null;
  let changePercent = null;
  if (price !== null && price !== undefined && prevClose !== null && prevClose !== undefined && prevClose !== 0) {
    change = +(price - prevClose).toFixed(4);
    changePercent = +((change / prevClose) * 100).toFixed(4);
  }

  // 名称 / 币种（当场从 meta 获取，无需数据库）
  const name = meta.longName || meta.shortName || null;
  const currency = meta.currency || null;

  return json({
    symbol,
    region,
    name,
    price,
    currency,
    datetime,
    interval: "realtime",
    open,
    high,
    low,
    close: price,
    volume,
    change,
    changePercent,
    fiftyTwoWeekHigh: meta.fiftyTwoWeekHigh ?? null,
    fiftyTwoWeekLow: meta.fiftyTwoWeekLow ?? null,
    marketTime: meta.regularMarketTime ? new Date(meta.regularMarketTime * 1000).toISOString() : null,
    source: "Yahoo 实时行情（当场调取）",
  });
}

// ============================================================
// 下载原始 CSV（gzip 压缩）到本地
// 返回 R2 中存储的原始 gzip 字节（Content-Encoding: gzip），
// 浏览器/curl 可直接下载为 {symbol}_{interval}.csv.gz
// ============================================================
async function handleDownload(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/download`,
        description: "下载股票K线数据的 gzip 压缩 CSV（原始文件，体积小）",
        params: {
          symbol: "股票代码（必填），如 AAPL / 0700.HK / 600519.SS",
          interval: `周期，默认 1d。可选：${Object.keys(INTERVAL_DIR).join(" / ")}`,
        },
        example: `${API_BASE}/download?symbol=AAPL&interval=1h`,
        note: "返回 gzip 压缩的 .csv.gz 文件；浏览器下载后可用 WinRAR/7-Zip 解压，或直接用 data 接口获取解压后的文本。",
      },
    });
  }
  const interval = (params.get("interval") || "1d").toLowerCase();
  if (!INTERVAL_DIR[interval]) {
    return error(`Invalid interval: ${interval}. Allowed: ${Object.keys(INTERVAL_DIR).join(", ")}`);
  }
  const region = (params.get("region") || inferRegion(symbol)).toLowerCase();
  const dir = INTERVAL_DIR[interval];
  const r2keys = [`${region}/${dir}/${symbol}.csv`];
  // 中国 A股(.SS/.SZ) 回退到 cn_etf
  if (region === "cn") r2keys.push(`cn_etf/${dir}/${symbol}.csv`);

  // 直接从 R2 取原始字节（不解压）
  let obj = null;
  let r2key = null;
  for (const k of r2keys) {
    try {
      obj = await env.MARKET_DATA_R2.get(k);
      if (obj) { r2key = k; break; }
    } catch {
      obj = null;
    }
  }
  if (!obj) {
    return error(`No data for ${symbol} (${interval}).`, 404);
  }
  const bytes = await obj.arrayBuffer();

  // 文件名：{symbol}_{interval}.csv.gz
  const filename = `${symbol}_${interval}.csv.gz`;
  const headers = {
    "Content-Type": "application/gzip",
    "Content-Disposition": `attachment; filename="${filename}"`,
    ...corsHeaders(),
  };
  // 只有确实是 gzip 才加 Content-Encoding，否则是纯文本直接给 .csv
  const data = new Uint8Array(bytes);
  const isGzip = data.length >= 2 && data[0] === 0x1f && data[1] === 0x8b;
  if (!isGzip) {
    headers["Content-Type"] = "text/csv; charset=utf-8";
    headers["Content-Disposition"] = `attachment; filename="${symbol}_${interval}.csv"`;
  }
  return new Response(bytes, { status: 200, headers });
}

// ============================================================
// 指数成分股 / 清单
// ============================================================
async function handleUniverse(params, env) {
  const index = (params.get("index") || "").trim().toLowerCase();
  if (!index) {
    return json({
      usage: {
        endpoint: `${API_BASE}/universe`,
        description: "获取指定指数/清单的成分股代码",
        params: {
          index: `必填。可选：${Object.keys(INDEX_LABEL).join(" / ")}`,
        },
        example: `${API_BASE}/universe?index=csi300`,
      },
    });
  }
  if (!INDEX_LABEL[index]) {
    return error(`Invalid index: ${index}. Allowed: ${Object.keys(INDEX_LABEL).join(", ")}`);
  }

  const text = await fetchUniverseText(index, env);
  if (text === null) {
    return error(`No universe file: universe/${index}.csv`, 404);
  }

  // universe 文件为每行一个股票代码（无表头）
  const symbols = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l !== "" && !l.startsWith("#"));

  return json({
    index,
    name: INDEX_LABEL[index],
    count: symbols.length,
    symbols,
  });
}

// ============================================================
// 可用指数/清单列表
// ============================================================
async function handleIndices(env) {
  const names = Object.keys(INDEX_LABEL);
  const items = [];
  for (const name of names) {
    try {
      const text = await fetchUniverseText(name, env);
      const count = text === null ? 0 : text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l !== "" && !l.startsWith("#")).length;
      items.push({ index: name, name: INDEX_LABEL[name], count });
    } catch {
      items.push({ index: name, name: INDEX_LABEL[name], count: 0 });
    }
  }
  return json({ base: API_BASE, indices: items });
}

// ============================================================
// 按区域列出股票代码
// ============================================================
async function handleSymbols(params, env) {
  const region = (params.get("region") || "").trim().toLowerCase() || "us";
  if (!REGION_LABEL[region]) {
    return error(`Invalid region: ${region}. Allowed: ${Object.keys(REGION_LABEL).join(", ")}`);
  }
  const limit = Math.min(parseInt(params.get("limit") || "100", 10), 1000);
  const offset = Math.max(parseInt(params.get("offset") || "0", 10), 0);

  const text = await fetchUniverseText(region, env);
  if (text === null) {
    return error(`No universe file: universe/${region}.csv`, 404);
  }

  const symbols = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l !== "" && !l.startsWith("#"));

  const page = symbols.slice(offset, offset + limit);
  return json({
    region,
    region_name: REGION_LABEL[region],
    total: symbols.length,
    offset,
    limit,
    count: page.length,
    symbols: page,
  });
}

// ============================================================
// 实时新闻（当场调取源站，不走缓存，结果入库 KV）
// 用法：
//   GET /news-yh/live    → 实时拉取雅虎香港头条并入库
//   GET /news-em/live    → 实时拉取东方财富 7x24h 并入库
// ============================================================

const EASTMONEY_API = "https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_80_1_.html";
const YAHOO_HK_URL = "https://hk.finance.yahoo.com/topic/latest-news/";

async function handleLiveNews(which, params, env, ctx) {
  // 1) 实时拉取源站
  let raw, contentType;
  try {
    if (which === "em") {
      const url = `${YAHOO_CHART_PROXY}/${EASTMONEY_API}`;
      const resp = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          "Referer": "https://kuaixun.eastmoney.com/",
          "X-Requested-With": "XMLHttpRequest",
        },
      });
      if (!resp.ok) throw new Error(`Eastmoney HTTP ${resp.status}`);
      raw = await resp.text();
      contentType = "eastmoney";
    } else if (which === "yh") {
      const url = `${YAHOO_CHART_PROXY}/${YAHOO_HK_URL}`;
      const resp = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
          "Accept": "text/html,application/xhtml+xml",
        },
      });
      if (!resp.ok) throw new Error(`Yahoo HK HTTP ${resp.status}`);
      raw = await resp.text();
      contentType = "yahoo_html";
    } else {
      return error("Unknown news source", 400);
    }
  } catch (e) {
    return error(`实时新闻拉取失败: ${e.message}`, 502);
  }

  // 2) 解析
  let newsData;
  try {
    if (contentType === "eastmoney") {
      // 东财返回格式：var ajaxResult={...} ;
      const m = raw.match(/ajaxResult\s*=\s*(\{.*\})\s*;?\s*$/s);
      if (!m) throw new Error("Eastmoney 返回格式异常");
      const parsed = JSON.parse(m[1]);
      const lives = parsed.LivesList || [];
      const items = lives.map(n => ({
        id: n.id,
        title: n.title || n.simtitle || "",
        digest: n.digest || n.simdigest || "",
        showtime: n.showtime || "",
        pub_ts: n.sort ? parseInt(String(n.sort).slice(0, 10)) : null,
        url_pc: n.url_w || null,
        url_mobile: n.url_m || null,
        editor: n.editor_name || "",
        source: "东方财富 7x24h",
      }));
      newsData = { source: "eastmoney", count: items.length, news: items };
    } else {
      // Yahoo HK：HTML 解析（提取标题 + 链接）
      const items = [];
      const seenUrls = new Set();
      const cardRe2 = /<a [^>]*href="(https?:\/\/hk\.finance\.yahoo\.com\/news\/[^"]+\.html[^"]*)"[^>]*>([\s\S]*?)<\/a>/g;
      let m;
      while ((m = cardRe2.exec(raw)) !== null) {
        const url = m[1];
        if (seenUrls.has(url)) continue;
        seenUrls.add(url);
        const title = m[2].replace(/<[^>]+>/g, "").trim();
        if (title.length < 4) continue;
        items.push({
          title,
          url,
          publisher: null,
          rel_time: null,
          pub_ts: null,
          source: "Yahoo Finance HK",
        });
      }
      // 尝试提取时间和来源
      for (const item of items) {
        const idx = raw.indexOf(item.url);
        if (idx > 0) {
          const snippet = raw.slice(Math.max(0, idx - 400), idx + 200);
          const pubM = snippet.match(/<span class="publisher">(.*?)<\/span>/s);
          if (pubM) item.publisher = pubM[1].replace(/<[^>]+>/g, "").trim();
          const tmM = snippet.match(/<span class="published-date">(.*?)<\/span>/s);
          if (tmM) item.rel_time = tmM[1].replace(/<[^>]+>/g, "").trim();
        }
      }
      newsData = { source: "yahoo_hk", count: items.length, news: items };
    }
  } catch (e) {
    return error(`新闻解析失败: ${e.message}`, 502);
  }

  // 3) 入库 KV（后续请求走缓存，不用再实时拉）
  if (env && env.STATIC_KV) {
    const kvKey = `news:${which}:live`;
    const kvValue = JSON.stringify(newsData, null, 2);
    ctx.waitUntil(env.STATIC_KV.put(kvKey, kvValue));
  }

  // 4) 返回
  const limit = Math.min(parseInt(params.get("limit") || "20", 10), 80);
  const list = (newsData.news || []).slice(0, limit);
  return json({
    source: newsData.source,
    total: newsData.count,
    count: list.length,
    limit,
    live: true,
    items: list,
  });
}

// ============================================================
// 聚合新闻
//   /news-yh → 雅虎香港财经头条（繁体）
//   /news-em → 东方财富 7x24h 快讯（简体）
//   /news    → 以上两源合并按发布时间倒序（扁平列表）
// 默认 limit = 20；?limit=N 获取更多或更少；?help=1 返回文档。
// ============================================================
const DEFAULT_NEWS_LIMIT = 20;

async function handleAggNews(which, params, env) {
  if (params.get("help") === "1") {
    return json({
      endpoints: {
        "/news-yh": {
          description: "雅虎香港财经头条（hk.finance.yahoo.com/topic/latest-news/），中文繁体。",
          source_url: "https://hk.finance.yahoo.com/topic/latest-news/",
          params: { limit: `可选：返回条数，默认 ${DEFAULT_NEWS_LIMIT}` },
          example: `${API_BASE}/news-yh?limit=50`,
        },
        "/news-em": {
          description: "东方财富 7x24h 财经快讯（kuaixun.eastmoney.com），中文简体。",
          source_url: "https://kuaixun.eastmoney.com/",
          params: { limit: `可选：返回条数，默认 ${DEFAULT_NEWS_LIMIT}，最多 80` },
          example: `${API_BASE}/news-em?limit=80`,
        },
        "/news": {
          description: "聚合新闻：雅虎香港头条 + 东方财富 7x24h 合并，扁平列表按发布时间倒序。",
          params: { limit: `可选：返回条数，默认 ${DEFAULT_NEWS_LIMIT}` },
          example: `${API_BASE}/news?limit=50`,
        },
      },
    });
  }

  const userLimit = parseInt(params.get("limit") || "0", 10);
  const limit = userLimit > 0 ? userLimit : DEFAULT_NEWS_LIMIT;

  // 直连 KV（新闻是小 JSON，KV 读几乎免费 + 低延迟），KV 没有再回退 R2。
  // 这样命中时完全不走 R2 binding → R2 Class B 为 0，Worker CPU 更低。
  async function loadNews(name /* yh | em */) {
    // 先 KV
    if (env.STATIC_KV) {
      try {
        const kvText = await env.STATIC_KV.get(`news:${name}`);
        if (typeof kvText === "string" && kvText.length > 0) {
          return kvText;
        }
      } catch {}
    }
    return fetchUpstream(`news/${name}.json`, env);
  }

  if (["yh", "em"].includes(which)) {
    const text = await loadNews(which);
    if (text === null) {
      return error(`暂未采集到${which === "yh" ? "雅虎香港" : "东方财富"}新闻。news/${which}.json 未入库。`, 404);
    }
    try {
      const data = JSON.parse(text);
      const list = (Array.isArray(data.news) ? data.news : []).slice(0, limit);
      return json({
        total: Array.isArray(data.news) ? data.news.length : list.length,
        count: list.length,
        limit,
        items: list,
      });
    } catch {
      return error(`news/${which}.json 解析失败`, 502);
    }
  }

  if (which === "all") {
    const [yhRaw, emRaw] = await Promise.all([
      loadNews("yh"),
      loadNews("em"),
    ]);

    const items = [];
    if (yhRaw) {
      try {
        const d = JSON.parse(yhRaw);
        for (const n of d.news || []) {
          items.push({
            channel: "yahoo_hk",
            title: n.title,
            url: n.url,
            digest: null,
            pub_ts: n.pub_ts,
            pub_time: n.pub_time,
            rel_time: n.rel_time || null,
            publisher: n.publisher || n.source || null,
          });
        }
      } catch {}
    }
    if (emRaw) {
      try {
        const d = JSON.parse(emRaw);
        for (const n of d.news || []) {
          items.push({
            channel: "eastmoney",
            title: n.title,
            url: n.url_pc || n.url_mobile || null,
            digest: n.digest || null,
            pub_ts: n.pub_ts || null,
            pub_time: n.pub_time || null,
            showtime: n.showtime || null,
            editor: n.editor || null,
            publisher: "东方财富",
            comment_num: n.comment_num || 0,
          });
        }
      } catch {}
    }
    items.sort((a, b) => (b.pub_ts || 0) - (a.pub_ts || 0));
    const page = items.slice(0, limit);
    return json({
      total: items.length,
      count: page.length,
      limit,
      items: page,
    });
  }

  return error(`Unknown news channel: ${which}`, 400);
}

// ============================================================
// 选股器：读 KV 快照 → 内存过滤 → 返回
// KV key: screener:{interval}:{region}（由 screener_precompute.py 写入）
// KV value: JSON { "AAPL": { ma5:..., rsi14:..., ... }, ... }
// ============================================================
const SCREENER_OPS = {
  gt: (a, b) => a > b,
  gte: (a, b) => a >= b,
  lt: (a, b) => a < b,
  lte: (a, b) => a <= b,
  eq: (a, b) => Math.abs(a - b) < 1e-6,
};
const SCREENER_BOOL_FIELDS = new Set([
  "ma5_gt_ma10","ma10_gt_ma20","ma20_gt_ma60",
  "macd_gt_signal","macd_gt_zero","price_gt_ma20","price_gt_ma60",
  "rsi_oversold","rsi_overbought","volume_surge",
]);

async function handleScreener(params, env) {
  const scope = (params.get("scope") || params.get("market") || "").toLowerCase();
  if (!scope) {
    return json({
      usage: {
        endpoint: `${API_BASE}/screener`,
        description: "选股器：从 KV 快照中按技术指标条件过滤股票",
        params: {
          scope: "选股范围，如 daily:us / daily:cn / daily:hk / daily:etf",
          ma5_gt: "MA5 > 指定值，如 ma5_gt=300",
          rsi14_lt: "RSI14 < 指定值，如 rsi14_lt=30",
          change_1d_gt: "日涨幅 > 指定百分比",
          ma5_gt_ma10: "布尔：MA5 > MA10（多头）",
          macd_gt_signal: "布尔：MACD 金叉",
          rsi_oversold: "布尔：RSI14 < 30",
          rsi_overbought: "布尔：RSI14 > 70",
          volume_surge: "布尔：成交量 > 均量×2",
          sort: "排序字段，默认 change_1d",
          order: "asc / desc（默认 desc）",
          limit: "返回条数，默认 50，最大 500",
        },
        example: `${API_BASE}/screener?scope=daily:us&ma5_gt_ma10=true&rsi14_lt=30&sort=change_1d&limit=20`,
        note: "快照由 GitHub Actions 定时预计算写入 KV（screener_precompute.py）",
      },
    });
  }

  if (!env || !env.STATIC_KV) {
    return error("KV 未绑定（STATIC_KV），选股器不可用", 503);
  }

  const kvKey = `screener:${scope}`;
  // 兼容旧版 scope 命名：daily → 1d
  const normalizedKey = kvKey.replace(":daily:", ":1d:");
  let snapshotText;
  try {
    snapshotText = await env.STATIC_KV.get(normalizedKey);
  } catch (e) {
    return error(`KV 读取失败: ${e.message}`, 502);
  }
  if (!snapshotText) {
    return error(`无选股快照: ${normalizedKey}。请先运行 screener_precompute.py`, 404);
  }

  let snapshot;
  try {
    snapshot = JSON.parse(snapshotText);
  } catch {
    return error(`快照 JSON 解析失败: ${normalizedKey}`, 502);
  }

  const symbols = Object.keys(snapshot);
  if (symbols.length === 0) {
    return json({ scope, count: 0, results: [], note: "快照为空" });
  }

  // 收集过滤条件
  const numericFilters = [];
  const boolFilters = [];
  for (const [key, value] of params.entries()) {
    if (["scope","market","sort","order","limit"].includes(key)) continue;
    if (SCREENER_BOOL_FIELDS.has(key)) {
      if (value === "true" || value === "1") boolFilters.push(key);
      continue;
    }
    const parts = key.split("_");
    if (parts.length >= 2) {
      const op = parts[parts.length - 1];
      if (SCREENER_OPS[op]) {
        const field = parts.slice(0, -1).join("_");
        const numVal = parseFloat(value);
        if (!isNaN(numVal)) numericFilters.push({ field, op, value: numVal });
      }
    }
  }

  // 过滤
  const results = [];
  for (const symbol of symbols) {
    const data = snapshot[symbol];
    if (!data) continue;
    let pass = true;

    for (const f of numericFilters) {
      const val = data[f.field];
      if (val === undefined || val === null || isNaN(val)) { pass = false; break; }
      if (!SCREENER_OPS[f.op](val, f.value)) { pass = false; break; }
    }
    if (!pass) continue;

    for (const cond of boolFilters) {
      switch (cond) {
        case "ma5_gt_ma10": if (!(data.ma5 > data.ma10)) pass = false; break;
        case "ma10_gt_ma20": if (!(data.ma10 > data.ma20)) pass = false; break;
        case "ma20_gt_ma60": if (!(data.ma20 > data.ma60)) pass = false; break;
        case "macd_gt_signal": if (!(data.macd > data.macd_signal)) pass = false; break;
        case "macd_gt_zero": if (!(data.macd > 0)) pass = false; break;
        case "price_gt_ma20": if (!(data.close > data.ma20)) pass = false; break;
        case "price_gt_ma60": if (!(data.close > data.ma60)) pass = false; break;
        case "rsi_oversold": if (!(data.rsi14 < 30)) pass = false; break;
        case "rsi_overbought": if (!(data.rsi14 > 70)) pass = false; break;
        case "volume_surge": if (!(data.volume > data.volume_ma20 * 2)) pass = false; break;
      }
      if (!pass) break;
    }

    if (pass) results.push({ symbol, ...data });
  }

  // 排序
  const sortField = params.get("sort") || "change_1d";
  const sortOrder = (params.get("order") || "desc").toLowerCase();
  results.sort((a, b) => {
    const av = a[sortField], bv = b[sortField];
    if (av === undefined || av === null) return 1;
    if (bv === undefined || bv === null) return -1;
    return sortOrder === "asc" ? av - bv : bv - av;
  });

  const limit = Math.min(parseInt(params.get("limit") || "50", 10), 500);
  const page = results.slice(0, limit);

  return json({
    scope, total: symbols.length, matched: results.length,
    count: page.length, limit, sort: sortField, order: sortOrder,
    results: page,
  });
}

// ============================================================
// 状态
// ============================================================
function handleStatus(request) {
  const currentBase = request ? `https://${request.url ? new URL(request.url).host : API_BASE}` : API_BASE;
  return json({
    service: "StockAPI",
    base: API_BASE,
    current: currentBase,
    repo: `${REPO_OWNER}/${REPO_NAME}@${REPO_BRANCH}`,
    endpoints: {
      kline: `${API_BASE}/kline`,
      price: `${API_BASE}/price`,
      download: `${API_BASE}/download`,
      quote: `${API_BASE}/quote`,
      news: `${API_BASE}/news`,
      "news-yh": `${API_BASE}/news-yh`,
      "news-em": `${API_BASE}/news-em`,
      "news-yh/live": `${API_BASE}/news-yh/live`,
      "news-em/live": `${API_BASE}/news-em/live`,
      universe: `${API_BASE}/universe`,
      indices: `${API_BASE}/indices`,
      symbols: `${API_BASE}/symbols`,
      screener: `${API_BASE}/screener`,
    },
    intervals: Object.keys(INTERVAL_DIR),
    regions: Object.keys(REGION_LABEL),
    indexes: Object.keys(INDEX_LABEL),
    note: "数据由 GitHub Actions 自动增量采集，5m/15m/30m 由 1m 重采样计算，1h 为雅虎原生小时K线。",
  });
}

// ============================================================
// 首页：项目介绍 + API 文档 + 在线演示（自包含单页）
// ============================================================
const HOME_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="StockAPI — 免费行情K线数据接口，基于 GitHub Actions 自动采集 + Cloudflare Workers 边缘分发">
<title>StockAPI · 免费行情K线接口</title>
<style>
  :root{
    --bg:#070a0f; --panel:#0e141d; --panel2:#121a26; --line:#1e293b;
    --text:#e6edf3; --muted:#8b98a9; --dim:#5b6675;
    --accent:#34d399; --accent2:#22d3a5; --amber:#fbbf24; --red:#f87171; --blue:#60a5fa;
    --mono:ui-monospace,"SF Mono","SFMono-Regular",Menlo,Consolas,monospace;
    --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  html{scroll-behavior:smooth}
  body{background:var(--bg);color:var(--text);font-family:var(--sans);line-height:1.6;-webkit-font-smoothing:antialiased}
  .wrap{max-width:1080px;margin:0 auto;padding:0 24px}
  a{color:var(--accent2);text-decoration:none}
  a:hover{text-decoration:underline}
  code{font-family:var(--mono)}
  ::selection{background:rgba(52,211,153,.25)}

  nav{position:sticky;top:0;z-index:50;background:rgba(7,10,15,.85);backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}
  .nav{display:flex;align-items:center;justify-content:space-between;height:60px}
  .brand{display:flex;align-items:center;gap:10px;font-weight:700;font-size:17px}
  .brand .dot{width:10px;height:10px;border-radius:50%;background:var(--accent);box-shadow:0 0 12px var(--accent)}
  .brand b{font-family:var(--mono)}
  .nav-links{display:flex;gap:22px;font-size:14px;color:var(--muted)}
  .nav-links a{color:var(--muted)}
  .nav-links a:hover{color:var(--text);text-decoration:none}

  .hero{padding:88px 0 52px}
  .badge{display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);font-size:12px;color:var(--accent);border:1px solid rgba(52,211,153,.3);background:rgba(52,211,153,.08);padding:5px 12px;border-radius:999px;margin-bottom:22px}
  .badge .pulse{width:7px;height:7px;border-radius:50%;background:var(--accent)}
  h1{font-size:clamp(30px,5vw,52px);line-height:1.1;letter-spacing:-.02em;font-weight:800}
  h1 .grad{background:linear-gradient(90deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}
  .sub{margin-top:18px;font-size:17px;color:var(--muted);max-width:680px}
  .codes{margin-top:30px;display:grid;gap:12px}
  .code{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px;font-family:var(--mono);font-size:13.5px;overflow-x:auto;white-space:nowrap}
  .code .cmt{color:var(--dim)}
  .code .cmd{color:var(--accent)}
  .code .url{color:var(--text)}
  .stat-band{margin-top:40px;display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
  .stat{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px}
  .stat .n{font-family:var(--mono);font-size:26px;font-weight:700;color:var(--accent)}
  .stat .l{font-size:12.5px;color:var(--muted);margin-top:2px}

  section{padding:52px 0}
  .sec-head{display:flex;align-items:baseline;gap:12px;margin-bottom:26px}
  .sec-head .idx{font-family:var(--mono);color:var(--accent);font-size:13px}
  .sec-head h2{font-size:24px;font-weight:700;letter-spacing:-.01em}
  .sec-head .tag{font-size:12px;color:var(--dim);font-family:var(--mono)}

  .chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:22px}
  .chip{font-family:var(--mono);font-size:12px;color:var(--muted);border:1px solid var(--line);background:var(--panel);padding:5px 11px;border-radius:999px}
  .chip b{color:var(--accent)}

  .demo{background:var(--panel);border:1px solid var(--line);border-radius:16px;overflow:hidden}
  .demo-tabs{display:flex;gap:4px;padding:10px 12px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
  .demo-tab{font-family:var(--mono);font-size:12.5px;color:var(--muted);padding:8px 14px;border-radius:8px 8px 0 0;cursor:pointer;border-bottom:2px solid transparent}
  .demo-tab.on{color:var(--accent);border-bottom-color:var(--accent);background:rgba(52,211,153,.06)}
  .demo-body{display:grid;grid-template-columns:340px 1fr}
  .demo-form{padding:20px;border-right:1px solid var(--line);display:flex;flex-direction:column;gap:14px}
  .field label{display:block;font-size:12px;color:var(--muted);margin-bottom:6px;font-family:var(--mono)}
  .field input,.field select{width:100%;background:var(--panel2);border:1px solid var(--line);border-radius:8px;color:var(--text);padding:10px 12px;font-family:var(--mono);font-size:13px;outline:none}
  .field input:focus,.field select:focus{border-color:var(--accent)}
  .run{background:var(--accent);color:#03251a;border:none;border-radius:8px;padding:11px;font-family:var(--mono);font-weight:700;font-size:14px;cursor:pointer;transition:filter .15s}
  .run:hover{filter:brightness(1.08)}
  .run:active{transform:translateY(1px)}
  .demo-out{padding:0;margin:0;background:#0a0f16;font-family:var(--mono);font-size:12.5px;line-height:1.7;overflow:auto;max-height:460px}
  .demo-out pre{padding:20px;white-space:pre-wrap;word-break:break-word}
  .demo-out .ok{color:var(--accent)}
  .demo-out .err{color:var(--red)}
  .demo-out .dim{color:var(--dim)}

  .table-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px}
  table{width:100%;border-collapse:collapse;font-size:14px;min-width:640px}
  th,td{text-align:left;padding:11px 16px;border-bottom:1px solid var(--line)}
  th{font-family:var(--mono);font-size:12px;color:var(--muted);background:var(--panel);text-transform:uppercase;letter-spacing:.04em}
  tr:last-child td{border-bottom:none}
  td code{color:var(--accent2);background:rgba(34,211,165,.08);padding:1px 6px;border-radius:5px;font-size:12.5px}
  td .req{color:var(--red);font-weight:700}
  td .opt{color:var(--dim)}

  .ep-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .ep{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px}
  .ep .m{font-family:var(--mono);color:var(--accent);font-size:13px;font-weight:700}
  .ep .d{font-size:13px;color:var(--muted);margin-top:4px}
  .ep .ex{font-family:var(--mono);font-size:12px;color:var(--blue);margin-top:8px;background:var(--panel2);padding:6px 10px;border-radius:8px;overflow-x:auto;white-space:nowrap}

  .fields{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  .fcard{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:18px}
  .fcard h3{font-size:15px;margin-bottom:12px;font-family:var(--mono)}
  .fcard h3 .tag{font-size:11px;color:var(--dim);font-weight:400}
  .fcard ul{list-style:none}
  .fcard li{display:flex;align-items:baseline;gap:10px;padding:7px 0;border-bottom:1px dashed var(--line);font-size:13.5px}
  .fcard li:last-child{border-bottom:none}
  .fcard .k{font-family:var(--mono);color:var(--accent2);min-width:96px}
  .fcard .d{color:var(--muted)}

  /* 详细字段说明表 */
  .fsub{font-size:16px;font-weight:700;margin:30px 0 12px;font-family:var(--mono)}
  .fsub .tag{font-size:11px;color:var(--dim);font-weight:400;font-family:var(--mono)}
  .ftable{overflow-x:auto;border:1px solid var(--line);border-radius:12px;margin-bottom:8px}
  .ftable table{width:100%;border-collapse:collapse;font-size:13px;min-width:640px}
  .ftable th,.ftable td{text-align:left;padding:9px 12px;border-bottom:1px solid var(--line);vertical-align:top}
  .ftable th{background:var(--panel);font-family:var(--mono);font-weight:600;color:var(--muted);font-size:12px}
  .ftable tr:last-child td{border-bottom:none}
  .ftable td code{font-family:var(--mono);color:var(--accent2);font-size:12px}
  .ftable .dim-row{color:var(--dim);font-size:12px}

  footer{padding:40px 0 56px;border-top:1px solid var(--line);color:var(--dim);font-size:13px}
  .foot{display:flex;flex-wrap:wrap;gap:8px;justify-content:space-between;align-items:center}

  @media(max-width:860px){
    .demo-body{grid-template-columns:1fr}
    .demo-form{border-right:none;border-bottom:1px solid var(--line)}
    .fields,.ep-grid{grid-template-columns:1fr}
    .stat-band{grid-template-columns:1fr 1fr}
    .nav-links{display:none}
    .hero{padding:60px 0 36px}
  }
</style>
</head>
<body>
<nav><div class="wrap nav">
  <div class="brand"><span class="dot"></span><b>StockAPI</b></div>
  <div class="nav-links">
    <a href="#demo">在线演示</a>
    <a href="#endpoints">接口一览</a>
    <a href="#api">API 文档</a>
    <a href="#fields">数据字段</a>
    <a href="#examples">示例</a>
  </div>
</div></nav>

<header class="hero"><div class="wrap">
  <div class="badge"><span class="pulse"></span> 免费 · 无需 Key · 全球市场 · 边缘分发 · 实时价格</div>
  <h1>免费行情 K 线<br>数据 <span class="grad">接口</span></h1>
  <p class="sub">由 <b>GitHub Actions 自动采集</b> A股全市场(4595) / 美股Russell1000(1022) / 港股恒生(87) / 韩股KOSPI200(48) / <b>美股ETF(355)</b> 的 <b>日K、1分钟、5、15、30分钟、1小时</b> K线（美股含盘前盘后延长时段），gzip 压缩存入 <b>Cloudflare R2</b>，由 <b>Cloudflare Workers</b> 在边缘节点自动解压并转成 JSON / CSV 返回；<b>/price</b> 实时价格当场调取 Yahoo API，零服务器成本，供量化系统直接调用。</p>
  <div class="codes">
    <div class="code"><span class="cmt"># 一行请求，返回 AAPL 最近 5 条日K（历史数据走 R2）</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=AAPL&amp;interval=1d&amp;limit=5</span>"</div>
    <div class="code"><span class="cmt"># 实时价格（当场调取 Yahoo，非缓存）</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/price?symbol=AAPL</span>" &nbsp; <span class="cmd">curl</span> "<span class="url">${API_BASE}/price?symbol=600519.SS</span>"</div>
  </div>
  <div class="stat-band">
    <div class="stat"><div class="n">6107</div><div class="l">股票 + ETF（A股/美股/恒生/KOSPI200/美股ETF）</div></div>
    <div class="stat"><div class="n">6</div><div class="l">周期（日K/1m/5m/15m/30m/1h）</div></div>
    <div class="stat"><div class="n">5</div><div class="l">市场（A股/美股/港股/韩股/美股ETF）</div></div>
    <div class="stat"><div class="n">0</div><div class="l">费用（公开仓库 + Workers 免费额度）</div></div>
  </div>
</div></header>

<section id="demo"><div class="wrap">
  <div class="sec-head"><span class="idx">01</span><h2>在线演示</h2><span class="tag">GET · JSON</span></div>
  <div class="demo">
    <div class="demo-tabs">
      <div class="demo-tab on" data-t="kline">kline</div>
      <div class="demo-tab" data-t="price">price</div>
      <div class="demo-tab" data-t="quote">quote</div>
      <div class="demo-tab" data-t="universe">universe</div>
    </div>
    <div class="demo-body">
      <div class="demo-form">
        <div class="field" data-f="kline"><label>symbol</label><input id="sym" value="AAPL" spellcheck="false"></div>
        <div class="field" data-f="kline"><label>interval</label>
          <select id="itv">
            <option value="1d" selected>1d · 日线</option>
            <option value="1h">1h · 1小时</option>
            <option value="30m">30m · 半小时</option>
            <option value="15m">15m · 15分钟</option>
            <option value="5m">5m · 5分钟</option>
            <option value="1m">1m · 1分钟</option>
          </select>
        </div>
        <div class="field" data-f="kline"><label>limit</label><input id="lim" value="10" type="number" min="1"></div>
        <div class="field" data-f="price" style="display:none"><label>symbol</label><input id="psym" value="AAPL" spellcheck="false"></div>
        <div class="field" data-f="quote" style="display:none"><label>symbol</label><input id="qsym" value="0700.HK" spellcheck="false"></div>
        <div class="field" data-f="universe" style="display:none"><label>index</label>
          <select id="uidx">
            <option value="csi300" selected>csi300 · 沪深300</option>
            <option value="csi500">csi500 · 中证500</option>
            <option value="nasdaq100">nasdaq100 · 纳指100</option>
            <option value="sp500">sp500 · 标普500</option>
            <option value="hsi">hsi · 恒生指数</option>
          </select>
        </div>
        <button class="run" id="go">运行请求</button>
      </div>
      <div class="demo-out"><pre id="out"><span class="dim">// 在左侧输入参数，点击「运行请求」查看返回结果。&#10;// kline：历史K线（走 R2） AAPL / 0700.HK / 600519.SS / 000001.SZ&#10;// price：实时价格（当场调取 Yahoo） AAPL / 600519.SS / 005930.KS&#10;// quote：个股名称、行业、市值、最新价等&#10;// universe：获取指数成分股代码清单</span></pre></div>
    </div>
  </div>
</div></section>

<section id="endpoints"><div class="wrap">
  <div class="sec-head"><span class="idx">02</span><h2>接口一览</h2></div>
  <div class="ep-grid">
    <div class="ep"><div class="m">GET /kline</div><div class="d">K线数据（日K / 1m / 5m / 15m / 30m / 1h）</div><div class="ex">/kline?symbol=AAPL&amp;interval=1d&amp;limit=5</div></div>
    <div class="ep"><div class="m">GET /price</div><div class="d">实时价格（当场调取 Yahoo API，含涨跌幅/52周高低，非数据库缓存）</div><div class="ex">/price?symbol=AAPL</div></div>
    <div class="ep"><div class="m">GET /download</div><div class="d">下载 gzip 压缩的原始 CSV（体积小，可离线分析）</div><div class="ex">/download?symbol=AAPL&amp;interval=1h</div></div>
    <div class="ep"><div class="m">GET /quote</div><div class="d">个股元数据（名称/行业/市值/最新价/52周高低…）</div><div class="ex">/quote?symbol=600519.SS</div></div>
    <div class="ep"><div class="m">GET /news</div><div class="d">聚合新闻（雅虎 + 东方财富扁平合并），按发布时间倒序，默认 20 条，加 limit 获取更多</div><div class="ex">/news?limit=50</div></div>
    <div class="ep"><div class="m">GET /news-yh</div><div class="d">雅虎香港财经头条（繁体），默认 20 条，采集端每 5 分钟刷新</div><div class="ex">/news-yh?limit=24</div></div>
    <div class="ep"><div class="m">GET /news-em</div><div class="d">东方财富 7x24h 快讯（简体），默认 20 条，采集端每 5 分钟刷新</div><div class="ex">/news-em?limit=80</div></div>
    <div class="ep"><div class="m">GET /universe</div><div class="d">指数成分股清单（csi300/csi500/nasdaq100/sp500/hsi）</div><div class="ex">/universe?index=csi300</div></div>
    <div class="ep"><div class="m">GET /indices</div><div class="d">全部可用指数/清单及其成分数量</div><div class="ex">/indices</div></div>
    <div class="ep"><div class="m">GET /symbols</div><div class="d">按区域列出股票代码（支持分页）</div><div class="ex">/symbols?region=cn&amp;limit=10</div></div>
    <div class="ep"><div class="m">GET /status</div><div class="d">服务配置信息（区间/区域/指数）</div><div class="ex">/status</div></div>
  </div>
</div></section>

<section id="api"><div class="wrap">
  <div class="sec-head"><span class="idx">03</span><h2>API 文档</h2></div>
  <div class="table-wrap">
    <table>
      <tr><th>参数</th><th>必填</th><th>默认</th><th>说明</th></tr>
      <tr><td><code>symbol</code></td><td><span class="req">是</span></td><td>—</td><td>股票代码：<code>AAPL</code> / <code>0700.HK</code> / <code>600519.SS</code> / <code>000001.SZ</code></td></tr>
      <tr><td><code>interval</code></td><td><span class="opt">否</span></td><td><code>1d</code></td><td>周期：<code>1d</code>(日线) <code>1m</code>(1分钟) <code>5m</code>(5分钟) <code>15m</code>(15分钟) <code>30m</code>(半小时) <code>1h</code>(1小时)</td></tr>
      <tr><td><code>start</code></td><td><span class="opt">否</span></td><td>—</td><td>起始日期 <code>YYYY-MM-DD</code>（含）</td></tr>
      <tr><td><code>end</code></td><td><span class="opt">否</span></td><td>—</td><td>结束日期 <code>YYYY-MM-DD</code>（含）</td></tr>
      <tr><td><code>limit</code></td><td><span class="opt">否</span></td><td>全部</td><td>最多返回行数；默认返回最新 N 条</td></tr>
      <tr><td><code>order</code></td><td><span class="opt">否</span></td><td><code>asc</code></td><td><code>asc</code> 时间升序 / <code>desc</code> 最新在前</td></tr>
      <tr><td><code>format</code></td><td><span class="opt">否</span></td><td><code>json</code></td><td><code>json</code> / <code>csv</code>（返回原始 CSV 文本）</td></tr>
    </table>
  </div>
  <p style="margin-top:14px;font-size:13px;color:var(--muted)">区域自动识别：裸代码→美股，<code>.HK</code>→港股，<code>.SS/.SZ</code>→A股，<code>.KS/.KQ</code>→韩股。也可用 <code>region</code> 参数显式指定。</p>

  <div class="ep-grid" style="margin-top:26px">
    <div class="ep"><div class="m">GET /price</div><div class="d">实时最新价格快照：从最新K线读取，返回价格、涨跌幅、最高最低、成交量。</div><div class="ex">/price?symbol=AAPL</div></div>
    <div class="ep"><div class="m">GET /download</div><div class="d">下载 gzip 压缩的原始 CSV（保存为 .csv.gz，体积小，适合离线分析）。</div><div class="ex">/download?symbol=600519.SS&amp;interval=1d</div></div>
  </div>
</div></section>

<section id="fields"><div class="wrap">
  <div class="sec-head"><span class="idx">04</span><h2>字段说明</h2></div>
  <p style="font-size:13px;color:var(--muted);margin-bottom:18px">以下为各接口返回字段的完整说明：名称、类型、描述、示例值、是否可为空（<code>null</code> / 缺失）。</p>

  <h3 class="fsub">一、K线数据 <span class="tag">GET /kline</span></h3>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>示例</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>symbol</code></td><td>string</td><td>股票代码</td><td><code>"AAPL"</code></td><td>否</td></tr>
        <tr><td><code>region</code></td><td>string</td><td>市场区域</td><td><code>"us"</code></td><td>否</td></tr>
        <tr><td><code>interval</code></td><td>string</td><td>K线周期</td><td><code>"1d"</code></td><td>否</td></tr>
        <tr><td><code>count</code></td><td>number</td><td>返回条数</td><td><code>5</code></td><td>否</td></tr>
        <tr><td><code>order</code></td><td>string</td><td>排序方向</td><td><code>"asc"</code></td><td>否</td></tr>
        <tr><td><code>data[]</code></td><td>array</td><td>K线数组</td><td><code>[...]</code></td><td>否（空则 <code>[]</code>）</td></tr>
        <tr><td><code>data[].Date</code></td><td>string</td><td>交易日期（日线）</td><td><code>"2026-08-14"</code></td><td>否</td></tr>
        <tr><td><code>data[].Datetime</code></td><td>string</td><td>时间戳（分钟线，UTC）</td><td><code>"2026-08-14 23:59:59"</code></td><td>否</td></tr>
        <tr><td><code>data[].Open</code></td><td>number</td><td>开盘价</td><td><code>305.10</code></td><td>否</td></tr>
        <tr><td><code>data[].High</code></td><td>number</td><td>最高价</td><td><code>305.66</code></td><td>否</td></tr>
        <tr><td><code>data[].Low</code></td><td>number</td><td>最低价</td><td><code>300.57</code></td><td>否</td></tr>
        <tr><td><code>data[].Close</code></td><td>number</td><td>收盘价</td><td><code>302.25</code></td><td>否</td></tr>
        <tr><td><code>data[].Adj Close</code></td><td>number</td><td>复权收盘价（延长时段 bar 可能缺失）</td><td><code>302.25</code></td><td>是（延长时段为 null）</td></tr>
        <tr><td><code>data[].Volume</code></td><td>number</td><td>成交量（股，延长时段为 0）</td><td><code>41657800</code></td><td>是（延长时段为 0）</td></tr>
      </tbody>
    </table>
  </div>

  <h3 class="fsub">二、实时价格 <span class="tag">GET /price</span></h3>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>示例</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>symbol</code></td><td>string</td><td>股票代码</td><td><code>"AAPL"</code></td><td>否</td></tr>
        <tr><td><code>region</code></td><td>string</td><td>市场区域</td><td><code>"us"</code></td><td>否</td></tr>
        <tr><td><code>name</code></td><td>string</td><td>公司名称</td><td><code>"Apple Inc."</code></td><td>是（meta 缺失）</td></tr>
        <tr><td><code>price</code></td><td>number</td><td>最新价</td><td><code>305.77</code></td><td>否</td></tr>
        <tr><td><code>currency</code></td><td>string</td><td>计价货币</td><td><code>"USD"</code></td><td>是（meta 缺失）</td></tr>
        <tr><td><code>datetime</code></td><td>string</td><td>最新bar时间</td><td><code>"2026-08-14 19:00:00"</code></td><td>否</td></tr>
        <tr><td><code>interval</code></td><td>string</td><td>价格来源周期（1h/1m/1d）</td><td><code>"1h"</code></td><td>否</td></tr>
        <tr><td><code>open</code></td><td>number</td><td>当日/区间开盘</td><td><code>305.10</code></td><td>否</td></tr>
        <tr><td><code>high</code></td><td>number</td><td>当日/区间最高</td><td><code>305.66</code></td><td>否</td></tr>
        <tr><td><code>low</code></td><td>number</td><td>当日/区间最低</td><td><code>300.57</code></td><td>否</td></tr>
        <tr><td><code>close</code></td><td>number</td><td>最新收盘</td><td><code>305.77</code></td><td>否</td></tr>
        <tr><td><code>volume</code></td><td>number</td><td>最新bar成交量</td><td><code>123456</code></td><td>是（延长时段 0）</td></tr>
        <tr><td><code>change</code></td><td>number</td><td>涨跌额（相对前收盘）</td><td><code>-7.56</code></td><td>是（数据不足）</td></tr>
        <tr><td><code>changePercent</code></td><td>number</td><td>涨跌幅（%）</td><td><code>-2.41</code></td><td>是（数据不足）</td></tr>
        <tr><td><code>source</code></td><td>string</td><td>数据来源说明</td><td><code>"Yahoo 实时行情（当场调取）"</code></td><td>否</td></tr>
        <tr><td><code>fiftyTwoWeekHigh</code></td><td>number</td><td>52周最高价（实时）</td><td><code>344.57</code></td><td>是</td></tr>
        <tr><td><code>fiftyTwoWeekLow</code></td><td>number</td><td>52周最低价（实时）</td><td><code>223.78</code></td><td>是</td></tr>
        <tr><td><code>marketTime</code></td><td>string</td><td>最新行情时间（ISO）</td><td><code>"2026-08-14T20:00:01Z"</code></td><td>是</td></tr>
      </tbody>
    </table>
  </div>

  <h3 class="fsub">三、个股元数据 <span class="tag">GET /quote</span></h3>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>示例</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>symbol</code></td><td>string</td><td>股票代码</td><td><code>"AAPL"</code></td><td>否</td></tr>
        <tr><td><code>region</code></td><td>string</td><td>市场区域</td><td><code>"us"</code></td><td>否</td></tr>
        <tr><td><code>name</code></td><td>string</td><td>公司名称（longName 或 shortName）</td><td><code>"Apple Inc."</code></td><td>是</td></tr>
        <tr><td><code>currency</code></td><td>string</td><td>计价货币</td><td><code>"USD"</code></td><td>是</td></tr>
        <tr><td><code>exchange</code></td><td>string</td><td>交易所名称</td><td><code>"NasdaqGS"</code></td><td>是</td></tr>
        <tr><td><code>isin</code></td><td>string</td><td>ISIN 国际证券识别码</td><td><code>"US0378331005"</code></td><td>是</td></tr>
        <tr><td><code>instrumentType</code></td><td>string</td><td>证券类型（EQUITY/ETF）</td><td><code>"EQUITY"</code></td><td>是</td></tr>
        <tr><td><code>quoteType</code></td><td>string</td><td>Yahoo 报价类型</td><td><code>"EQUITY"</code></td><td>是</td></tr>
        <tr><td><code>sector</code></td><td>string</td><td>所属行业板块（如 Technology）</td><td><code>"Technology"</code></td><td>是</td></tr>
        <tr><td><code>industry</code></td><td>string</td><td>细分行业</td><td><code>"Consumer Electronics"</code></td><td>是</td></tr>
        <tr><td><code>firstTradeDate</code></td><td>number</td><td>上市日期（Unix 秒）</td><td><code>345459600</code></td><td>是</td></tr>
        <tr><td><code>timezone</code></td><td>string</td><td>交易所时区</td><td><code>"America/New_York"</code></td><td>是</td></tr>
        <tr><td><code>gmtoffset</code></td><td>number</td><td>时区偏移（秒）</td><td><code>-18000</code></td><td>是</td></tr>
        <tr><td><code>hasPrePostMarketData</code></td><td>boolean</td><td>是否有盘前盘后数据</td><td><code>true</code></td><td>是</td></tr>
        <tr><td><code>regularMarketPrice</code></td><td>number</td><td>最新价</td><td><code>305.93</code></td><td>是</td></tr>
        <tr><td><code>regularMarketDayHigh</code></td><td>number</td><td>当日最高</td><td><code>306.20</code></td><td>是</td></tr>
        <tr><td><code>regularMarketDayLow</code></td><td>number</td><td>当日最低</td><td><code>300.57</code></td><td>是</td></tr>
        <tr><td><code>regularMarketVolume</code></td><td>number</td><td>当日成交量</td><td><code>41657800</code></td><td>是</td></tr>
        <tr><td><code>regularMarketTime</code></td><td>number</td><td>最新行情时间（Unix 秒）</td><td><code>1786828800</code></td><td>是</td></tr>
        <tr><td><code>fiftyTwoWeekHigh</code></td><td>number</td><td>52周最高价</td><td><code>344.57</code></td><td>是</td></tr>
        <tr><td><code>fiftyTwoWeekLow</code></td><td>number</td><td>52周最低价</td><td><code>223.78</code></td><td>是</td></tr>
        <tr><td><code>chartPreviousClose</code></td><td>number</td><td>前收盘价</td><td><code>305.26</code></td><td>是</td></tr>
        <tr><td><code>change</code></td><td>number</td><td>涨跌额</td><td><code>0.67</code></td><td>是</td></tr>
        <tr><td><code>changePercent</code></td><td>number</td><td>涨跌幅（%）</td><td><code>0.22</code></td><td>是</td></tr>
      </tbody>
    </table>
  </div>

  <h3 class="fsub">quote 财务/估值子字段 <span class="tag">旧 yfinance 完整 meta（若已入库）</span></h3>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>marketCap</code></td><td>number</td><td>总市值（元）</td><td>是</td></tr>
        <tr><td><code>trailingPE / forwardPE</code></td><td>number</td><td>TTM / 前瞻市盈率</td><td>是</td></tr>
        <tr><td><code>priceToBook</code></td><td>number</td><td>市净率（P/B）</td><td>是</td></tr>
        <tr><td><code>dividendYield / dividendRate</code></td><td>number</td><td>股息率（小数）/ 每股股息</td><td>是</td></tr>
        <tr><td><code>trailingEps / forwardEps</code></td><td>number</td><td>每股收益 TTM / 预测 EPS</td><td>是</td></tr>
        <tr><td><code>beta</code></td><td>number</td><td>Beta 波动系数</td><td>是</td></tr>
        <tr><td><code>volume / averageVolume</code></td><td>number</td><td>成交量 / 平均成交量</td><td>是</td></tr>
        <tr><td><code>sharesOutstanding / floatShares</code></td><td>number</td><td>总股本 / 流通股本</td><td>是</td></tr>
        <tr><td><code>targetMeanPrice / HighPrice / LowPrice</code></td><td>number</td><td>分析师目标价（均值/最高/最低）</td><td>是</td></tr>
        <tr><td><code>recommendationKey</code></td><td>string</td><td>分析师评级（buy/hold/sell）</td><td>是</td></tr>
        <tr><td><code>totalRevenue / grossProfits</code></td><td>number</td><td>总营收 / 毛利</td><td>是</td></tr>
        <tr><td><code>freeCashflow</code></td><td>number</td><td>自由现金流</td><td>是</td></tr>
        <tr><td><code>totalDebt / totalCash</code></td><td>number</td><td>总负债 / 总现金</td><td>是</td></tr>
        <tr><td><code>profitMargins</code></td><td>number</td><td>利润率（小数）</td><td>是</td></tr>
        <tr><td><code>returnOnEquity / returnOnAssets</code></td><td>number</td><td>净资产收益率 / 总资产收益率</td><td>是</td></tr>
        <tr><td><code>earningsGrowth / revenueGrowth</code></td><td>number</td><td>盈利 / 营收增长率（小数）</td><td>是</td></tr>
        <tr><td><code>fiftyDayAverage / twoHundredDayAverage</code></td><td>number</td><td>50日 / 200日均线</td><td>是</td></tr>
        <tr><td><code>financials</code></td><td>array</td><td>年度财务数据（若已入库）</td><td>是</td></tr>
        <tr><td><code>dividends</code></td><td>array</td><td>历史分红记录（若已入库）</td><td>是</td></tr>
        <tr><td><code>splits</code></td><td>array</td><td>拆股记录（若已入库）</td><td>是</td></tr>
        <tr><td><code>recommendations_summary</code></td><td>array</td><td>分析师评级汇总（若已入库）</td><td>是</td></tr>
        <tr><td><code>major_holders / institutional_holders</code></td><td>array</td><td>大股东 / 机构持股（若已入库）</td><td>是</td></tr>
      </tbody>
    </table>
  </div>

  <h3 class="fsub">四、新闻接口 <span class="tag">GET /news · /news-yh · /news-em</span></h3>
  <div class="fcard" style="margin-top:10px;margin-bottom:16px">
    <ul>
      <li><span class="k">通用外层</span><span class="d"><code>{ total, count, limit, items: [...] }</code>，默认 <code>limit=20</code>，用 <code>?limit=N</code> 取更多</span></li>
      <li><span class="k">采集频率</span><span class="d">GitHub Actions 每 5 分钟刷新一次 R2 中的 <code>news/yh.json</code> 与 <code>news/em.json</code></span></li>
      <li><span class="k">边缘缓存</span><span class="d">/news 30s /news-yh 60s /news-em 30s，命中时不进 Worker、不耗 R2 读</span></li>
    </ul>
  </div>

  <h4 class="fsub2">4.1 雅虎香港头条 · <code>items[]</code> 字段（<code>/news-yh</code>）</h4>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>示例</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>title</code></td><td>string</td><td>新闻标题（繁体中文）</td><td><code>"亞洲股市普遍上漲，日本、中國延續升勢"</code></td><td>否</td></tr>
        <tr><td><code>url</code></td><td>string</td><td>Yahoo 新闻原文链接（<code>hk.finance.yahoo.com/news/...</code>）</td><td><code>"https://hk.finance.yahoo.com/news/..."</code></td><td>否</td></tr>
        <tr><td><code>pub_ts</code></td><td>number</td><td>发布时间（Unix 秒）</td><td><code>1786944538</code></td><td>是</td></tr>
        <tr><td><code>pub_time</code></td><td>string</td><td>发布时间（ISO 8601，已本地化）</td><td><code>"2026-08-17T05:28:58+00:00"</code></td><td>是</td></tr>
        <tr><td><code>rel_time</code></td><td>string</td><td>相对时间（雅虎原始，繁体中文）</td><td><code>"20分前"</code>/<code>"剛剛"</code>/<code>"昨日"</code></td><td>是</td></tr>
        <tr><td><code>publisher</code></td><td>string</td><td>新闻来源（媒体名）</td><td><code>"Investing.com HK"</code>/<code>"AASTOCKS"</code></td><td>是</td></tr>
        <tr><td><code>source</code></td><td>string</td><td>固定标识</td><td><code>"Yahoo Finance HK"</code></td><td>否</td></tr>
      </tbody>
    </table>
  </div>

  <h4 class="fsub2">4.2 东方财富 7x24h · <code>items[]</code> 字段（<code>/news-em</code>）</h4>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>示例</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>id</code></td><td>string</td><td>东方财富新闻 ID</td><td><code>"202608173843023630"</code></td><td>否</td></tr>
        <tr><td><code>title</code></td><td>string</td><td>新闻标题（简体中文）</td><td><code>"上证指数涨1%"</code></td><td>否</td></tr>
        <tr><td><code>digest</code></td><td>string</td><td>新闻摘要 / 第一段正文</td><td><code>"上证指数涨幅扩大至1%，深证成指涨1.94%…"</code></td><td>是</td></tr>
        <tr><td><code>showtime</code></td><td>string</td><td>东方财富显示时间（亚洲/上海时区字符串）</td><td><code>"2026-08-17 13:44:32"</code></td><td>否</td></tr>
        <tr><td><code>pub_ts</code></td><td>number</td><td>发布时间（Unix 秒）</td><td><code>1786945472</code></td><td>是</td></tr>
        <tr><td><code>pub_time</code></td><td>string</td><td>发布时间（ISO 8601）</td><td><code>"2026-08-17T05:44:32+00:00"</code></td><td>是</td></tr>
        <tr><td><code>url_pc</code></td><td>string</td><td>东方财富 PC 版原文链接</td><td><code>"http://finance.eastmoney.com/a/..."</code></td><td>是</td></tr>
        <tr><td><code>url_mobile</code></td><td>string</td><td>东方财富 移动版原文链接</td><td><code>"https://wap.eastmoney.com/a/..."</code></td><td>是</td></tr>
        <tr><td><code>image</code></td><td>string</td><td>封面图 URL</td><td><code>"https://..."</code></td><td>是</td></tr>
        <tr><td><code>editor</code></td><td>string</td><td>编辑名</td><td><code>"编辑A"</code></td><td>是</td></tr>
        <tr><td><code>columns</code></td><td>array[string]</td><td>东方财富栏目编号</td><td><code>["100","102","104"]</code></td><td>是</td></tr>
        <tr><td><code>comment_num</code></td><td>number</td><td>评论数</td><td><code>7</code></td><td>是</td></tr>
        <tr><td><code>news_type</code></td><td>string</td><td>新闻类型编号</td><td><code>"1"</code></td><td>是</td></tr>
        <tr><td><code>source</code></td><td>string</td><td>固定标识</td><td><code>"东方财富 7x24h"</code></td><td>否</td></tr>
      </tbody>
    </table>
  </div>

  <h4 class="fsub2">4.3 聚合新闻 · <code>items[]</code> 字段（<code>/news</code>）</h4>
  <div class="ftable">
    <table>
      <thead><tr><th>字段</th><th>类型</th><th>描述</th><th>示例</th><th>可空</th></tr></thead>
      <tbody>
        <tr><td><code>channel</code></td><td>string</td><td>来源通道</td><td><code>"eastmoney"</code> 或 <code>"yahoo_hk"</code></td><td>否</td></tr>
        <tr><td><code>title</code></td><td>string</td><td>标题</td><td>—</td><td>否</td></tr>
        <tr><td><code>url</code></td><td>string</td><td>原文链接（yahoo 直链 / eastmoney 优先 PC 版）</td><td>—</td><td>否</td></tr>
        <tr><td><code>digest</code></td><td>string</td><td>摘要（eastmoney 有；yahoo_hk 为 <code>null</code>）</td><td>—</td><td>是</td></tr>
        <tr><td><code>pub_ts</code></td><td>number</td><td>发布时间（Unix 秒），聚合列表按此字段倒序</td><td>—</td><td>是</td></tr>
        <tr><td><code>pub_time</code></td><td>string</td><td>发布时间（ISO 8601）</td><td>—</td><td>是</td></tr>
        <tr><td><code>showtime</code></td><td>string</td><td>东方财富显示时间（yahoo_hk 为 <code>null</code>）</td><td><code>"2026-08-17 13:44:32"</code></td><td>是</td></tr>
        <tr><td><code>rel_time</code></td><td>string</td><td>雅虎相对时间（eastmoney 无此字段）</td><td><code>"20分前"</code></td><td>是</td></tr>
        <tr><td><code>publisher</code></td><td>string</td><td>媒体来源；eastmoney 固定 <code>"东方财富"</code></td><td>—</td><td>是</td></tr>
        <tr><td><code>editor</code></td><td>string</td><td>编辑；仅 eastmoney 有</td><td>—</td><td>是</td></tr>
        <tr><td><code>comment_num</code></td><td>number</td><td>评论数；仅 eastmoney 有</td><td><code>7</code></td><td>是</td></tr>
      </tbody>
    </table>
  </div>

  <div class="fcard" style="margin-top:18px">
    <h3>K线时间范围与数据说明</h3>
    <ul>
      <li><span class="k">日K</span><span class="d">近 5 年；不含延长时段（盘中聚合）</span></li>
      <li><span class="k">1小时K</span><span class="d">近 6 个月；美股含盘前盘后延长时段（4:00–20:00 美东）</span></li>
      <li><span class="k">1分钟K</span><span class="d">近 5 天；美股含延长时段</span></li>
      <li><span class="k">5m/15m/30m</span><span class="d">由 1m 重采样派生（Open 首 / High 最高 / Low 最低 / Close 末 / Volume 求和）</span></li>
      <li><span class="k">时间戳</span><span class="d">分钟K为 UTC 时间；日K为交易日期</span></li>
    </ul>
  </div>
</div></section>

<section id="examples"><div class="wrap">
  <div class="sec-head"><span class="idx">05</span><h2>示例</h2></div>
  <div class="codes">
    <div class="code"><span class="cmt"># 最近 5 条日K（默认升序，limit 取最新）</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=MSFT&amp;interval=1d&amp;limit=5</span>"</div>
    <div class="code"><span class="cmt"># 指定日期区间 + 最新在前</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=600519.SS&amp;start=2025-01-01&amp;end=2025-12-31&amp;order=desc</span>"</div>
    <div class="code"><span class="cmt"># 港股 1 小时K线，返回 CSV</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=9988.HK&amp;interval=1h&amp;limit=100&amp;format=csv</span>"</div>
    <div class="code"><span class="cmt"># 个股信息 / 指数成分股 / 区域代码分页</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/quote?symbol=0700.HK</span>" &nbsp; <span class="cmd">curl</span> "<span class="url">${API_BASE}/universe?index=sp500</span>" &nbsp; <span class="cmd">curl</span> "<span class="url">${API_BASE}/symbols?region=us&amp;limit=5</span>"</div>
  </div>
</div></section>

<footer><div class="wrap foot">
  <span>StockAPI · 数据由 GitHub Actions 自动采集，经由 Cloudflare Workers 分发</span>
  <span>数据来自 Yahoo Finance，仅供学习研究</span>
</div></footer>

<script>
(function(){
  var out=document.getElementById("out");
  var tabs=document.querySelectorAll(".demo-tab");
  var active="kline";
  function esc(s){ return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function setTab(t){
    active=t;
    tabs.forEach(function(tb){ tb.classList.toggle("on", tb.getAttribute("data-t")===t); });
    document.querySelectorAll(".demo-form .field").forEach(function(f){
      f.style.display = f.getAttribute("data-f")===t ? "" : "none";
    });
  }
  tabs.forEach(function(tb){ tb.addEventListener("click", function(){ setTab(tb.getAttribute("data-t")); }); });
  function run(){
    var q;
    if(active==="quote"){
      q="/quote?symbol="+encodeURIComponent(document.getElementById("qsym").value.trim()||"0700.HK");
    }else if(active==="universe"){
      q="/universe?index="+encodeURIComponent(document.getElementById("uidx").value);
    }else if(active==="price"){
      q="/price?symbol="+encodeURIComponent(document.getElementById("psym").value.trim()||"AAPL");
    }else{
      var s=document.getElementById("sym").value.trim()||"AAPL";
      var i=document.getElementById("itv").value;
      var l=document.getElementById("lim").value||"10";
      q="/kline?symbol="+encodeURIComponent(s)+"&interval="+i+"&limit="+l;
    }
    out.innerHTML="<span class=\\"dim\\">// GET "+esc(q)+"</span>\\n";
    fetch(q).then(function(r){
      if(!r.ok){ throw new Error("HTTP "+r.status); }
      return r.json();
    }).then(function(d){
      var html="<span class=\\"ok\\">// "+esc(q)+" → "+d.count+"</span>\\n";
      html+=JSON.stringify(d,null,2);
      out.innerHTML=html;
    }).catch(function(e){
      out.innerHTML="<span class=\\"err\\">// 请求失败："+esc(e.message)+"</span>";
    });
  }
  document.getElementById("go").addEventListener("click",run);
  document.addEventListener("DOMContentLoaded",function(){ setTab("kline"); run(); });
})();
</script>
</body>
</html>`;

// ============================================================
// 入口
// ============================================================
// 各接口的边缘缓存 TTL（秒）。
// 历史数据（kline）只会在新数据入库后变化，60s 内可安全命中边缘缓存；
// 元数据/新闻/清单变动更慢，给更长的 TTL 以最大化省额度；
// price 是实时 YAHOO 数据，用最短的 15s 平衡"实时"与省额度。
const CACHE_TTL = {
  index: 300, // 首页（纯静态文档）
  kline: 60,
  quote: 300,
  universe: 300,
  indices: 300,
  symbols: 300,
  download: 60,
  price: 15, // 实时：只缓 15s，避免每次都打 Yahoo/耗 Worker
  "news-yh": 60, // Yahoo HK 头条：每 5 分钟采集，缓存 60s
  "news-em": 30, // 东方财富 7x24h：每 5 分钟采集但内容更新快，缓存 30s
  "news-all": 30,
  screener: 15, // 选股快照：KV 毫秒级读，缓存 15s 平衡新鲜与额度
};

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }

    // 首页
    if (path === "/" || path === "") {
      return edgeCache(url.href, CACHE_TTL.index, env, ctx, () => html(HOME_HTML));
    }

    const params = url.searchParams;

    // 路由分发：除价格外全部缓存到边缘，命中即短路，不读 R2、不耗 CPU
    const routes = {
      "/kline": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleKline(params, env)),
      "/quote": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleQuote(params, env)),
      "/price": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handlePrice(params, env)),
      "/download": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleDownload(params, env)),
      "/universe": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleUniverse(params, env)),
      "/indices": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleIndices(env)),
      "/symbols": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleSymbols(params, env)),
      "/status": () => handleStatus(request),
      "/news-yh": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleAggNews("yh", params, env)),
      "/news-em": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleAggNews("em", params, env)),
      "/news": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleAggNews("all", params, env)),
      "/news-yh/live": () =>
        handleLiveNews("yh", params, env, ctx),
      "/news-em/live": () =>
        handleLiveNews("em", params, env, ctx),
      "/screener": (ttl) =>
        edgeCache(url.href, ttl, env, ctx, () => handleScreener(params, env)),
    };

    const task = routes[path];
    if (task) {
      return await task(CACHE_TTL[path.slice(1)] ?? CACHE_TTL.index);
    }

    return error(
      "Not found. Use /, /kline, /price, /download, /quote, /news, /news-yh, /news-em, /news-yh/live, /news-em/live, /universe, /indices, /symbols, /screener, /status",
      404
    );
  },
};