"""Yahoo Finance 元数据（媒体/行情快照）采集，经反代访问。

从 Yahoo chart API 的 meta 字段采集股票的基本媒体信息（无需认证）：
    - 名称/代码/交易所/币种
    - 实时价格/52周高低/当日高低/成交量
    - 上市日期/时区
    - 当日涨跌/前收盘

存储为 {region}/meta/{symbol}.json，供 API 的 /quote 和 /price 使用。

注意：市值、PE、行业、财务等需要 quoteSummary 接口（需 crumb 认证），
本模块只采集 chart API 免费提供的字段。如需更完整数据可后续扩展。
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any

import os  # noqa: E402

import config  # noqa: E402

_ORIGIN = "https://query1.finance.yahoo.com/v8/finance/chart/"
_PROXY_BASE = config.YAHOO_CHART_PROXY


def fetch_meta(symbol: str, retries: int | None = None, delay: float | None = None) -> dict:
    """拉取单只股票的 chart meta 字段并整理为媒体信息快照。

    返回字典（供 JSON 存储），失败重试后仍失败抛出异常。
    """
    raw_url = _ORIGIN + urllib.parse.quote(symbol) + "?interval=1d&range=5d"
    # 直连优先（服务器/GitHub Actions 海外）；国内可设 YAHOO_USE_PROXY=1 走反代
    url = (f"{_PROXY_BASE.rstrip('/')}/{raw_url}" if os.environ.get("YAHOO_USE_PROXY") == "1" else raw_url)
    retries = config.MAX_RETRIES if retries is None else retries
    delay = config.REQUEST_DELAY if delay is None else delay

    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            meta = (data.get("chart") or {}).get("result", [{}])[0].get("meta") or {}
            return _normalize(meta)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries - 1:
                wait = delay * (2**attempt)
                print(f"    重试 {attempt+1}/{retries-1}（等 {wait:.0f}s）：{exc}", flush=True)
                time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return {}


def _normalize(meta: dict) -> dict:
    """把 chart meta 整理为干净的媒体信息快照。"""
    regular_market_price = meta.get("regularMarketPrice")
    chart_prev_close = meta.get("chartPreviousClose")
    regular_market_day_high = meta.get("regularMarketDayHigh")
    regular_market_day_low = meta.get("regularMarketDayLow")

    change = None
    change_percent = None
    if isinstance(regular_market_price, (int, float)) and isinstance(chart_prev_close, (int, float)) and chart_prev_close:
        change = round(regular_market_price - chart_prev_close, 4)
        change_percent = round((change / chart_prev_close) * 100, 4)

    return {
        "symbol": meta.get("symbol"),
        "longName": meta.get("longName"),
        "shortName": meta.get("shortName"),
        "currency": meta.get("currency"),
        "exchange": meta.get("fullExchangeName") or meta.get("exchangeName"),
        "instrumentType": meta.get("instrumentType"),
        "firstTradeDate": meta.get("firstTradeDate"),  # 上市日期（unix 秒）
        "timezone": meta.get("exchangeTimezoneName"),
        "gmtoffset": meta.get("gmtoffset"),
        "hasPrePostMarketData": meta.get("hasPrePostMarketData"),
        # 行情快照
        "regularMarketPrice": regular_market_price,
        "regularMarketDayHigh": regular_market_day_high,
        "regularMarketDayLow": regular_market_day_low,
        "regularMarketVolume": meta.get("regularMarketVolume"),
        "regularMarketTime": meta.get("regularMarketTime"),
        "fiftyTwoWeekHigh": meta.get("fiftyTwoWeekHigh"),
        "fiftyTwoWeekLow": meta.get("fiftyTwoWeekLow"),
        "chartPreviousClose": chart_prev_close,
        "change": change,
        "changePercent": change_percent,
    }


def fetch_meta_full(symbol: str, retries: int | None = None, delay: float | None = None) -> dict:
    """拉取单只股票的增强 meta：chart meta + search 接口的板块/行业字段。

    Yahoo quoteSummary（市值/PE/财务等完整基本面）已对免费通道系统性 429
    限流，无法获取。这里通过免费的 /v1/finance/search 接口补充
    sector / industry / quoteType / 交易所等字段，尽量接近旧仓库的完整 meta。
    """
    meta = fetch_meta(symbol, retries=retries, delay=delay)
    if not meta:
        return meta

    try:
        import yahoo_news  # noqa: F401 - 复用 search 客户端

        data = yahoo_news.fetch_news(symbol, news_count=0, retries=retries, delay=delay)
        quote = data.get("quote") or {}
        if quote:
            meta["quoteType"] = quote.get("quoteType")
            meta["sector"] = quote.get("sector") or quote.get("sectorDisp")
            meta["industry"] = quote.get("industry") or quote.get("industryDisp")
            if not meta.get("exchange"):
                meta["exchange"] = quote.get("exchDisp") or quote.get("exchange")
            if not meta.get("longName"):
                meta["longName"] = quote.get("longname")
            if not meta.get("shortName"):
                meta["shortName"] = quote.get("shortname")
    except Exception:  # noqa: BLE001 - 板块字段失败不影响主 meta
        pass

    return meta
