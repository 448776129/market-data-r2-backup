"""Yahoo Finance chart API 客户端（通过反代访问）。

绕过 yfinance 库，直接调用 Yahoo 官方 K 线接口 /v8/finance/chart/，
并通过用户自建的 Cloudflare Worker 反代（img2.365200.xyz）转发请求，
解决国内环境直连 Yahoo 被 403 的问题。

接口特性：
  - 原生支持延长时段：includePrePost=true 时美股 1m/5m/15m/30m/60m 均含盘前盘后
    （60m 会多出 16:00~20:00 的盘后bar，而不仅仅是盘中 9:30~16:00）
  - 返回结构统一：meta + timestamp + indicators.quote

用法（供其它 fetch 脚本调用）：
    from yahoo_chart import fetch_kline
    df = fetch_kline("AAPL", "1h", period="5d", prepost=True)
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import pandas as pd

import config  # noqa: E402

# Yahoo chart API 官方端点（经反代访问）
# 反代格式：{PROXY_BASE}/{原始URL}
_ORIGIN = "https://query1.finance.yahoo.com/v8/finance/chart/"
_PROXY_BASE = config.YAHOO_CHART_PROXY  # 如 https://img2.365200.xyz


def _build_url(symbol: str, interval: str, params: dict[str, Any]) -> str:
    """构造反代后的完整 URL。"""
    raw = _ORIGIN + urllib.parse.quote(symbol) + "?" + urllib.parse.urlencode(params)
    return f"{_PROXY_BASE.rstrip('/')}/{raw}"


def _request(url: str, timeout: int = 30) -> dict:
    """发起 GET 请求并解析 JSON；失败抛出异常。"""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        # 反代可能把 Yahoo 的错误透传；附上状态码便于排查
        raise RuntimeError(f"chart API HTTP {exc.code}: {url}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"chart API 网络错误: {exc.reason}") from exc


def fetch_kline(
    symbol: str,
    interval: str = "1h",
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
    prepost: bool = True,
    retries: int | None = None,
    delay: float | None = None,
) -> pd.DataFrame:
    """拉取单只股票 K 线，返回列与 yfinance 一致的 DataFrame（含 Adj Close/Volume）。

    参数与 yfinance.history 对齐：
      - period: 如 "1d"/"5d"/"1mo"/"3mo"/"6mo"/"1y"/"2y"/"5y"/"max"
      - start/end: YYYY-MM-DD 或 YYYY-MM-DD HH:MM，与 period 互斥
      - prepost: 是否包含盘前/盘后延长时段（美股）
    返回索引为 UTC 时间戳（DatetimeIndex），列: Open/High/Low/Close/Adj Close/Volume。
    失败重试后仍失败则抛出异常。
    """
    params: dict[str, Any] = {"interval": interval, "includePrePost": str(prepost).lower()}
    if period:
        params["range"] = period
    else:
        # Yahoo chart API 要求 period1 与 period2 同时提供（只传 period1 返回 400）
        if start:
            params["period1"] = int(pd.Timestamp(start).timestamp())
        else:
            # 默认起始：6 个月（与 yfinance 1h 的 period 对齐，留足数据）
            params["period1"] = int(pd.Timestamp.now().timestamp()) - 6 * 30 * 86400
        if end:
            params["period2"] = int(pd.Timestamp(end).timestamp())
        else:
            params["period2"] = int(pd.Timestamp.now().timestamp())

    retries = config.MAX_RETRIES if retries is None else retries
    delay = config.REQUEST_DELAY if delay is None else delay

    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            url = _build_url(symbol, interval, params)
            data = _request(url)
            return _parse_chart(data, symbol, interval)
        except Exception as exc:  # noqa: BLE001 - 重试瞬时错误
            last_exc = exc
            if attempt < retries - 1:
                wait = delay * (2**attempt)
                print(f"    重试 {attempt+1}/{retries-1}（等 {wait:.0f}s）：{exc}", flush=True)
                time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return pd.DataFrame()


def _parse_chart(data: dict, symbol: str, interval: str) -> pd.DataFrame:
    """将 chart API 返回的 JSON 解析为标准 OHLCV DataFrame。"""
    result = (data.get("chart") or {}).get("result")
    if not result:
        err = (data.get("chart") or {}).get("error")
        raise RuntimeError(f"{symbol} chart API 无数据: {err}")

    res = result[0]
    meta = res.get("meta", {})
    ts = res.get("timestamp") or []
    quote = (res.get("indicators") or {}).get("quote") or []
    adjclose = ((res.get("indicators") or {}).get("adjclose") or [{}])[0]
    if not ts or not quote:
        # 无数据：返回空 DataFrame（与 yfinance 行为一致）
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"])

    q = quote[0]
    rows = []
    for i, t in enumerate(ts):
        rows.append(
            {
                "Open": q.get("open", [])[i],
                "High": q.get("high", [])[i],
                "Low": q.get("low", [])[i],
                "Close": q.get("close", [])[i],
                "Adj Close": (adjclose.get("adjclose") or [None] * len(ts))[i],
                "Volume": q.get("volume", [])[i],
            }
        )

    df = pd.DataFrame(rows, index=pd.to_datetime(ts, unit="s", utc=True))
    # 转成与 yfinance 一致的列类型
    df = df.astype({"Open": "float64", "High": "float64", "Low": "float64", "Close": "float64"})
    df["Adj Close"] = pd.to_numeric(df["Adj Close"], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")
    # 去掉时区，与现有入库文件一致（本地文件为 naive）
    df.index = df.index.tz_localize(None)
    if interval == "1d":
        # 日K归一化到纯日期（去掉雅虎附带的盘中时间戳）
        df.index = df.index.normalize()
        df.index.name = "Date"
    else:
        df.index.name = "Datetime"
    df = df.dropna(subset=["Close"])
    return df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
