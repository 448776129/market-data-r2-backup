"""纯 Python 技术指标计算引擎（无 pandas/numpy 依赖）。

与 scripts/indicators.py 计算口径完全一致（已逐项与 pandas_ta 对齐）：
  - MA : 简单移动平均
  - EMA: pandas_ta 风格（presma=True: SMA 种子 + adjust=False 递归）
  - MACD: ema12/ema26 + signal=ema9(macd from first_valid)
  - RSI: Wilder 平滑（ewm adjust=False, alpha=1/period）
  - KDJ: pd_rma 平滑（ewm adjust=True, alpha=1/signal）
  - 布林带: rolling std ddof=1
  - Volume MA / 涨跌幅

输入/输出均为 list[float]（与输入等长，不足周期处为 float('nan')）。
"""

from __future__ import annotations

import math
from typing import Callable

NAN = float("nan")


def _is_nan(v: float) -> bool:
    return isinstance(v, float) and math.isnan(v)


def _last_valid(arr: list[float], idx: int = -1) -> float | None:
    """从 idx 往回找最后一个非 NaN 值。"""
    i = idx if idx >= 0 else len(arr) - 1
    for j in range(i, -1, -1):
        v = arr[j]
        if not _is_nan(v):
            return v
    return None


def _to_list(s) -> list[float]:
    if isinstance(s, list):
        return [float(x) for x in s]
    return [float(x) for x in s]


# ── 均线 ────────────────────────────────────────────────────────

def ma(series, period: int = 5) -> list[float]:
    """简单移动平均。前 period-1 个为 NaN。"""
    s = _to_list(series)
    n = len(s)
    out = [NAN] * n
    if n < period:
        return out
    window_sum = sum(s[0:period])
    for i in range(period - 1, n):
        if i > period - 1:
            window_sum += s[i] - s[i - period]
        out[i] = window_sum / period
    return out


def ema(series, period: int = 12) -> list[float]:
    """pandas_ta 风格 EMA（presma=True）：
    1) 第 period-1 个位置用 SMA 作种子
    2) 之后 ewm(span=period, adjust=False) 递归:
       alpha = 2/(period+1); ema[i] = (1-alpha)*prev + alpha*val
    """
    s = _to_list(series)
    n = len(s)
    out = [NAN] * n
    if n < period:
        return out
    seed = sum(s[0:period]) / period
    out[period - 1] = seed
    alpha = 2.0 / (period + 1)
    for i in range(period, n):
        out[i] = (1 - alpha) * out[i - 1] + alpha * s[i]
    return out


# ── MACD ────────────────────────────────────────────────────────

def macd(series, fast: int = 12, slow: int = 26, signal: int = 9) -> dict[str, list[float]]:
    s = _to_list(series)
    n = len(s)
    base = [NAN] * n
    if n < slow:
        return {"macd": base, "signal": base.copy(), "histogram": base.copy()}

    ema_fast = ema(s, fast)
    ema_slow = ema(s, slow)
    macd_line = [a - b if not (_is_nan(a) or _is_nan(b)) else NAN for a, b in zip(ema_fast, ema_slow)]

    # signal: 从第一个有效 index 截断后 ema9
    first_valid = next((i for i, v in enumerate(macd_line) if not _is_nan(v)), None)
    if first_valid is None:
        return {"macd": macd_line, "signal": base.copy(), "histogram": base.copy()}

    sub = macd_line[first_valid:]
    sig_sub = ema(sub, signal)  # presma 种子 = 前 9 个 macd 均值
    signal_line = [NAN] * first_valid + sig_sub

    histogram = [
        a - b if not (_is_nan(a) or _is_nan(b)) else NAN
        for a, b in zip(macd_line, signal_line)
    ]
    return {"macd": macd_line, "signal": signal_line, "histogram": histogram}


# ── RSI ─────────────────────────────────────────────────────────

def rsi(series, period: int = 14) -> list[float]:
    """pandas_ta 风格（mamode="rma"）：
    rma = ewm(alpha=1/period, adjust=False)
    RSI = 100 * avg_gain / (avg_gain + avg_loss)
    """
    s = _to_list(series)
    n = len(s)
    out = [NAN] * n
    if n < period + 1:
        return out

    # 计算涨跌幅（第 0 个为 NaN）
    gains = [NAN] * n
    losses = [NAN] * n
    for i in range(1, n):
        diff = s[i] - s[i - 1]
        gains[i] = max(diff, 0.0)
        losses[i] = max(-diff, 0.0)

    alpha = 1.0 / period
    avg_gain = _ewm_adjust_false(gains, alpha)
    avg_loss = _ewm_adjust_false(losses, alpha)

    for i in range(n):
        ag, al = avg_gain[i], avg_loss[i]
        if _is_nan(ag) or _is_nan(al):
            out[i] = NAN
        else:
            denom = ag + al
            out[i] = 100.0 * ag / denom if denom > 0 else 100.0
    return out


def _ewm_adjust_false(values: list[float], alpha: float) -> list[float]:
    """pandas ewm(alpha, adjust=False) 等价。
    从第一个非 NaN 值开始递归: out[i] = (1-a)*out[i-1] + a*val[i]
    NaN 处输出 NaN，但内部状态保留（与 pandas ignore_na=False 一致）。
    """
    n = len(values)
    out = [NAN] * n
    first = next((i for i, v in enumerate(values) if not _is_nan(v)), None)
    if first is None:
        return out
    out[first] = values[first]
    state = values[first]
    for i in range(first + 1, n):
        v = values[i]
        if _is_nan(v):
            out[i] = NAN
        else:
            state = (1 - alpha) * state + alpha * v
            out[i] = state
    return out


def _ewm_adjust_true(values: list[float], alpha: float, min_periods: int = 1) -> list[float]:
    """pandas ewm(alpha, min_periods=n).mean() 等价（adjust=True）。
    num[i] = x[i] + (1-a)*num[i-1]; den[i] = 1 + (1-a)*den[i-1]; out = num/den
    前 min_periods-1 个有效观察输出 NaN。
    """
    n = len(values)
    out = [NAN] * n
    first = next((i for i, v in enumerate(values) if not _is_nan(v)), None)
    if first is None:
        return out

    num, den = 0.0, 0.0
    valid_count = 0
    for i in range(n):
        v = values[i]
        if _is_nan(v):
            continue
        num = v + (1 - alpha) * num
        den = 1 + (1 - alpha) * den
        valid_count += 1
        if valid_count >= min_periods:
            out[i] = num / den
    return out


# ── KDJ ─────────────────────────────────────────────────────────

def kdj(high, low, close, k_period: int = 9, d_period: int = 3) -> dict[str, list[float]]:
    """pandas_ta 风格：RSV → pd_rma（ewm adjust=True, alpha=1/signal）平滑 K/D"""
    h = _to_list(high)
    l = _to_list(low)
    c = _to_list(close)
    n = len(c)
    base = [NAN] * n
    if n < k_period:
        return {"k": base, "d": base.copy(), "j": base.copy()}

    # RSV
    rsv = [NAN] * n
    for i in range(k_period - 1, n):
        hh = max(h[i - k_period + 1: i + 1])
        ll = min(l[i - k_period + 1: i + 1])
        rng = hh - ll
        rsv[i] = 100.0 * (c[i] - ll) / rng if rng > 0 else NAN

    alpha = 1.0 / d_period
    k_vals = _ewm_adjust_true(rsv, alpha, min_periods=d_period)
    d_vals = _ewm_adjust_true(k_vals, alpha, min_periods=d_period)
    j_vals = [3 * k - 2 * d if not (_is_nan(k) or _is_nan(d)) else NAN
              for k, d in zip(k_vals, d_vals)]
    return {"k": k_vals, "d": d_vals, "j": j_vals}


# ── 布林带 ──────────────────────────────────────────────────────

def bollinger(series, period: int = 20, std_dev: float = 2.0) -> dict[str, list[float]]:
    """布林带（std ddof=1，与 pandas_ta 默认一致）。"""
    s = _to_list(series)
    n = len(s)
    base = [NAN] * n
    if n < period:
        return {"upper": base, "middle": base.copy(), "lower": base.copy()}

    upper, middle, lower = [NAN] * n, [NAN] * n, [NAN] * n
    for i in range(period - 1, n):
        window = s[i - period + 1: i + 1]
        mean = sum(window) / period
        var = sum((x - mean) ** 2 for x in window) / (period - 1)  # ddof=1
        std = math.sqrt(var)
        middle[i] = mean
        upper[i] = mean + std_dev * std
        lower[i] = mean - std_dev * std
    return {"upper": upper, "middle": middle, "lower": lower}


# ── 成交量均线 ──────────────────────────────────────────────────

def volume_ma(volume, period: int = 20) -> list[float]:
    return ma(volume, period)


def price_change(series, periods: int = 1) -> list[float]:
    """涨跌幅（百分比）。"""
    s = _to_list(series)
    n = len(s)
    out = [NAN] * n
    for i in range(periods, n):
        prev = s[i - periods]
        out[i] = (s[i] - prev) / prev * 100.0 if prev != 0 else NAN
    return out


# ── 全套指标 ────────────────────────────────────────────────────

def compute_all(close, high, low, volume) -> dict:
    """计算全套指标。

    Args:
        close, high, low, volume: list[float]

    Returns:
        {ma5, ma10, ma20, ma60, ema12, ema26,
         macd{...}, rsi14, kdj{...}, bollinger{...},
         volume_ma5, volume_ma20, change_1d, change_5d, change_20d}
    """
    return {
        "ma5": ma(close, 5),
        "ma10": ma(close, 10),
        "ma20": ma(close, 20),
        "ma60": ma(close, 60),
        "ema12": ema(close, 12),
        "ema26": ema(close, 26),
        "macd": macd(close),
        "rsi14": rsi(close, 14),
        "kdj": kdj(high, low, close),
        "bollinger": bollinger(close),
        "volume_ma5": volume_ma(volume, 5),
        "volume_ma20": volume_ma(volume, 20),
        "change_1d": price_change(close, 1),
        "change_5d": price_change(close, 5),
        "change_20d": price_change(close, 20),
    }