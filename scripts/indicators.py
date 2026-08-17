"""技术指标计算引擎（纯计算，无IO依赖）。

所有函数接收 pandas.Series 或 numpy.ndarray，返回 numpy.ndarray（与输入等长，
不足周期的位置填充 NaN）。

支持的指标：
  - MA / EMA / MACD / RSI / KDJ / Bollinger Bands / Volume MA
"""

from __future__ import annotations

import numpy as np
import pandas as pd


# ── 工具函数 ────────────────────────────────────────────────────

def _to_series(s: pd.Series | np.ndarray | list) -> pd.Series:
    """确保输入为 pd.Series。"""
    if isinstance(s, pd.Series):
        return s
    return pd.Series(np.asarray(s, dtype=float))


def _nan_like(s: pd.Series) -> np.ndarray:
    """返回与输入等长的全 NaN 数组。"""
    return np.full(len(s), np.nan)


def _too_short(close: pd.Series, min_periods: int) -> bool:
    return len(close) < min_periods


# ── 均线 ────────────────────────────────────────────────────────

def ma(close: pd.Series | np.ndarray | list, period: int = 5) -> np.ndarray:
    """简单移动平均线 (SMA)。

    Args:
        close: 收盘价序列
        period: 周期，默认 5

    Returns:
        np.ndarray，前 period-1 个位置为 NaN
    """
    close = _to_series(close)
    if _too_short(close, period):
        return _nan_like(close)
    return close.rolling(window=period, min_periods=period).mean().to_numpy()


def ema(close: pd.Series | np.ndarray | list, period: int = 12) -> np.ndarray:
    """指数移动平均线 (EMA)。

    TA Lib / pandas_ta 风格（presma=True）：
      1) 第 period-1 个位置用 SMA 作为种子
      2) 之后 ewm(span=period, adjust=False) 递推

    Args:
        close: 收盘价序列
        period: 周期，默认 12

    Returns:
        np.ndarray
    """
    close = _to_series(close)
    if _too_short(close, period):
        return _nan_like(close)

    c = close.copy()
    sma_nth = c.iloc[0:period].mean()
    c.iloc[:period - 1] = np.nan
    c.iloc[period - 1] = sma_nth
    return c.ewm(span=period, adjust=False).mean().to_numpy()


# ── MACD ────────────────────────────────────────────────────────

def macd(
    close: pd.Series | np.ndarray | list,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> dict[str, np.ndarray]:
    """MACD 指标。

    Args:
        close: 收盘价序列
        fast: 快线周期，默认 12
        slow: 慢线周期，默认 26
        signal: 信号线周期，默认 9

    Returns:
        {"macd": np.ndarray, "signal": np.ndarray, "histogram": np.ndarray}
        macd = EMA(fast) - EMA(slow)
        signal = EMA(macd, signal)
        histogram = macd - signal
    """
    close = _to_series(close)
    if _too_short(close, slow):
        n = len(close)
        base = _nan_like(close)
        return {"macd": base, "signal": base.copy(), "histogram": base.copy()}

    ema_fast = pd.Series(ema(close, fast), index=close.index)
    ema_slow = pd.Series(ema(close, slow), index=close.index)
    macd_line = ema_fast - ema_slow

    # signal 线：先在第一个有效 index 处截断，再走 presma 风格 EMA（与 pandas_ta 一致）
    first_valid = macd_line.first_valid_index()
    if first_valid is None:
        base = _nan_like(close)
        return {"macd": base, "signal": base.copy(), "histogram": base.copy()}
    macd_fvi = macd_line.loc[first_valid:]
    signal_slice = pd.Series(ema(macd_fvi, signal), index=macd_fvi.index)
    signal_line = pd.Series(np.nan, index=close.index)
    signal_line.loc[first_valid:] = signal_slice

    histogram = macd_line - signal_line

    return {
        "macd": macd_line.to_numpy(),
        "signal": signal_line.to_numpy(),
        "histogram": histogram.to_numpy(),
    }


# ── RSI ─────────────────────────────────────────────────────────

def rsi(
    close: pd.Series | np.ndarray | list,
    period: int = 14,
) -> np.ndarray:
    """相对强弱指标 (RSI)。

    pandas_ta 风格（mamode="rma"）：
      positive_avg = rma(gain, period)   # rma = ewm(alpha=1/period, min_periods=period)
      negative_avg = rma(loss, period)
      RSI = 100 * positive_avg / (positive_avg + negative_avg)

    该公式在正/负平均全为 0 时安全返回（无除零）。

    Args:
        close: 收盘价序列
        period: 周期，默认 14

    Returns:
        np.ndarray
    """
    close = _to_series(close)
    if _too_short(close, period + 1):
        return _nan_like(close)

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    # rma（Wilder 平滑）：close.ewm(alpha=1/period, adjust=False)，与 pandas_ta.overlap.rma 一致
    alpha = 1.0 / period
    avg_gain = gain.ewm(alpha=alpha, adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False).mean()

    rsi_values = 100 * avg_gain / (avg_gain + avg_loss)
    return rsi_values.to_numpy()


# ── KDJ ─────────────────────────────────────────────────────────

def kdj(
    high: pd.Series | np.ndarray | list,
    low: pd.Series | np.ndarray | list,
    close: pd.Series | np.ndarray | list,
    k_period: int = 9,
    d_period: int = 3,
    j_period: int = 3,
) -> dict[str, np.ndarray]:
    """KDJ 随机指标。

    K = 2/3 * K_prev + 1/3 * RSV
    D = 2/3 * D_prev + 1/3 * K
    J = 3 * K - 2 * D

    RSV = (close - low_N) / (high_N - low_N) * 100
    其中 low_N 为 N 日内最低价，high_N 为 N 日内最高价

    Args:
        high: 最高价序列
        low: 最低价序列
        close: 收盘价序列
        k_period: K 周期，默认 9
        d_period: D 周期，默认 3
        j_period: J 周期，默认 3

    Returns:
        {"k": np.ndarray, "d": np.ndarray, "j": np.ndarray}
    """
    high = _to_series(high)
    low = _to_series(low)
    close = _to_series(close)
    if _too_short(close, k_period):
        base = _nan_like(close)
        return {"k": base, "d": base.copy(), "j": base.copy()}

    h_n = high.rolling(window=k_period, min_periods=k_period).max()
    l_n = low.rolling(window=k_period, min_periods=k_period).min()

    # RSV（pandas_ta 用 non_zero_range 避免除零；此处分母为 0 时置 NaN）
    rng = (h_n - l_n).replace(0, np.nan)
    rsv = 100 * (close - l_n) / rng

    # K/D 用 pd_rma 平滑（ewm(alpha=1/signal, min_periods=signal, adjust=True)），与 pandas_ta 一致
    alpha_k = 1.0 / d_period
    k_values = rsv.ewm(alpha=alpha_k, min_periods=d_period).mean().to_numpy()
    d_values = pd.Series(k_values, index=close.index).ewm(
        alpha=alpha_k, min_periods=d_period
    ).mean().to_numpy()
    j_values = 3 * k_values - 2 * d_values

    return {"k": k_values, "d": d_values, "j": j_values}


# ── 布林带 ──────────────────────────────────────────────────────

def bollinger(
    close: pd.Series | np.ndarray | list,
    period: int = 20,
    std_dev: float = 2.0,
) -> dict[str, np.ndarray]:
    """布林带 (Bollinger Bands)。

    Args:
        close: 收盘价序列
        period: 周期，默认 20
        std_dev: 标准差倍数，默认 2.0

    Returns:
        {"upper": np.ndarray, "middle": np.ndarray, "lower": np.ndarray}
    """
    close = _to_series(close)
    if _too_short(close, period):
        base = _nan_like(close)
        return {"upper": base, "middle": base.copy(), "lower": base.copy()}

    middle = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std(ddof=1)  # pandas_ta 默认 ddof=1
    upper = middle + std_dev * std
    lower = middle - std_dev * std

    return {
        "upper": upper.to_numpy(),
        "middle": middle.to_numpy(),
        "lower": lower.to_numpy(),
    }


# ── 成交量均线 ──────────────────────────────────────────────────

def volume_ma(
    volume: pd.Series | np.ndarray | list,
    period: int = 20,
) -> np.ndarray:
    """成交量移动平均。

    Args:
        volume: 成交量序列
        period: 周期，默认 20

    Returns:
        np.ndarray
    """
    volume = _to_series(volume)
    if _too_short(volume, period):
        return _nan_like(volume)
    return volume.rolling(window=period, min_periods=period).mean().to_numpy()


# ── 涨跌幅 ──────────────────────────────────────────────────────

def price_change(
    close: pd.Series | np.ndarray | list,
    periods: int = 1,
) -> np.ndarray:
    """涨跌幅（百分比）。

    Args:
        close: 收盘价序列
        periods: 间隔期数，1=日涨跌幅，5=周涨跌幅，20=月涨跌幅

    Returns:
        np.ndarray
    """
    close = _to_series(close)
    if _too_short(close, periods + 1):
        return _nan_like(close)
    return close.pct_change(periods=periods).to_numpy() * 100


# ── 综合选股指标计算 ────────────────────────────────────────────

def compute_all(
    df: pd.DataFrame,
    *,
    close_col: str = "Close",
    high_col: str = "High",
    low_col: str = "Low",
    volume_col: str = "Volume",
) -> dict[str, np.ndarray | dict]:
    """计算全套技术指标，返回 dict。

    Args:
        df: 包含 OHLCV 数据的 DataFrame
        close_col: 收盘价列名
        high_col: 最高价列名
        low_col: 最低价列名
        volume_col: 成交量列名

    Returns:
        {
            "ma5": np.ndarray,
            "ma10": np.ndarray,
            "ma20": np.ndarray,
            "ma60": np.ndarray,
            "ema12": np.ndarray,
            "ema26": np.ndarray,
            "macd": {"macd": ..., "signal": ..., "histogram": ...},
            "rsi14": np.ndarray,
            "kdj": {"k": ..., "d": ..., "j": ...},
            "bollinger": {"upper": ..., "middle": ..., "lower": ...},
            "volume_ma5": np.ndarray,
            "volume_ma20": np.ndarray,
            "change_1d": np.ndarray,
            "change_5d": np.ndarray,
            "change_20d": np.ndarray,
        }
    """
    close = df[close_col]
    high = df[high_col]
    low = df[low_col]
    volume = df[volume_col]

    result = {
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
    return result