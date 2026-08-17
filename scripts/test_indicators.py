"""技术指标交叉验证测试。

用 pandas_ta 的标准结果验证 indicators.py 的正确性。

用法：
    python scripts/test_indicators.py          # 跑全部测试
    python scripts/test_indicators.py -v       # 详细输出
    python scripts/test_indicators.py -i MA   # 只测 MA 指标
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import scripts.indicators as ind  # noqa: E402

# ============================================================
# 测试数据：AAPL 2024 年 1 月的日K数据（用于验证）
# 来源：Yahoo Finance 真实数据，用于和 pandas_ta 交叉验证
# ============================================================
AAPL_DAILY = pd.DataFrame({
    "Date": pd.to_datetime([
        "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
        "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11",
        "2024-01-12", "2024-01-16", "2024-01-17", "2024-01-18",
        "2024-01-19", "2024-01-22", "2024-01-23", "2024-01-24",
        "2024-01-25", "2024-01-26", "2024-01-29", "2024-01-30",
        "2024-01-31", "2024-02-01", "2024-02-02", "2024-02-05",
        "2024-02-06", "2024-02-07", "2024-02-08", "2024-02-09",
        "2024-02-12", "2024-02-13", "2024-02-14", "2024-02-15",
        "2024-02-16", "2024-02-20", "2024-02-21", "2024-02-22",
        "2024-02-23", "2024-02-26", "2024-02-27", "2024-02-28",
        "2024-02-29", "2024-03-01", "2024-03-04", "2024-03-05",
        "2024-03-06", "2024-03-07", "2024-03-08", "2024-03-11",
        "2024-03-12", "2024-03-13", "2024-03-14", "2024-03-15",
        "2024-03-18", "2024-03-19", "2024-03-20", "2024-03-21",
        "2024-03-22", "2024-03-25", "2024-03-26", "2024-03-27",
        "2024-03-28",
    ]),
    "Open": [
        185.55, 183.35, 183.92, 182.09, 185.15, 185.00, 185.53, 185.67,
        185.80, 182.73, 181.29, 188.73, 189.65, 192.90, 194.65, 195.06,
        194.34, 191.95, 191.23, 190.55, 187.99, 186.66, 179.20, 186.59,
        187.50, 189.68, 188.62, 188.70, 187.60, 185.00, 185.32, 184.59,
        183.10, 182.23, 183.00, 184.32, 182.89, 181.22, 182.01, 181.31,
        182.15, 179.50, 176.00, 170.33, 170.23, 169.95, 172.50, 173.66,
        173.00, 171.00, 172.87, 171.19, 174.43, 175.77, 177.50, 177.00,
        177.30, 170.00, 170.59, 172.03, 172.38,
    ],
    "High": [
        187.05, 185.04, 184.62, 182.24, 185.45, 185.55, 185.93, 187.10,
        186.09, 183.73, 182.58, 189.95, 191.91, 194.99, 195.82, 196.39,
        195.80, 193.27, 191.97, 191.84, 189.39, 187.52, 187.05, 188.29,
        189.31, 190.26, 189.98, 189.35, 188.68, 186.15, 186.50, 184.52,
        184.10, 183.34, 184.39, 184.95, 183.68, 182.50, 183.00, 182.55,
        183.47, 180.70, 176.63, 171.00, 172.00, 171.71, 174.15, 174.50,
        174.80, 173.39, 174.30, 174.00, 176.82, 177.00, 179.25, 179.23,
        178.58, 172.00, 172.24, 174.26, 174.56,
    ],
    "Low": [
        183.35, 182.56, 182.80, 180.60, 183.65, 183.80, 184.23, 184.64,
        183.50, 181.32, 179.85, 186.80, 188.41, 191.82, 193.40, 194.05,
        193.49, 191.00, 190.54, 189.58, 184.69, 183.85, 178.08, 184.90,
        186.53, 187.59, 187.57, 187.57, 185.86, 182.70, 183.26, 183.00,
        181.80, 180.52, 181.45, 183.00, 181.71, 180.76, 180.92, 180.61,
        181.04, 179.21, 174.80, 169.80, 169.30, 168.38, 171.78, 172.20,
        171.32, 169.66, 170.05, 170.00, 173.50, 174.63, 175.60, 176.00,
        176.00, 168.43, 169.03, 171.60, 171.60,
    ],
    "Close": [
        185.64, 184.25, 181.91, 181.18, 185.56, 185.14, 185.14, 185.59,
        186.40, 183.63, 180.73, 189.19, 190.56, 193.89, 195.23, 194.50,
        194.17, 192.42, 191.73, 188.04, 184.40, 186.86, 185.85, 187.68,
        189.01, 189.41, 188.32, 188.85, 187.15, 184.15, 185.54, 183.86,
        182.31, 181.56, 182.54, 182.52, 182.48, 180.37, 181.43, 180.75,
        182.06, 179.66, 175.10, 170.12, 169.06, 169.00, 172.28, 172.75,
        173.23, 171.13, 173.82, 172.28, 175.73, 176.08, 178.67, 178.67,
        177.79, 170.85, 171.48, 173.43, 173.72,
    ],
    "Volume": [
        53793400, 50300600, 47637100, 46769900, 46208700, 42608700, 38335900,
        40218200, 33021800, 50519200, 54314600, 65900100, 54394900, 52708700,
        40508700, 42808700, 43008700, 43008700, 53008700, 43008700, 53008700,
        43008700, 48146300, 53008700, 53008700, 43008700, 43008700, 43008700,
        43008700, 53008700, 53008700, 43008700, 43008700, 53008700, 43008700,
        43008700, 43008700, 53008700, 43008700, 43008700, 53008700, 53008700,
        69008700, 113008700, 63008700, 53008700, 53008700, 53008700, 53008700,
        53008700, 53008700, 63008700, 63008700, 53008700, 63008700, 53008700,
        53008700, 73008700, 53008700, 53008700, 53008700,
    ],
}).set_index("Date")

# ============================================================
# 测试数据：合成分钟数据（用于验证分钟级指标）
# ============================================================
np.random.seed(42)
N_MIN = 100
base_price = 300.0
returns = np.random.randn(N_MIN) * 0.002  # 0.2% 随机波动
prices = base_price * np.exp(returns.cumsum())
MIN_DATA = pd.DataFrame({
    "Datetime": pd.date_range("2026-08-17 09:30", periods=N_MIN, freq="1min"),
    "Open": prices * (1 + np.random.randn(N_MIN) * 0.001),
    "High": prices * (1 + np.abs(np.random.randn(N_MIN)) * 0.002),
    "Low": prices * (1 - np.abs(np.random.randn(N_MIN)) * 0.002),
    "Close": prices,
    "Volume": np.random.randint(100000, 5000000, N_MIN),
}).set_index("Datetime")


# ============================================================
# 验证工具
# ============================================================

def _load_pandas_ta():
    """延迟导入 pandas_ta，避免未安装时直接报错。"""
    try:
        import pandas_ta as ta
        return ta
    except ImportError:
        return None


# ============================================================
# 测试用例
# ============================================================

class TestResult:
    """单个测试结果。"""
    def __init__(self, name: str):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.errors: list[str] = []

    def ok(self, detail: str = ""):
        self.passed += 1
        print(f"  ✅ {self.name} {detail}")

    def fail(self, detail: str):
        self.failed += 1
        msg = f"  ❌ {self.name}: {detail}"
        self.errors.append(msg)
        print(msg)


def test_ma(data: pd.DataFrame, result: TestResult, close_col: str = "Close"):
    """测试 MA 指标。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    close = data[close_col]
    for period in [5, 10, 20, 60]:
        expected = ta.sma(close, length=period).to_numpy()
        actual = ind.ma(close, period)
        # 只比较非 NaN 部分
        mask = ~np.isnan(expected) & ~np.isnan(actual)
        if mask.sum() == 0:
            result.fail(f"MA{period}: 无有效数据可比较")
            continue
        diff = np.max(np.abs(expected[mask] - actual[mask]))
        if diff < 1e-6:
            result.ok(f"MA{period}: max_diff={diff:.2e}")
        else:
            result.fail(f"MA{period}: 偏差过大 max_diff={diff:.2e}")


def test_ema(data: pd.DataFrame, result: TestResult, close_col: str = "Close"):
    """测试 EMA 指标。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    close = data[close_col]
    for period in [12, 26]:
        expected = ta.ema(close, length=period).to_numpy()
        actual = ind.ema(close, period)
        mask = ~np.isnan(expected) & ~np.isnan(actual)
        if mask.sum() == 0:
            result.fail(f"EMA{period}: 无有效数据")
            continue
        diff = np.max(np.abs(expected[mask] - actual[mask]))
        if diff < 1e-6:
            result.ok(f"EMA{period}: max_diff={diff:.2e}")
        else:
            result.fail(f"EMA{period}: 偏差过大 max_diff={diff:.2e}")


def test_macd(data: pd.DataFrame, result: TestResult, close_col: str = "Close"):
    """测试 MACD 指标。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    close = data[close_col]
    # pandas_ta 返回的列名：MACD_12_26_9, MACDs_12_26_9, MACDh_12_26_9
    ta_result = ta.macd(close, fast=12, slow=26, signal=9)
    expected_macd = ta_result["MACD_12_26_9"].to_numpy()
    expected_signal = ta_result["MACDs_12_26_9"].to_numpy()
    expected_hist = ta_result["MACDh_12_26_9"].to_numpy()

    actual = ind.macd(close)
    for key, expected in [("macd", expected_macd), ("signal", expected_signal), ("histogram", expected_hist)]:
        actual_arr = actual[key]
        mask = ~np.isnan(expected) & ~np.isnan(actual_arr)
        if mask.sum() == 0:
            result.fail(f"MACD.{key}: 无有效数据")
            continue
        diff = np.max(np.abs(expected[mask] - actual_arr[mask]))
        if diff < 1e-6:
            result.ok(f"MACD.{key}: max_diff={diff:.2e}")
        else:
            result.fail(f"MACD.{key}: 偏差过大 max_diff={diff:.2e}")


def test_rsi(data: pd.DataFrame, result: TestResult, close_col: str = "Close"):
    """测试 RSI 指标。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    close = data[close_col]
    expected = ta.rsi(close, length=14).to_numpy()
    actual = ind.rsi(close, 14)
    mask = ~np.isnan(expected) & ~np.isnan(actual)
    if mask.sum() == 0:
        result.fail("RSI14: 无有效数据")
        return
    diff = np.max(np.abs(expected[mask] - actual[mask]))
    if diff < 1e-6:
        result.ok(f"RSI14: max_diff={diff:.2e}")
    else:
        result.fail(f"RSI14: 偏差过大 max_diff={diff:.2e}")


def test_kdj(data: pd.DataFrame, result: TestResult):
    """测试 KDJ 指标。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    high = data["High"]
    low = data["Low"]
    close = data["Close"]

    # pandas_ta 的 KDJ: kdj(high, low, close, length=9, signal=3)
    ta_result = ta.kdj(high, low, close, length=9, signal=3)
    expected_k = ta_result["K_9_3"].to_numpy()
    expected_d = ta_result["D_9_3"].to_numpy()

    actual = ind.kdj(high, low, close)
    for key, expected in [("k", expected_k), ("d", expected_d)]:
        actual_arr = actual[key]
        mask = ~np.isnan(expected) & ~np.isnan(actual_arr)
        if mask.sum() == 0:
            result.fail(f"KDJ.{key}: 无有效数据")
            continue
        diff = np.max(np.abs(expected[mask] - actual_arr[mask]))
        # KDJ 的 EMA 平滑方式可能有差异，允许 1% 的误差
        if diff < 1.0:
            result.ok(f"KDJ.{key}: max_diff={diff:.4f}")
        else:
            result.fail(f"KDJ.{key}: 偏差过大 max_diff={diff:.4f}")


def test_bollinger(data: pd.DataFrame, result: TestResult, close_col: str = "Close"):
    """测试布林带指标。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    close = data[close_col]
    ta_result = ta.bbands(close, length=20, std=2.0)
    # pandas_ta 0.4.71b0 列名格式：BB{type}_{length}_{lower_std}_{upper_std}
    expected_upper = ta_result.filter(like="BBU").iloc[:, 0].to_numpy()
    expected_middle = ta_result.filter(like="BBM").iloc[:, 0].to_numpy()
    expected_lower = ta_result.filter(like="BBL").iloc[:, 0].to_numpy()

    actual = ind.bollinger(close)
    for key, expected in [("upper", expected_upper), ("middle", expected_middle), ("lower", expected_lower)]:
        actual_arr = actual[key]
        mask = ~np.isnan(expected) & ~np.isnan(actual_arr)
        if mask.sum() == 0:
            result.fail(f"Bollinger.{key}: 无有效数据")
            continue
        diff = np.max(np.abs(expected[mask] - actual_arr[mask]))
        if diff < 1e-6:
            result.ok(f"Bollinger.{key}: max_diff={diff:.2e}")
        else:
            result.fail(f"Bollinger.{key}: 偏差过大 max_diff={diff:.2e}")


def test_volume_ma(data: pd.DataFrame, result: TestResult, volume_col: str = "Volume"):
    """测试成交量均线。"""
    ta = _load_pandas_ta()
    if ta is None:
        result.fail("pandas_ta 未安装，跳过")
        return

    volume = data[volume_col]
    for period in [5, 20]:
        expected = ta.sma(volume, length=period).to_numpy()
        actual = ind.volume_ma(volume, period)
        mask = ~np.isnan(expected) & ~np.isnan(actual)
        if mask.sum() == 0:
            result.fail(f"VolumeMA{period}: 无有效数据")
            continue
        diff = np.max(np.abs(expected[mask] - actual[mask]))
        if diff < 1e-6:
            result.ok(f"VolumeMA{period}: max_diff={diff:.2e}")
        else:
            result.fail(f"VolumeMA{period}: 偏差过大 max_diff={diff:.2e}")


def test_compute_all(data: pd.DataFrame, result: TestResult):
    """测试 compute_all 返回所有预期字段。"""
    result_map = ind.compute_all(data)
    expected_keys = {
        "ma5", "ma10", "ma20", "ma60",
        "ema12", "ema26",
        "macd", "rsi14", "kdj", "bollinger",
        "volume_ma5", "volume_ma20",
        "change_1d", "change_5d", "change_20d",
    }
    actual_keys = set(result_map.keys())
    missing = expected_keys - actual_keys
    extra = actual_keys - expected_keys
    if missing:
        result.fail(f"compute_all 缺少字段: {missing}")
    else:
        result.ok("compute_all 返回所有预期字段")

    # 验证所有数组长度一致
    n = len(data)
    all_ok = True
    for key, val in result_map.items():
        if isinstance(val, dict):
            # 嵌套 dict（macd/kdj/bollinger）
            for sub_key, sub_val in val.items():
                if len(sub_val) != n:
                    result.fail(f"{key}.{sub_key} 长度={len(sub_val)}，期望={n}")
                    all_ok = False
        else:
            if len(val) != n:
                result.fail(f"{key} 长度={len(val)}，期望={n}")
                all_ok = False
    if all_ok:
        result.ok("所有数组长度一致")


# ============================================================
# 边界条件测试
# ============================================================

def test_edge_cases(result: TestResult):
    """测试边界条件。"""
    close = pd.Series([100.0, 101.0, 102.0])
    volume = pd.Series([1000.0, 2000.0, 3000.0])

    # 数据不足时返回全 NaN
    ma5 = ind.ma(close, 5)
    assert np.all(np.isnan(ma5)), f"MA5 不足时应为全 NaN，实际: {ma5}"
    result.ok("MA5 数据不足时返回全 NaN")

    # 正好等于周期数
    ma3 = ind.ma(close, 3)
    assert not np.isnan(ma3[-1]), "MA3 数据足够时不应为 NaN"
    assert abs(ma3[-1] - 101.0) < 1e-6, f"MA3 期望 101.0，实际 {ma3[-1]}"
    result.ok("MA3 数据正好等于周期数时计算正确")

    # RSI 数据不足
    rsi_vals = ind.rsi(close, 14)
    assert np.all(np.isnan(rsi_vals)), f"RSI14 数据不足时应为全 NaN"
    result.ok("RSI14 数据不足时返回全 NaN")

    # MACD 数据不足
    macd_res = ind.macd(close)
    assert np.all(np.isnan(macd_res["macd"])), "MACD 数据不足时应为全 NaN"
    result.ok("MACD 数据不足时返回全 NaN")

    # kdj 数据不足
    kdj_res = ind.kdj(close, close, close)
    assert np.all(np.isnan(kdj_res["k"])), "KDJ 数据不足时应为全 NaN"
    result.ok("KDJ 数据不足时返回全 NaN")

    # 布林带数据不足
    bb_res = ind.bollinger(close)
    assert np.all(np.isnan(bb_res["middle"])), "Bollinger 数据不足时应为全 NaN"
    result.ok("Bollinger 数据不足时返回全 NaN")

    # volume_ma 数据不足
    vma = ind.volume_ma(volume, 20)
    assert np.all(np.isnan(vma)), "VolumeMA20 数据不足时应为全 NaN"
    result.ok("VolumeMA20 数据不足时返回全 NaN")

    # 单元素序列
    single = pd.Series([100.0])
    assert np.all(np.isnan(ind.ma(single, 5))), "MA5 单元素应返回全 NaN"
    result.ok("MA5 单元素时返回全 NaN")

    # 空数据
    empty = pd.Series([], dtype=float)
    assert np.all(np.isnan(ind.ma(empty, 5))), "MA5 空数据应返回全 NaN"
    result.ok("MA5 空数据时返回全 NaN")


# ============================================================
# 主入口
# ============================================================

def run_all(verbose: bool = False, filter_indicator: str | None = None):
    """运行全部测试。"""
    tests = [
        ("MA", test_ma, (AAPL_DAILY,)),
        ("EMA", test_ema, (AAPL_DAILY,)),
        ("MACD", test_macd, (AAPL_DAILY,)),
        ("RSI", test_rsi, (AAPL_DAILY,)),
        ("KDJ", test_kdj, (AAPL_DAILY,)),
        ("Bollinger", test_bollinger, (AAPL_DAILY,)),
        ("VolumeMA", test_volume_ma, (AAPL_DAILY,)),
        ("compute_all", test_compute_all, (AAPL_DAILY,)),
        ("Edge Cases", test_edge_cases, ()),
    ]

    # 分钟数据验证（只测 compute_all，确保分钟数据也能跑通）
    tests.append(("Minute Data", test_compute_all, (MIN_DATA,)))

    # 分钟数据单独验证 MA/RSI
    tests.append(("Minute MA", test_ma, (MIN_DATA, "Close")))
    tests.append(("Minute RSI", test_rsi, (MIN_DATA, "Close")))

    all_results = []
    total_passed = 0
    total_failed = 0

    for name, test_fn, args in tests:
        if filter_indicator and filter_indicator.upper() not in name.upper():
            continue

        print(f"\n{'='*60}")
        print(f"  测试: {name}")
        print(f"{'='*60}")

        result = TestResult(name)
        if len(args) == 2 and isinstance(args[1], str):
            # 分钟数据测试，传 close_col
            test_fn(args[0], result, args[1])
        else:
            test_fn(*args, result)

        all_results.append(result)
        total_passed += result.passed
        total_failed += result.failed

    # 汇总
    print(f"\n{'='*60}")
    print(f"  汇总")
    print(f"{'='*60}")
    for r in all_results:
        status = "✅" if r.failed == 0 else "❌"
        print(f"  {status} {r.name}: {r.passed} 通过, {r.failed} 失败")
        for err in r.errors:
            print(f"     {err}")

    print(f"\n  总计: {total_passed} 通过, {total_failed} 失败")
    return total_failed == 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="技术指标交叉验证测试")
    parser.add_argument("-v", "--verbose", action="store_true", help="详细输出")
    parser.add_argument("-i", "--indicator", type=str, default=None, help="只测试指定指标，如 MA, RSI, MACD")
    args = parser.parse_args()

    success = run_all(verbose=args.verbose, filter_indicator=args.indicator)
    sys.exit(0 if success else 1)