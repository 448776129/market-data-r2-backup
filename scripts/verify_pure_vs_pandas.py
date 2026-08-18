"""验证 indicators_pure.py（纯 Python）与 indicators.py（pandas 版）结果一致。

用 AAPL 真实 R2 数据同时跑两个版本，逐项对比 max_diff。
pandas 版已通过 pandas_ta 交叉验证（35/35），因此 pure 版一致即正确。

用法（需 R2 凭据 + pandas）：
    python verify_pure_vs_pandas.py
"""
import os, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import pandas as pd
import numpy as np
import r2store
import indicators as ind_pd        # pandas 版（已与 pandas_ta 对齐）
import indicators_pure as ind_pure # 纯 Python 版

def load_close_ohlcv(region, symbol, interval):
    key = f"{region}/{r2store_map(interval)}/{symbol}.csv"
    text = r2store.get_csv_text(key)
    if text is None:
        return None
    if text.startswith("\ufeff"):
        text = text[1:]
    df = pd.read_csv(io_string(text))
    return df

def r2store_map(interval):
    from config import KLINE_SUBDIR, INTRADAY_M1_SUBDIR, INTRADAY_M1H_SUBDIR
    return {
        "1d": KLINE_SUBDIR,
        "1m": INTRADAY_M1_SUBDIR,
        "1h": INTRADAY_M1H_SUBDIR,
    }[interval]

import io
def io_string(text):
    return io.StringIO(text)

def compare(name, arr_pd, arr_pure):
    """对比两个数组，输出共同有效范围内的 max_diff。"""
    a = np.asarray(arr_pd, dtype=float)
    b = np.asarray(arr_pure, dtype=float)
    mask = ~np.isnan(a) & ~np.isnan(b)
    if mask.sum() == 0:
        print(f"  ⚠️ {name}: 无共同有效值")
        return None
    diff = np.max(np.abs(a[mask] - b[mask]))
    return diff

def main():
    print("=" * 60)
    print("  indicators_pure.py vs indicators.py 验证")
    print("=" * 60)

    tests = [
        ("us", "AAPL", "1d"),
        ("us", "MSFT", "1d"),
        ("cn", "600519.SS", "1d"),
        ("us", "NVDA", "1d"),
    ]

    all_ok = True
    for region, symbol, interval in tests:
        df = load_close_ohlcv(region, symbol, interval)
        if df is None:
            print(f"\n--- {symbol}: R2 无数据，跳过 ---")
            continue
        close = df["Close"].tolist()
        high = df["High"].tolist()
        low = df["Low"].tolist()
        volume = df["Volume"].tolist()
        print(f"\n--- {symbol} ({len(close)} 行) ---")

        r_pd = ind_pd.compute_all(df)
        r_pure = ind_pure.compute_all(close, high, low, volume)

        checks = [
            ("ma5", r_pd["ma5"], r_pure["ma5"]),
            ("ma10", r_pd["ma10"], r_pure["ma10"]),
            ("ma20", r_pd["ma20"], r_pure["ma20"]),
            ("ma60", r_pd["ma60"], r_pure["ma60"]),
            ("ema12", r_pd["ema12"], r_pure["ema12"]),
            ("ema26", r_pd["ema26"], r_pure["ema26"]),
            ("macd.macd", r_pd["macd"]["macd"], r_pure["macd"]["macd"]),
            ("macd.signal", r_pd["macd"]["signal"], r_pure["macd"]["signal"]),
            ("macd.hist", r_pd["macd"]["histogram"], r_pure["macd"]["histogram"]),
            ("rsi14", r_pd["rsi14"], r_pure["rsi14"]),
            ("kdj.k", r_pd["kdj"]["k"], r_pure["kdj"]["k"]),
            ("kdj.d", r_pd["kdj"]["d"], r_pure["kdj"]["d"]),
            ("kdj.j", r_pd["kdj"]["j"], r_pure["kdj"]["j"]),
            ("bb.upper", r_pd["bollinger"]["upper"], r_pure["bollinger"]["upper"]),
            ("bb.middle", r_pd["bollinger"]["middle"], r_pure["bollinger"]["middle"]),
            ("bb.lower", r_pd["bollinger"]["lower"], r_pure["bollinger"]["lower"]),
            ("vol_ma5", r_pd["volume_ma5"], r_pure["volume_ma5"]),
            ("vol_ma20", r_pd["volume_ma20"], r_pure["volume_ma20"]),
            ("chg_1d", r_pd["change_1d"], r_pure["change_1d"]),
            ("chg_5d", r_pd["change_5d"], r_pure["change_5d"]),
            ("chg_20d", r_pd["change_20d"], r_pure["change_20d"]),
        ]

        ok_cnt = 0
        for name, a, b in checks:
            diff = compare(name, a, b)
            if diff is None:
                continue
            # 价格类指标容差 1e-6（两者应完全一致），除非是极大数值（如 MSTR 高价股）
            tol = max(1e-6, abs(np.nanmax(np.abs(np.asarray(a, dtype=float)))) * 1e-9)
            if diff <= tol:
                ok_cnt += 1
            else:
                all_ok = False
                print(f"  ❌ {name}: max_diff={diff:.6e} (tol={tol:.2e})")
        print(f"  ✅ {ok_cnt}/{len(checks)} 项一致（未列出的项为无共同有效值）")

    print("\n" + "=" * 60)
    print("  " + ("✅ 全部一致：纯 Python 版正确" if all_ok else "❌ 存在不一致"))
    print("=" * 60)
    return 0 if all_ok else 1

if __name__ == "__main__":
    sys.exit(main())