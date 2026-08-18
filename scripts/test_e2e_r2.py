"""端到端测试：从 R2 读真实 K 线 → indicators 计算 → 验证。

需要 R2 凭据环境变量：
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
"""
import os
import sys
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import pandas as pd
import numpy as np
import r2store
import indicators as ind
import screener_precompute as sp

# 测试用股票
TEST_CASES = [
    ("us", "AAPL", "1d"),
    ("us", "MSFT", "1d"),
    ("hk", "0700.HK", "1d"),
    ("cn", "600519.SS", "1d"),
]

def main():
    print("=" * 60)
    print("  端到端测试：R2 真实数据 → 指标计算 → 快照验证")
    print("=" * 60)

    all_ok = True
    for region, symbol, interval in TEST_CASES:
        print(f"\n--- {region}/{symbol} ({interval}) ---")

        # 1. 从 R2 读取
        df = sp.load_kline_from_r2(region, symbol, interval)
        if df is None:
            print(f"  ⚠️ R2 中无数据，跳过（可能尚未采集）")
            continue

        print(f"  R2 读取成功: {len(df)} 行, 列={list(df.columns)}")
        print(f"  日期范围: {df.iloc[0, 0]} → {df.iloc[-1, 0]}")

        # 2. 检查数据完整性
        required_cols = ["Open", "High", "Low", "Close", "Volume"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"  ❌ 缺少列: {missing}")
            all_ok = False
            continue

        # 3. 计算 indicators
        try:
            result = ind.compute_all(df)
        except Exception as e:
            print(f"  ❌ compute_all 失败: {e}")
            all_ok = False
            continue

        # 4. 验证每个指标数组长度 = DataFrame 行数
        n = len(df)
        len_ok = True
        for key, val in result.items():
            if isinstance(val, dict):
                for sub_key, sub_val in val.items():
                    if len(sub_val) != n:
                        print(f"  ❌ {key}.{sub_key} 长度={len(sub_val)} ≠ {n}")
                        len_ok = False
            else:
                if len(val) != n:
                    print(f"  ❌ {key} 长度={len(val)} ≠ {n}")
                    len_ok = False
        if len_ok:
            print(f"  ✅ 所有指标数组长度 = {n}")

        # 5. compute_snapshot 提取最新行
        snap = sp.compute_snapshot(df)
        if snap is None:
            print(f"  ❌ compute_snapshot 返回 None")
            all_ok = False
            continue

        # 6. 验证快照字段完整
        expected_fields = [
            "close", "change_1d", "change_5d", "change_20d",
            "ma5", "ma10", "ma20", "ma60",
            "ema12", "ema26",
            "macd", "macd_signal", "macd_histogram",
            "rsi14", "kdj_k", "kdj_d", "kdj_j",
            "bb_upper", "bb_middle", "bb_lower",
            "volume", "volume_ma5", "volume_ma20",
            "updated",
        ]
        missing_fields = [f for f in expected_fields if f not in snap]
        if missing_fields:
            print(f"  ❌ 快照缺少字段: {missing_fields}")
            all_ok = False
        else:
            print(f"  ✅ 快照字段完整 ({len(expected_fields)} 字段)")

        # 7. 打印关键指标值供人工核对
        print(f"  📊 {symbol} 快照:")
        print(f"     close={snap['close']:.2f} change_1d={snap['change_1d']:.2f}%")
        print(f"     ma5={snap['ma5']:.2f} ma20={snap['ma20']:.2f} ma60={snap['ma60']:.2f}")
        print(f"     rsi14={snap['rsi14']:.2f}")
        print(f"     macd={snap['macd']:.4f} signal={snap['macd_signal']:.4f}")
        print(f"     kdj_k={snap['kdj_k']:.2f} kdj_j={snap['kdj_j']:.2f}")

        # 8. 验证数值合理性（非 NaN，非极端值）
        for field in ["close", "ma5", "rsi14"]:
            val = snap[field]
            if val is None or (isinstance(val, float) and np.isnan(val)):
                print(f"  ❌ {field} = {val}（不应为 None/NaN）")
                all_ok = False
            elif field == "rsi14" and (val < 0 or val > 100):
                print(f"  ❌ rsi14={val} 超出 [0,100] 范围")
                all_ok = False

    print(f"\n{'=' * 60}")
    if all_ok:
        print("  ✅ 全部通过：R2 数据读取 + 指标计算 + 快照生成")
    else:
        print("  ❌ 有失败项，请检查上方输出")
    print(f"{'=' * 60}")
    return 0 if all_ok else 1

if __name__ == "__main__":
    sys.exit(main())
