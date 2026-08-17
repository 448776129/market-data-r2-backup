"""构建 universe 股票清单（写入 data/universe/）。

从用户提供的本地持仓/列表文件生成各区域的全市场清单：
    - us: IWB_holdings.csv（iShares Russell 1000 持仓，取 Equity 类 Ticker）
    - cn: GPLIST.xls（沪市）+ A股列表.xlsx（深市），合并为沪深全市场
    - hk: 恒生指数成分股（从 yfiua 拉取，见 fetch_universe.py）
    - kr: KOSPI 200 前 50 核心成分股（内置 KR_CODES，已人工核对代码）

用法：
    python scripts/build_universe.py --us <IWB.csv> --cn-sh <GPLIST.xls> \
        --cn-sz <A股列表.xlsx>
韩股清单用 `python scripts/build_universe.py --kr`（使用内置 KR_CODES）。
生成的清单与 Yahoo 符号约定一致（美股裸代码、A股 .SS/.SZ、韩股 .KS、港股 .HK）。

依赖：pandas + openpyxl + xlrd（一次性运行，转换后清单提交到仓库即可）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402


def write_universe(region: str, symbols: list[str]) -> Path:
    """将符号列表去重排序后写入 data/universe/{region}.csv。"""
    out = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / f"{region}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(set(symbols))
    out.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"  [{region}] 共 {len(rows)} 只 -> {out.relative_to(ROOT)}", flush=True)
    return out


def build_us(path: str) -> list[str]:
    """从 IWB_holdings.csv 提取 Equity 类 Ticker。"""
    df = pd.read_csv(path, skiprows=9)
    # 仅保留股票（剔除 Futures/Money Market/Cash 等）
    df = df[df.get("Asset Class", "Equity") == "Equity"]
    tickers = df["Ticker"].astype(str).str.strip()
    return [t for t in tickers if t and t != "nan"]


def build_cn(sh_path: str, sz_path: str) -> list[str]:
    """沪市(GPLIST)加 .SS，深市(A股列表)加 .SZ，合并全市场。"""
    # 沪市：GPLIST.xls 的 A股代码列，6 位纯数字
    sh = pd.read_excel(sh_path)
    sh_codes = sh["A股代码"].astype(str).str.strip()
    sh_syms = [c.zfill(6) + ".SS" for c in sh_codes if c and c != "nan"]

    # 深市：A股列表.xlsx 的 A股代码列，int 类型需补前导零到 6 位
    sz = pd.read_excel(sz_path)
    sz_codes = sz["A股代码"].astype(str).str.strip()
    sz_syms = [c.zfill(6) + ".SZ" for c in sz_codes if c and c != "nan"]

    print(f"  沪市 {len(sh_syms)} 只, 深市 {len(sz_syms)} 只", flush=True)
    return sh_syms + sz_syms


# KOSPI 200 前 50 核心成分股（6 位代码，已逐只验证 Yahoo 可解析）
# 修正记录：
#   - 095400 -> 009540（HD Korea Shipbuilding & Offshore）
#   - 000830 -> 028260（Samsung C&T 现用代码，旧代码已退市）
KR_CODES: list[str] = [
    # 1. 信息技术与半导体
    "005930", "000660", "066570", "011070", "009150", "042700",
    # 2. 电池与二次电池
    "373220", "006400", "051910", "003670", "066970",
    # 3. 汽车与交通装备
    "005380", "000270", "012330", "086280", "161890",
    # 4. 造船、国防与重工
    "329180", "009540", "012450", "042660", "010140", "034020",
    # 5. 生物医药与医疗
    "207940", "068270", "326030", "000100",
    # 6. 钢铁、资源与化学
    "005490", "010130", "010950", "096770",
    # 7. 金融、保险与控股
    "105560", "055550", "086790", "316140", "028260", "003550", "034730",
    # 8. 互联网、科技与娱乐
    "035420", "035720", "352820", "259960", "036570",
    # 9. 通信、能源与消费
    "017670", "030200", "015760", "033780", "090430", "271560",
]


def build_kr() -> list[str]:
    """返回 KOSPI 200 前 50 核心成分股（加 .KS 后缀）。"""
    return [c + ".KS" for c in KR_CODES]


def main() -> int:
    parser = argparse.ArgumentParser(description="构建 universe 股票清单")
    parser.add_argument("--us", help="IWB_holdings.csv 路径")
    parser.add_argument("--cn-sh", help="GPLIST.xls（沪市）路径")
    parser.add_argument("--cn-sz", help="A股列表.xlsx（深市）路径")
    parser.add_argument(
        "--kr",
        action="store_true",
        help="写入内置的 KOSPI 200 前 50 核心成分股清单",
    )
    args = parser.parse_args()

    if args.us:
        write_universe("us", build_us(args.us))
    if args.cn_sh and args.cn_sz:
        write_universe("cn", build_cn(args.cn_sh, args.cn_sz))
    if args.kr:
        write_universe("kr", build_kr())

    print("提示: 港股清单请用 python scripts/fetch_universe.py --index hsi 拉取", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
