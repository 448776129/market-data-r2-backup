"""指数成分股清单拉取脚本。

从公开数据源拉取用户配置的指数成分股，写入 universe 目录：
    data/universe/{index}.csv

数据源来自 yfiua/index-constituents，符号与 Yahoo Finance 完全一致，
可直接用于 yfinance 拉取K线。

支持的指数（config.INDEX_SOURCES）：
  - csi300:  沪深300（A股）
  - csi500:  中证500（A股）
  - nasdaq100: 纳指100（美股）
  - sp500:    标普500（美股）
  - hsi:      恒生指数（港股）

用法：
    python scripts/fetch_universe.py            # 拉取全部指数
    python scripts/fetch_universe.py --index csi300  # 仅指定指数
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import kvstore  # noqa: E402
import r2store  # noqa: E402


def download(url: str) -> str:
    """下载文件并解码为文本。"""
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_index_csv(text: str) -> list[str]:
    """解析成分股 CSV（Symbol,Name 两列），返回 Symbol 列表。"""
    reader = csv.DictReader(io.StringIO(text))
    symbols: list[str] = []
    for row in reader:
        sym = (row.get("Symbol") or "").strip()
        if sym:
            symbols.append(sym)
    return symbols


def fetch_index(index: str) -> list[str]:
    """拉取指定指数成分股列表。"""
    url = config.INDEX_SOURCES.get(index)
    if not url:
        raise ValueError(f"未知指数数据源: {index}")
    text = download(url)
    return parse_index_csv(text)


def run(index: str | None) -> int:
    indices = [index] if index else list(config.INDEX_SOURCES)
    failed = False
    for idx in indices:
        try:
            print(f"下载 {idx} 成分股...", flush=True)
            symbols = fetch_index(idx)
            if not symbols:
                print(f"  未解析出任何成分股，跳过 {idx}", file=sys.stderr)
                failed = True
                continue
            cfg = config.INDEX_CONFIG.get(idx, {})
            fname = cfg.get("file", f"{idx}.csv")
            out = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / fname
            out.parent.mkdir(parents=True, exist_ok=True)
            rows = sorted(set(symbols))
            csv_text = "\n".join(rows) + "\n"
            out.write_text(csv_text, encoding="utf-8")
            # 上传到 R2 + KV（Worker 优先读 KV，毫秒级、不耗 R2 读额度）
            r2store.put_universe(idx, csv_text)
            kvstore.put_universe(idx, csv_text)
            print(f"  共 {len(rows)} 只 -> {out.relative_to(ROOT)} (R2+KV)", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [失败] {idx}: {exc}", file=sys.stderr)
            failed = True
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="拉取指数成分股清单")
    parser.add_argument(
        "--index",
        choices=sorted(config.INDEX_SOURCES),
        help="仅拉取指定指数（默认全部）",
    )
    args = parser.parse_args()
    return run(args.index)


if __name__ == "__main__":
    sys.exit(main())