"""跨运行持久化的采集状态清单（manifest）。

用于替代「每次运行都去读每只股票的完整 R2 文件来判断是否变化」的做法，
从而把 R2 的读写次数从「按股票数 × 运行次数」降为「几乎只在真正变化时触碰」。

每种采集类别各存一个清单，对象键：
    _state/{category}/{region}_b{batch}.json

每个 (region, batch) 只会被对应的那个 job 读写，天然无并发冲突。

清单内容（每只股票一个条目）：
    kline: {symbol: {"1d": "2026-08-14", "1m": "<ISO ts>", "1h": "<ISO ts>"}}  # 各周期最后时间
    meta : {symbol: "<md5hex>"}             # 已入库 meta 内容的指纹，变了才写
    news : {symbol: {"h": "<md5hex>"}}      # 最近一次新闻 url 集合的指纹，新增才追加写

读不到 / 解析失败时返回空 dict，调用方按「该状态未知」处理（等价于增量全量，自愈）。
"""

from __future__ import annotations

import json

import r2store  # noqa: F401 - 复用 R2 读写（S3 Credentials 从环境变量读取）


def _key(category: str, region: str, batch: int = 0) -> str:
    return f"_state/{category}/{region}_b{batch}.json"


def read(category: str, region: str, batch: int = 0) -> dict:
    raw = r2store.get_bytes(_key(category, region, batch))
    if raw is None:
        return {}
    try:
        data = json.loads(raw.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001 - 状态损坏按空处理，下次自愈
        return {}


def write(category: str, region: str, batch: int, data: dict) -> None:
    r2store.put_bytes(
        _key(category, region, batch),
        json.dumps(data, ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
    )