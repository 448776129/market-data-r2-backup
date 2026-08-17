"""股票区域与符号配置。

按区域划分市场数据，每个区域对应 data/ 下的一个子目录。
新增或调整股票时只需修改本文件。
"""

from __future__ import annotations

# 区域 -> 符号列表
# 符号需符合 yfinance 的 ticker 格式：
#   - 美股: 如 AAPL、MSFT、TSLA
#   - 港股: 如 0700.HK、9988.HK
#   - A股:  如 600519.SS、000001.SZ
#   - 韩股: 如 005930.KS（三星电子）、000660.KS（SK海力士）
#
# 若某区域的符号列表留空（[]），则该市场被当作"全市场"模式：
# 从 universe 文件（data/universe/{region}.csv）读取全部股票代码。
#
# 当前股票范围（2026-08 由用户指定，经 scripts/build_universe.py 生成本地清单）：
#   - us: iShares Russell 1000 ETF 持仓（IWB，标普1000，1022 只 Equity）
#   - cn: 沪深 A 股全市场（沪市 GPLIST.xls + 深市 A股列表.xlsx，4595 只）
#   - hk: 恒生指数全部成分股（hsi.csv，88 只）
#   - kr: KOSPI 200 前 50 核心成分股（build_universe.KR_CODES，48 只）
#   - etf: 美股 ETF 集合（用户指定 355 只，data/universe/etf.csv）
#   - cn_etf: 中国（A股）ETF 集合（用户指定 211 只，data/universe/cn_etf.csv）
REGIONS: dict[str, list[str]] = {
    "us": [],  # 全市场模式：从 data/universe/us.csv 读取全部美股
    "hk": [],  # 全市场模式：从 data/universe/hk.csv 读取全部港股
    "cn": [],  # 全市场模式：从 data/universe/cn.csv 读取全部A股(沪+深)
    "kr": [],  # 全市场模式：从 data/universe/kr.csv 读取全部韩股
    "etf": [],  # 美股 ETF：从 data/universe/etf.csv 读取
    "cn_etf": [],  # 中国 A股 ETF：从 data/universe/cn_etf.csv 读取
}

# 历史数据拉取范围（yfinance period 参数）
HISTORY_PERIOD = "5y"
# K线周期（yfinance interval 参数）
INTERVAL = "1d"

# 数据根目录
DATA_DIR = "data"
# K线数据子目录（每只股票一个文件）
KLINE_SUBDIR = "kline"
# 非K线数据（快照/财务/分析师等）子目录（每只股票一个文件）
META_SUBDIR = "meta"
# 全市场股票列表子目录；文件名 = {region}.csv（如 us.csv、cn.csv）
UNIVERSE_SUBDIR = "universe"
# 各区域全市场列表文件名
UNIVERSE_FILES = {
    "us": "us.csv",
    "cn": "cn.csv",
    "hk": "hk.csv",
    "kr": "kr.csv",
    "etf": "etf.csv",
    "cn_etf": "cn_etf.csv",
}

# 请求间隔（秒）：控制对 Yahoo 的请求频率，避免触发限流导致 429/404
REQUEST_DELAY = 2
# 单只股票请求的最大重试次数（遇瞬时网络/限流错误时指数退避重试）
MAX_RETRIES = 3

# 增量同步运行间隔（分钟）：用于判断分钟K数据是否足够新鲜。
# 若某股票已有数据的最后时间点距今小于该值，说明本轮运行前刚同步过，
# 跳过请求（避免每半小时重复拉取同一批分钟K）。
INCREMENTAL_MIN_INTERVAL_MINUTES = 30

# 全市场股票列表的数据源（供 fetch_universe.py 使用）
# 注意：当前各区域清单由 scripts/build_universe.py 从本地文件生成并提交到仓库，
# 不再依赖在线全市场清单源。此项保留仅作参考。
# us: 每行一个美股代码
# hk: 港股代码清单（code 列），需加 .HK
# kr: KRX 缓存（动态日期），Code 列加 .KS
UNIVERSE_SOURCES = {
    "us": "https://raw.githubusercontent.com/abbadata/stock-tickers/main/data/allsymbols.txt",
    "hk": "https://raw.githubusercontent.com/darr/stock_code/master/hk_stock_code.csv",
}

# 指数成分股清单（按用户配置拉取，替代全市场）
# index 名 -> (清单文件名, 所属区域)
# 清单文件位于 data/universe/ 下，由 fetch_universe.py 从数据源更新
INDEX_CONFIG: dict[str, dict] = {
    "csi300": {"file": "csi300.csv", "region": "cn"},   # 沪深300（A股）
    "csi500": {"file": "csi500.csv", "region": "cn"},   # 中证500（A股）
    "ndx100": {"file": "nasdaq100.csv", "region": "us"},  # 纳指100
    "sp500": {"file": "sp500.csv", "region": "us"},       # 标普500
    "hsi": {"file": "hsi.csv", "region": "hk"},           # 恒生指数
}

# 指数成分股清单数据源（供 fetch_universe.py 使用）
# 来自 yfiua/index-constituents，符号与 Yahoo Finance 完全一致
INDEX_SOURCES = {
    "csi300": "https://yfiua.github.io/index-constituents/constituents-csi300.csv",
    "csi500": "https://yfiua.github.io/index-constituents/constituents-csi500.csv",
    "nasdaq100": "https://yfiua.github.io/index-constituents/constituents-nasdaq100.csv",
    "sp500": "https://yfiua.github.io/index-constituents/constituents-sp500.csv",
    "hsi": "https://yfiua.github.io/index-constituents/constituents-hsi.csv",
}

# 拉取的范围：默认按 INDEX_CONFIG 拉取指数成分股（用户配置）
# 关闭全市场模式：REGIONS 全部保持空即可，由 --index 指定指数
# 分钟级K线子目录（按周期分目录，每只股票一个文件）
INTRADAY_M1_SUBDIR = "kline_1m"
INTRADAY_M5_SUBDIR = "kline_5m"
INTRADAY_M15_SUBDIR = "kline_15m"
INTRADAY_M30_SUBDIR = "kline_30m"
INTRADAY_M1H_SUBDIR = "kline_1h"
# 分钟级K线各周期的 yfinance period（1m 仅保留约5~7天，1h 约730天）
INTRADAY_PERIOD = {"1m": "5d", "1h": "6mo"}
# 由 1m 重采样计算得到的周期：5m/15m/30m（雅虎不提供这些历史周期，代码计算）。
# 1h 由雅虎原生提供（含 15:30~16:00 收盘bar），不在此派生。
# 映射：衍生周期 -> pandas 重采样规则
INTRADAY_DERIVED = {
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
}
# 分钟级K线增量拉取时的回看缓冲天数：覆盖数据修订（除权/分红/错误修正）
INTRADAY_BUFFER_DAYS = 2

# Yahoo chart API 反代入口（用户自建 Cloudflare Worker）
# 国内直连 Yahoo 会被 403，通过反代转发 /v8/finance/chart/ 请求。
# 格式：反代根地址，内部拼 {YAHOO_CHART_PROXY}/{原始chart URL}
# 反代支持 includePrePost=true，美股 1m/5m/15m/30m/60m 均含盘前盘后延长时段
YAHOO_CHART_PROXY = "https://img2.365200.xyz"

# ---------------------------------------------------------------
# 增量查重：减少无效请求
# 定时任务每 1 小时运行一次，但市场休市（非交易时段）时不可能产生新K线。
# 通过判断"当前是否处于交易时段"与"当日K线是否已入库"，
# 在发起 Yahoo 请求前直接跳过，大幅减少 Actions 运行时间与请求量。
# ---------------------------------------------------------------
# 各区域市场时区（IANA 名称）
REGION_TZ = {
    "cn": "Asia/Shanghai",
    "hk": "Asia/Hong_Kong",
    "kr": "Asia/Seoul",
    "us": "America/New_York",
    "etf": "America/New_York",  # 美股 ETF 在美股市场交易
    "cn_etf": "Asia/Shanghai",  # 中国 ETF 在 A股市场交易
}
# 各区域可能产生新K线的时段（该市场本地时间，分钟），可多段：
#  - cn: 9:15-11:30, 13:00-15:05（含集合竞价，剔除午休）
#  - hk: 9:15-12:00, 13:00-16:10（剔除午休）
#  - kr: 9:00-15:30
#  - us: 4:00-20:00（含盘前盘后延长时段）
MARKET_SESSIONS: dict[str, tuple[tuple[int, int], ...]] = {
    "cn": ((9 * 60 + 15, 11 * 60 + 30), (13 * 60, 15 * 60 + 5)),
    "hk": ((9 * 60 + 15, 12 * 60), (13 * 60, 16 * 60 + 10)),
    "kr": ((9 * 60, 15 * 60 + 30),),
    "us": ((4 * 60, 20 * 60),),
    "etf": ((4 * 60, 20 * 60),),  # 与美股相同
    "cn_etf": ((9 * 60 + 15, 11 * 60 + 30), (13 * 60, 15 * 60 + 5)),  # 与 A股相同
}