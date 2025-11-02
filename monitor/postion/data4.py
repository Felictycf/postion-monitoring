# -*- coding: utf-8 -*-
"""
Fast Binance UM Perpetual - Open Interest Anomaly Scanner (loop, save to SQLite)
- 周期：5m/15m/30m/1h/4h
- 每隔 2.5 分钟扫描一次
- 每次扫描保存到 SQLite 数据库
"""

from binance.um_futures import UMFutures
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any, Tuple
from zoneinfo import ZoneInfo
from datetime import datetime
import math
import os
import time
import sqlite3

# ================= 可配置 =================
# 周期与历史长度（足够做滚动统计，不要太大）
PERIODS = {
    "5m":   192,   # 约16小时
    "15m":  160,
    "30m":  160,
    "1h":   160,
    "4h":   160,
}

# z-score 统计用窗口
ROLLING_WIN = {
    "5m":   96,
    "15m":  80,
    "30m":  80,
    "1h":   80,
    "4h":   80,
}

# 异动等级（仅 OI）：满足同级全部条件视为该级别（越前越严）
# level, min_z, min_pct, min_abs_usd
ANOMALY_THRESHOLDS = [
    ("Critical", 5.0, 0.10, 5_000_000),
    ("Major",    4.0, 0.07, 1_500_000),
    ("Moderate", 3.0, 0.04,   500_000),
    ("Minor",    2.0, 0.02,   100_000),
]

# 仅 USDT 计价；如需全部，设为 None
QUOTE_WHITELIST = {"USDT"}

# 扫描 24h 成交额 Top N（None=不筛选）
TOP_N = 300

# 并发线程数（根据带宽/机器调节，注意交易所限频）
MAX_WORKERS = 24

# 每个周期输出 TopN
PRINT_TOP_N_PER_PERIOD = 12

# 轮询间隔（秒）= 2.5 分钟
POLL_INTERVAL_SEC = 400

# SQLite 数据库路径
DB_PATH = "./oi_alerts.db"
# ========================================

# 初始化 UMFutures（兼容旧版库：不传 session）
um = UMFutures()  # 公共行情无需 Key

def get_um_perp_symbols() -> List[str]:
    info = um.exchange_info()
    syms, whitelist = [], QUOTE_WHITELIST
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            if (whitelist is None) or (s.get("quoteAsset") in whitelist):
                syms.append(s.get("symbol"))
    return sorted(syms)

def get_top_symbols_by_quote_volume(candidates: List[str], top_n: Optional[int]) -> List[str]:
    if top_n is None:
        return candidates
    tickers = um.ticker_24hr_price_change()  # list
    rows = []
    cset = set(candidates)
    for t in tickers:
        sym = t.get("symbol")
        if sym in cset:
            qv = float(t.get("quoteVolume", 0.0))
            rows.append((sym, qv))
    rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in rows[:top_n]]

def fetch_oi_hist(symbol: str, period: str, limit: int) -> pd.DataFrame:
    data = um.open_interest_hist(symbol=symbol, period=period, limit=limit)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    for col in ["sumOpenInterest", "sumOpenInterestValue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df[["timestamp","sumOpenInterest","sumOpenInterestValue"]].sort_values("timestamp").reset_index(drop=True)
    return df

def fetch_last_two_closes(symbol: str, period: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    返回 (last_close, prev_close, pct_change)
    pct_change = (last - prev) / prev
    """
    kl = um.klines(symbol=symbol, interval=period, limit=2)
    if not kl or len(kl) < 2:
        # 尝试退化到1根（仅返回最新价）
        if kl and len(kl) == 1:
            last_close = float(kl[0][4])
            return last_close, None, None
        return None, None, None
    prev_close = float(kl[0][4])
    last_close = float(kl[1][4])
    pct = None if prev_close == 0 else (last_close - prev_close) / prev_close
    return last_close, prev_close, pct

def classify_level(z_abs: float, pct: float, abs_usd: float) -> Optional[str]:
    def _is_nan(v):
        return (v is None) or (isinstance(v, float) and math.isnan(v))
    if _is_nan(z_abs) or _is_nan(pct) or _is_nan(abs_usd):
        return None
    for level, min_z, min_pct, min_abs in ANOMALY_THRESHOLDS:
        if z_abs >= min_z and pct >= min_pct and abs_usd >= min_abs:
            return level
    return None

def process_one(symbol: str, period: str, limit: int, win: int) -> Optional[Dict[str, Any]]:
    """
    仅抓 OI，算出是否异动；若异动，再补拉该周期的最新两根收盘价以计算价格变化幅度。
    返回告警字典；无告警返回 None
    """
    df = fetch_oi_hist(symbol, period, limit)
    if df.empty or len(df) < max(40, win + 5):
        return None

    # 差分与百分比
    df["dOI"] = df["sumOpenInterest"].diff()
    df["dOIValue"] = df["sumOpenInterestValue"].diff()
    base_val = df["sumOpenInterestValue"].shift(1).replace(0, np.nan)
    df["oi_value_pct"] = df["dOIValue"].abs() / base_val

    # z-score 基于 |ΔOI名义|
    roll = df["dOIValue"].abs().rolling(win, min_periods=max(20, win//2))
    mean_abs = roll.mean()
    std_abs = roll.std(ddof=0)
    df["z_abs"] = (df["dOIValue"].abs() - mean_abs) / std_abs

    last = df.iloc[-1]
    z_abs   = float(last["z_abs"]) if pd.notna(last["z_abs"]) else None
    pct     = float(last["oi_value_pct"]) if pd.notna(last["oi_value_pct"]) else None
    abs_usd = float(abs(last["dOIValue"])) if pd.notna(last["dOIValue"]) else None
    level = classify_level(z_abs, pct, abs_usd)
    if not level:
        return None

    # 仅触发时补拉价格，并计算价格变化幅度
    last_close, prev_close, px_pct = fetch_last_two_closes(symbol, period)
    direction = "↑" if (last["dOIValue"] > 0) else ("↓" if last["dOIValue"] < 0 else "=")

    return {
        "timestamp": last["timestamp"],  # UTC tz-aware
        "symbol": symbol,
        "period": period,
        "level": level,
        "direction": direction,
        "dOI": float(last["dOI"]) if pd.notna(last["dOI"]) else np.nan,
        "dOIValue": float(last["dOIValue"]) if pd.notna(last["dOIValue"]) else np.nan,
        "oi_value_pct": float(pct) if pct is not None else np.nan,
        "z_abs": float(z_abs) if z_abs is not None else np.nan,
        "price": float(last_close) if last_close is not None else np.nan,
        "price_pct": float(px_pct) if px_pct is not None else np.nan,
    }

def scan_once(symbols: List[str]) -> pd.DataFrame:
    tasks: List[Tuple[str,str,int,int]] = []
    for sym in symbols:
        for period, limit in PERIODS.items():
            tasks.append((sym, period, limit, ROLLING_WIN.get(period, 80)))

    alerts = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process_one, s, p, l, w): (s, p) for (s, p, l, w) in tasks}
        for fut in as_completed(futures):
            try:
                res = fut.result()
                if res:
                    alerts.append(res)
            except Exception as e:
                s, p = futures[fut]
                print(f"[WARN] {s} {p} failed: {e}")

    if not alerts:
        print("\n=== No anomalies (latest bars) ===")
        return pd.DataFrame(columns=[
            "timestamp","symbol","period","level","direction","dOI","dOIValue","oi_value_pct","z_abs","price","price_pct"
        ])

    df = pd.DataFrame(alerts)
    df["abs_dOIValue"] = df["dOIValue"].abs()
    df = df.sort_values(["period", "abs_dOIValue"], ascending=[True, False]).drop(columns=["abs_dOIValue"]).reset_index(drop=True)

    # 打印（时间转上海）
    print("\n=== OI Anomaly Alerts (latest bar, fast mode) ===")
    for period in PERIODS.keys():
        sub = df[df["period"] == period]
        if sub.empty:
            continue
        topk = min(PRINT_TOP_N_PER_PERIOD, len(sub))
        print(f"\n[{period}] Top {topk}:")
        for _, r in sub.head(topk).iterrows():
            ts_sh = pd.to_datetime(r["timestamp"], utc=True).tz_convert(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S%z")
            px = r['price'] if not np.isnan(r['price']) else 'n/a'
            pxpct = (f"{r['price_pct']*100:+.2f}%" if not np.isnan(r['price_pct']) else "n/a")
            print(
                f"{ts_sh}  {r['symbol']:>12}  {period:>3}  "
                f"{r['direction']}  {r['level']:<9}  "
                f"ΔOIv={r['dOIValue']:,.0f} USD  "
                f"pct={r['oi_value_pct']*100:5.1f}%  "
                f"z={r['z_abs']:.2f}  "
                f"px={px}  pxΔ={pxpct}"
            )
    return df

def init_database(db_path: str):
    """
    初始化 SQLite 数据库，创建 oi_alerts 表（如果不存在）。
    表结构：
    - id: 自增主键
    - scan_time_shanghai: 扫描时间（上海时区）
    - timestamp: 数据时间戳（UTC）
    - timestamp_shanghai: 数据时间戳（上海时区）
    - symbol: 交易对
    - period: 时间周期
    - level: 告警等级
    - direction: 方向
    - dOI: OI 变化（张数）
    - dOIValue: OI 名义变化（USD）
    - oi_value_pct: OI 百分比变化
    - z_abs: z-score
    - price: 最新收盘价
    - price_pct: 价格变化百分比
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oi_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_time_shanghai TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            timestamp_shanghai TEXT NOT NULL,
            symbol TEXT NOT NULL,
            period TEXT NOT NULL,
            level TEXT NOT NULL,
            direction TEXT NOT NULL,
            dOI REAL,
            dOIValue REAL,
            oi_value_pct REAL,
            z_abs REAL,
            price REAL,
            price_pct REAL
        )
    """)

    # 创建索引以提高查询性能
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_scan_time
        ON oi_alerts(scan_time_shanghai)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_symbol
        ON oi_alerts(symbol)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_period
        ON oi_alerts(period)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_level
        ON oi_alerts(level)
    """)

    conn.commit()
    conn.close()
    print(f"[Database] Initialized at {db_path}")

def save_to_database(df: pd.DataFrame, db_path: str, run_time_sh: datetime) -> int:
    """
    将当前扫描结果保存到 SQLite 数据库。
    - 添加 scan_time_shanghai（整轮扫描时间）
    - 添加 timestamp_shanghai（数据时间戳的上海时区版本）
    - 返回插入的记录数
    """
    if df.empty:
        print("[Database] No alerts to save")
        return 0

    # 准备数据
    df_out = df.copy()

    # 添加扫描时间（上海时区）
    df_out["scan_time_shanghai"] = run_time_sh.strftime("%Y-%m-%d %H:%M:%S%z")

    # 转换 timestamp 为字符串（UTC）
    df_out["timestamp"] = pd.to_datetime(df_out["timestamp"], utc=True).dt.strftime("%Y-%m-%d %H:%M:%S%z")

    # 添加上海时区的时间戳
    ts_sh = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(ZoneInfo("Asia/Shanghai"))
    df_out["timestamp_shanghai"] = ts_sh.dt.strftime("%Y-%m-%d %H:%M:%S%z")

    # 选择要保存的列
    columns = [
        "scan_time_shanghai", "timestamp", "timestamp_shanghai",
        "symbol", "period", "level", "direction",
        "dOI", "dOIValue", "oi_value_pct", "z_abs", "price", "price_pct"
    ]
    df_out = df_out[columns]

    # 保存到数据库
    conn = sqlite3.connect(db_path)
    df_out.to_sql("oi_alerts", conn, if_exists="append", index=False)
    conn.close()

    record_count = len(df_out)
    print(f"[Database] Saved {record_count} records to {db_path}")
    return record_count

def main_once(symbols: List[str]) -> pd.DataFrame:
    """执行一次扫描并返回结果 DataFrame（UTC 时间戳列），打印时已转上海。"""
    return scan_once(symbols)

def main():
    # 初始化数据库
    init_database(DB_PATH)

    print("Fetching UM perpetual symbols ...")
    syms_all = get_um_perp_symbols()
    print(f"UM perpetual (filtered by quote: {QUOTE_WHITELIST if QUOTE_WHITELIST else 'ALL'}): {len(syms_all)}")
    syms = get_top_symbols_by_quote_volume(syms_all, TOP_N)
    print(f"Scanning symbols: {len(syms)} (Top {TOP_N if TOP_N else 'ALL'} by 24h quote volume)")

    # 循环执行
    sh_tz = ZoneInfo("Asia/Shanghai")
    try:
        while True:
            run_ts_sh = datetime.now(tz=sh_tz)
            print(f"\n=== New scan @ {run_ts_sh.strftime('%Y-%m-%d %H:%M:%S%z')} (Asia/Shanghai) ===")
            df = main_once(syms)
            save_to_database(df, DB_PATH, run_ts_sh)
            # 休眠 2.5 分钟
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exit.")

if __name__ == "__main__":
    main()
