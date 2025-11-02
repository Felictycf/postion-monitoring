# -*- coding: utf-8 -*-
"""
Fast Binance UM Perpetual - Open Interest Anomaly Scanner (loop, save CSV per run)
- 周期：5m/15m/30m/1h/4h
- 每隔 2.5 分钟扫描一次
- 每次扫描保存一个 CSV（文件名包含上海时区时间戳）
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

# 每轮结果保存目录（会自动创建）
SAVE_DIR = "./oi_scans"
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
    仅抓 OI，计算 ΔOIValue 的 z-score / 百分比 / 绝对名义USD，决定是否触发。
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
            "timestamp","symbol","period","level","direction","dOI","dOIValue","oi_value_pct","z_abs"
        ])

    df = pd.DataFrame(alerts)
    df["abs_dOIValue"] = df["dOIValue"].abs()
    df = df.sort_values(["period", "abs_dOIValue"], ascending=[True, False]).drop(columns=["abs_dOIValue"]).reset_index(drop=True)

    # 打印（时间转上海）
    print("\n=== OI Anomaly Alerts (latest bar, OI-only) ===")
    for period in PERIODS.keys():
        sub = df[df["period"] == period]
        if sub.empty:
            continue
        topk = min(PRINT_TOP_N_PER_PERIOD, len(sub))
        print(f"\n[{period}] Top {topk}:")
        for _, r in sub.head(topk).iterrows():
            ts_sh = pd.to_datetime(r["timestamp"], utc=True).tz_convert(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S%z")
            print(
                f"{ts_sh}  {r['symbol']:>12}  {period:>3}  "
                f"{r['direction']}  {r['level']:<9}  "
                f"ΔOIv={r['dOIValue']:,.0f} USD  "
                f"pct={r['oi_value_pct']*100:5.1f}%  "
                f"z={r['z_abs']:.2f}"
            )
    return df

def save_scan_csv(df: pd.DataFrame, save_dir: str, run_time_sh: datetime) -> str:
    """
    将当前扫描结果保存为独立 CSV。
    - 文件名：oi_alerts_YYYYmmdd_HHMMSS_SH.csv（上海时区）
    - 数据中的时间列也转换为上海时间（新增 timestamp_shanghai 列）
    - 额外加一列 scan_time_shanghai（整轮扫描时间，便于回放）
    """
    os.makedirs(save_dir, exist_ok=True)

    # 统一时间：新增上海时间列
    if "timestamp" in df.columns and not df.empty:
        ts_sh = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(ZoneInfo("Asia/Shanghai"))
        df_out = df.copy()
        df_out.insert(1, "timestamp_shanghai", ts_sh.dt.strftime("%Y-%m-%d %H:%M:%S%z"))
    else:
        df_out = df.copy()
        df_out.insert(0, "timestamp_shanghai", pd.Series(dtype=str))

    # 扫描时间（整轮）
    df_out.insert(0, "scan_time_shanghai", run_time_sh.strftime("%Y-%m-%d %H:%M:%S%z"))

    # 文件名（上海时区）
    fname = f"oi_alerts_{run_time_sh.strftime('%Y%m%d_%H%M%S')}_SH.csv"
    fpath = os.path.join(save_dir, fname)
    df_out.to_csv(fpath, index=False, encoding="utf-8")
    print(f"[Saved] {fpath}")
    return fpath

def main_once(symbols: List[str]) -> pd.DataFrame:
    """执行一次扫描并返回结果 DataFrame（UTC 时间戳列），打印时已转上海。"""
    return scan_once(symbols)

def main():
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
            save_scan_csv(df, SAVE_DIR, run_ts_sh)
            # 休眠 2.5 分钟
            time.sleep(POLL_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Exit.")

if __name__ == "__main__":
    main()
