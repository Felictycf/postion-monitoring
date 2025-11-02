# -*- coding: utf-8 -*- 
"""
Fast Binance UM Perpetual - Open Interest Anomaly Scanner
- 并发扫描 USDT 永续（可切换全量），周期：15m / 30m / 1h / 4h
- 先仅抓 OI 历史做 z-score + 百分比，触发后再拉该标的该周期的最新收盘价
- 24h 成交额 Top N 预筛，显著减少调用
"""

from binance.um_futures import UMFutures
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple
import requests
import math

# ================= 可配置 =================
# 周期与历史长度（足够做滚动统计，不要太大）
PERIODS = {
    "15m": 160,
    "30m": 160,
    "1h":  160,
    "4h":  160,
}

# z-score 统计用窗口
ROLLING_WIN = {
    "15m": 80,
    "30m": 80,
    "1h":  80,
    "4h":  80,
}

# 异动等级：满足同级全部条件视为该级别（越前越严）
ANOMALY_THRESHOLDS = [
    ("Critical", 5.0, 0.10, 5_000_000),
    ("Major",    4.0, 0.07, 1_500_000),
    ("Moderate", 3.0, 0.04,   500_000),
    ("Minor",    2.0, 0.02,   100_000),
]

# 仅 USDT 计价；如需全部，设为 None
QUOTE_WHITELIST = {"USDT"}

# 扫描 24h 成交额 Top N（None=不筛选）
TOP_N = 120

# 并发线程数（可根据带宽/机器调大，注意交易所限频）
MAX_WORKERS = 24

# 每个周期输出 TopN
PRINT_TOP_N_PER_PERIOD = 12
# ========================================

# 连接复用：给 binance-connector 绑定一个 requests.Session
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
session.mount("https://", adapter)
session.mount("http://", adapter)

um = UMFutures()


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
    # 拉全部 24h 变动（返回现货/合约混合符号没关系，后面做交集）
    tickers = um.ticker_24hr_price_change()  # list
    rows = []
    cset = set(candidates)
    for t in tickers:
        sym = t.get("symbol")
        if sym in cset:
            # futures 24hr返回包含 "quoteVolume"
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

def fetch_last_close(symbol: str, period: str) -> Optional[float]:
    kl = um.klines(symbol=symbol, interval=period, limit=1)
    if not kl:
        return None
    close = float(kl[0][4])
    return close

def classify_level(z_abs: float, pct: float, abs_usd: float) -> Optional[str]:
    if any(map(lambda v: v is None or (isinstance(v, float) and math.isnan(v)), (z_abs, pct, abs_usd))):
        return None
    for level, min_z, min_pct, min_abs in ANOMALY_THRESHOLDS:
        if z_abs >= min_z and pct >= min_pct and abs_usd >= min_abs:
            return level
    return None

def process_one(symbol: str, period: str, limit: int, win: int) -> Optional[Dict[str, Any]]:
    """
    仅抓 OI，算出是否异动；若异动，再补拉1根价格。
    返回告警字典；无告警返回 None
    """
    df = fetch_oi_hist(symbol, period, limit)
    if df.empty or len(df) < max(40, win + 5):
        return None

    df["dOI"] = df["sumOpenInterest"].diff()
    df["dOIValue"] = df["sumOpenInterestValue"].diff()
    # 百分比：名义变化相对上一根名义
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

    price = fetch_last_close(symbol, period)  # 仅在触发后补拉
    direction = "↑" if (last["dOIValue"] > 0) else ("↓" if last["dOIValue"] < 0 else "=")

    return {
        "timestamp": last["timestamp"],
        "symbol": symbol,
        "period": period,
        "level": level,
        "direction": direction,
        "dOI": float(last["dOI"]) if pd.notna(last["dOI"]) else np.nan,
        "dOIValue": float(last["dOIValue"]) if pd.notna(last["dOIValue"]) else np.nan,
        "oi_value_pct": float(pct) if pct is not None else np.nan,
        "z_abs": float(z_abs) if z_abs is not None else np.nan,
        "price": float(price) if price is not None else np.nan,
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
        return pd.DataFrame()

    df = pd.DataFrame(alerts)
    # 每个周期按 |ΔOI名义| 排序
    df["abs_dOIValue"] = df["dOIValue"].abs()
    df = df.sort_values(["period", "abs_dOIValue"], ascending=[True, False]).reset_index(drop=True)

    print("\n=== OI Anomaly Alerts (latest bar, fast mode) ===")
    for period in PERIODS.keys():
        sub = df[df["period"] == period]
        if sub.empty:
            continue
        print(f"\n[{period}] Top {min(PRINT_TOP_N_PER_PERIOD, len(sub))}:")
        for _, r in sub.head(PRINT_TOP_N_PER_PERIOD).iterrows():
            ts = pd.to_datetime(r["timestamp"], utc=True).strftime("%Y-%m-%d %H:%M:%S%z")
            print(
                f"{ts}  {r['symbol']:>12}  {period:>3}  "
                f"{r['direction']}  {r['level']:<9}  "
                f"ΔOIv={r['dOIValue']:,.0f} USD  "
                f"pct={r['oi_value_pct']*100:5.1f}%  "
                f"z={r['z_abs']:.2f}  "
                f"px={r['price'] if not np.isnan(r['price']) else 'n/a'}"
            )
    return df.drop(columns=["abs_dOIValue"])

def main():
    print("Fetching UM perpetual symbols ...")
    syms = get_um_perp_symbols()
    print(f"UM perpetual (filtered by quote: {QUOTE_WHITELIST if QUOTE_WHITELIST else 'ALL'}): {len(syms)}")
    syms = get_top_symbols_by_quote_volume(syms, TOP_N)
    print(f"Scanning symbols: {len(syms)} (Top {TOP_N if TOP_N else 'ALL'} by 24h quote volume)")
    _ = scan_once(syms)

if __name__ == "__main__":
    main()
