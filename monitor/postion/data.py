# -*- coding: utf-8 -*-
"""
Binance UM Perpetual - Open Interest Anomaly Scanner
- 扫描所有在交易的 U本位永续合约（PERPETUAL）
- 周期：15m / 30m / 1h / 4h
- 指标：ΔOI（张数）、ΔOI名义(USD)、z-score、百分比变化
- 分级：Minor / Moderate / Major / Critical
- 触发：当最新一根 K（对应周期的 OI 栏）达到任一等级时打印告警
"""

from binance.um_futures import UMFutures
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional
import os

# ========== 可配置区 ==========
# 扫描周期与每个周期的历史长度（越长基准越稳，越慢）
PERIODS = {
    "15m": 200,
    "30m": 200,
    "1h": 200,
    "4h": 200,
}

# 速率控制（避免频控）
SLEEP_BETWEEN_CALLS = 0.12      # 每次HTTP调用后的sleep秒
SLEEP_BETWEEN_SYMBOLS = 0.05    # 同一symbol多个周期之间的短暂停
SLEEP_BETWEEN_ROUNDS = 0         # 扫描一轮所有symbol后是否休息

# 异动判定阈值（满足任意一个等级的全部条件即触发该等级）
# 说明：abs_usd = |ΔOI名义(USD)|，pct = |ΔOI名义| / 上一根OI名义
ANOMALY_THRESHOLDS = [
    # level,  min_z,  min_pct,  min_abs_usd
    ("Critical", 5.0, 0.10, 5_000_000),
    ("Major",    4.0, 0.07, 1_500_000),
    ("Moderate", 3.0, 0.04,   500_000),
    ("Minor",    2.0, 0.02,   100_000),
]

# 滚动窗口长度（用于计算 z-score 的基准），可按周期微调
# 若为 None 则随 PERIODS 的limit自动取 min(100, limit//2)
ROLLING_WINDOWS = {
    "15m": 100,
    "30m": 100,
    "1h":  100,
    "4h":  100,
}

# 仅对这些quote资产做白名单（减少小币种噪音），None=不过滤
QUOTE_WHITELIST = {"USDT"}  # 只扫描 *USDT 永续
# QUOTE_WHITELIST = None    # 扫描所有

# 打印与落盘
PRINT_TOP_N_PER_PERIOD = 10       # 每个周期打印TopN异动（按 |ΔOI名义USD| 排序）
SAVE_ALERTS_CSV = True
ALERTS_PATH = "./oi_anomaly_alerts.csv"

# ========== 代码区 ==========

um = UMFutures()  # 公共行情接口无需 API Key

def get_um_perp_symbols() -> List[str]:
    """获取在交易的 UM 永续合约列表，可选按报价币种白名单过滤。"""
    info = um.exchange_info()
    syms = []
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            sym = s.get("symbol")
            q = s.get("quoteAsset")
            if QUOTE_WHITELIST is None or q in QUOTE_WHITELIST:
                syms.append(sym)
    return sorted(syms)

def fetch_oi_hist(symbol: str, period: str, limit: int) -> pd.DataFrame:
    """
    拉取 U本位 永续合约 OI 历史（period in ['15m','30m','1h','4h',...], limit <= 500）
    返回列：timestamp, sumOpenInterest, sumOpenInterestValue
    """
    data = um.open_interest_hist(symbol=symbol, period=period, limit=limit)
    time.sleep(SLEEP_BETWEEN_CALLS)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    for col in ["sumOpenInterest", "sumOpenInterestValue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df[["timestamp", "sumOpenInterest", "sumOpenInterestValue"]].sort_values("timestamp")
    return df.reset_index(drop=True)

def fetch_price_close(symbol: str, period: str, limit: int) -> pd.DataFrame:
    """
    可选：获取相同周期 Kline 收盘价，用于方向参考（非触发条件）
    返回：timestamp(close_time), close
    """
    kl = um.klines(symbol=symbol, interval=period, limit=limit)
    time.sleep(SLEEP_BETWEEN_CALLS)
    if not kl:
        return pd.DataFrame()
    cols = ["open_time","open","high","low","close","volume","close_time",
            "qav","num_trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(kl, columns=cols[:len(kl[0])])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df[["close_time","close"]].rename(columns={"close_time":"timestamp"}).sort_values("timestamp").reset_index(drop=True)

def prepare_df(symbol: str, period: str, limit: int) -> pd.DataFrame:
    """合并 OI 与收盘价，计算 ΔOI / ΔOI名义 / 百分比 / z-score。"""
    oi = fetch_oi_hist(symbol, period, limit)
    if oi.empty or len(oi) < 20:
        return pd.DataFrame()
    px = fetch_price_close(symbol, period, limit)
    df = oi
    if not px.empty:
        df = pd.merge_asof(
            oi.sort_values("timestamp"),
            px.sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("3min")
        )
    # 差分
    df["dOI"] = df["sumOpenInterest"].diff()
    df["dOIValue"] = df["sumOpenInterestValue"].diff()
    # 百分比（名义对比上一根）
    df["oi_value_pct"] = df["dOIValue"].abs() / df["sumOpenInterestValue"].shift(1).replace(0, np.nan)
    # 方向
    df["direction"] = np.where(df["dOIValue"] > 0, "↑", np.where(df["dOIValue"] < 0, "↓", "="))
    # z-score 基于 |ΔOI名义|
    win = ROLLING_WINDOWS.get(period) or min(100, max(30, limit // 2))
    base = df["dOIValue"].abs().rolling(win, min_periods=20)
    df["roll_mean_abs"] = base.mean()
    df["roll_std_abs"] = base.std(ddof=0)
    df["z_abs"] = (df["dOIValue"].abs() - df["roll_mean_abs"]) / df["roll_std_abs"]
    return df

def classify_level(z_abs: float, pct: float, abs_usd: float) -> Optional[str]:
    """根据阈值表返回等级，返回最严重的一个等级；不满足则返回 None。"""
    if any(v is None or np.isnan(v) for v in (z_abs, pct, abs_usd)):
        return None
    for level, min_z, min_pct, min_abs in ANOMALY_THRESHOLDS:
        if z_abs >= min_z and pct >= min_pct and abs_usd >= min_abs:
            return level
    return None

def scan_once(symbols: List[str]) -> pd.DataFrame:
    """
    扫描一轮。返回所有触发的告警记录 DataFrame。
    字段：timestamp, symbol, period, level, direction, dOI, dOIValue, oi_value_pct, z_abs, price(optional)
    """
    alerts = []
    for sym in symbols:
        for period, limit in PERIODS.items():
            try:
                df = prepare_df(sym, period, limit)
                if df.empty or len(df) < 30:
                    continue
                last = df.iloc[-1]
                level = classify_level(
                    z_abs=float(last["z_abs"]),
                    pct=float(last["oi_value_pct"]) if pd.notna(last["oi_value_pct"]) else np.nan,
                    abs_usd=float(abs(last["dOIValue"])) if pd.notna(last["dOIValue"]) else np.nan
                )
                if level:
                    alerts.append({
                        "timestamp": last["timestamp"],
                        "symbol": sym,
                        "period": period,
                        "level": level,
                        "direction": str(last["direction"]),
                        "dOI": float(last["dOI"]) if pd.notna(last["dOI"]) else np.nan,
                        "dOIValue": float(last["dOIValue"]) if pd.notna(last["dOIValue"]) else np.nan,
                        "oi_value_pct": float(last["oi_value_pct"]) if pd.notna(last["oi_value_pct"]) else np.nan,
                        "z_abs": float(last["z_abs"]) if pd.notna(last["z_abs"]) else np.nan,
                        "price": float(last.get("close", np.nan)),
                    })
            except Exception as e:
                print(f"[WARN] {sym} {period} failed: {e}")
            time.sleep(SLEEP_BETWEEN_SYMBOLS)
        # 可在此控制每个symbol间隔
    # 输出与排名
    if alerts:
        alert_df = pd.DataFrame(alerts).sort_values(["level","timestamp"], ascending=[True, True])
        # 更合理：按 |ΔOI名义| 降序作为强弱参考
        alert_df = alert_df.sort_values(by=["period","dOIValue"], key=lambda s: s.abs(), ascending=[True, False]).reset_index(drop=True)
        print("\n=== OI Anomaly Alerts (latest bar) ===")
        for period in PERIODS.keys():
            sub = alert_df[alert_df["period"] == period]
            if sub.empty:
                continue
            print(f"\n[{period}] Top {min(PRINT_TOP_N_PER_PERIOD, len(sub))}:")
            for _, r in sub.head(PRINT_TOP_N_PER_PERIOD).iterrows():
                ts = r["timestamp"].strftime("%Y-%m-%d %H:%M:%S%z")
                print(
                    f"{ts}  {r['symbol']:>12}  {period:>3}  "
                    f"{r['direction']}  {r['level']:<9}  "
                    f"ΔOIv={r['dOIValue']:,.0f} USD  "
                    f"pct={r['oi_value_pct']*100:5.1f}%  "
                    f"z={r['z_abs']:.2f}  "
                    f"px={r['price'] if not np.isnan(r['price']) else 'n/a'}"
                )
        if SAVE_ALERTS_CSV:
            # 统一为UTC ISO
            out = alert_df.copy()
            out["timestamp_utc"] = pd.to_datetime(out["timestamp"], utc=True)
            cols = ["timestamp_utc","symbol","period","level","direction","dOI","dOIValue","oi_value_pct","z_abs","price"]
            out = out[cols]
            exists = os.path.exists(ALERTS_PATH)
            out.to_csv(ALERTS_PATH, mode="a", header=not exists, index=False)
            print(f"\n[Saved] Append alerts to: {ALERTS_PATH}")
        return alert_df
    else:
        print("\n=== No anomalies in the latest bars ===")
        return pd.DataFrame()

def main():
    print("Fetching UM perpetual symbols ...")
    symbols = get_um_perp_symbols()
    print(f"Total UM perpetual symbols: {len(symbols)}")
    # 单次扫描（如需循环监控，可自行 while True 包裹）
    _ = scan_once(symbols)
    if SLEEP_BETWEEN_ROUNDS > 0:
        time.sleep(SLEEP_BETWEEN_ROUNDS)

if __name__ == "__main__":
    main()
