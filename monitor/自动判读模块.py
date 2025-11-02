# -*- coding: utf-8 -*-
"""
Binance UM Perpetual - Robust 24h Analysis with Auto Interpretation
- 期货(UM) 1h: 价格/成交量
- OI 1h: open_interest_hist
- 现货(Spot) 1h: 价格 → 基差 Basis% = Fut/Spot - 1
- 资金费率(UM): funding_rate → 小时对齐并前向填充
- 就近对齐 merge_asof(tolerance=5min)，避免时间戳不一致导致空表
- 趋势打分 classify_trend（价格/EMA + OI + Funding + Basis + Volume）
- 自动判读 interpret_market_structure（给出一句话市场结构 + 细节理由）
- 绘图：价格vsOI、ΔOI名义、基差&资金费率
"""

from binance.um_futures import UMFutures
from binance.spot import Spot
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time

# ----------------- 可配置区 -----------------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]   # 分析标的（UM永续）
LIMIT = 24                                     # 最近N根小时K
SLEEP_SEC = 0.15
SHOW_PLOT = True
SAVE_PLOT = True
OUTPUT_DIR = "./"
TOL = pd.Timedelta("5min")                     # 数据对齐容忍度
# -------------------------------------------

um = UMFutures()
spot = Spot()

# ---------- 抓取 ----------
def fetch_oi_hist_um(symbol: str, limit: int = 24) -> pd.DataFrame:
    data = um.open_interest_hist(symbol=symbol, period="1h", limit=limit)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["sumOpenInterest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
    df["sumOpenInterestValue"] = pd.to_numeric(df["sumOpenInterestValue"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df.sort_values("timestamp")[["timestamp","sumOpenInterest","sumOpenInterestValue"]].reset_index(drop=True)

def fetch_futures_klines(symbol: str, limit: int = 24) -> pd.DataFrame:
    kl = um.klines(symbol=symbol, interval="1h", limit=limit)
    if not kl:
        return pd.DataFrame()
    cols = ["open_time","open","high","low","close","volume","close_time","qav",
            "num_trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(kl, columns=cols[:len(kl[0])])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")  # base 量
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df.rename(columns={"close_time": "timestamp"})
    return df[["timestamp","close","volume"]].sort_values("timestamp").reset_index(drop=True)

def fetch_spot_klines(symbol: str, limit: int = 24) -> pd.DataFrame:
    kl = spot.klines(symbol=symbol, interval="1h", limit=limit)
    if not kl:
        return pd.DataFrame()
    cols = ["open_time","open","high","low","close","volume","close_time","qav",
            "num_trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(kl, columns=cols[:len(kl[0])])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df.rename(columns={"close_time": "timestamp"})
    df = df[["timestamp","close"]].sort_values("timestamp").reset_index(drop=True)
    df.rename(columns={"close":"spot_close"}, inplace=True)
    return df

def fetch_funding_rate(symbol: str, hours_back: int = 72) -> pd.DataFrame:
    now_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
    start_ms = now_ms - hours_back * 3600_000
    items = um.funding_rate(symbol=symbol, startTime=start_ms, endTime=now_ms, limit=1000)
    df = pd.DataFrame(items)
    if df.empty:
        return df
    df["fundingRate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df = df.rename(columns={"fundingTime":"timestamp"})
    return df[["timestamp","fundingRate"]].sort_values("timestamp").reset_index(drop=True)

# ---------- 工具 ----------
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def analyze_merged(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["dPrice"] = out["fut_close"].diff()
    out["ret"] = out["fut_close"].pct_change()
    out["dOI"] = out["sumOpenInterest"].diff()
    out["dOIValue"] = out["sumOpenInterestValue"].diff()
    out["basisPct"] = out["fut_close"] / out["spot_close"] - 1.0
    out["ema6"] = ema(out["fut_close"], 6)
    out["ema24"] = ema(out["fut_close"], 24)
    out["short_build"] = (out["dOI"] > 0) & (out["dPrice"] < 0)
    return out

def classify_trend(df: pd.DataFrame) -> dict:
    """
    综合打分（>=1.75 Bullish；<=-1.75 Bearish；其余 Neutral）
    """
    res = {"label":"Neutral","score":0.0,"reasons":[]}
    if df.empty or len(df) < 12:
        res["reasons"].append("Data too short; default Neutral.")
        return res

    c_last = float(df["fut_close"].iloc[-1]); c_24 = float(df["fut_close"].iloc[0])
    ret24 = (c_last/c_24 - 1) if c_24 else 0.0
    if len(df) >= 7:
        c_6 = float(df["fut_close"].iloc[-7]); ret6 = (c_last/c_6 - 1) if c_6 else 0.0
    else:
        ret6 = np.nan
    oi_last = float(df["sumOpenInterestValue"].iloc[-1]); oi_24 = float(df["sumOpenInterestValue"].iloc[0])
    doi24 = oi_last - oi_24

    score = 0.0
    # 价格动量
    if ret24 > 0: score += 2; res["reasons"].append(f"24h price up {ret24:.2%} (+2)")
    elif ret24 < 0: score -= 2; res["reasons"].append(f"24h price down {ret24:.2%} (-2)")
    else: res["reasons"].append("24h price flat (+0)")
    if not np.isnan(ret6):
        if ret6 > 0: score += 1; res["reasons"].append(f"6h price up {ret6:.2%} (+1)")
        elif ret6 < 0: score -= 1; res["reasons"].append(f"6h price down {ret6:.2%} (-1)")
        else: res["reasons"].append("6h price flat (+0)")
    # 均线排列
    if df["ema6"].iloc[-1] > df["ema24"].iloc[-1]:
        score += 1; res["reasons"].append("EMA(6)>EMA(24) (+1)")
    else:
        score -= 1; res["reasons"].append("EMA(6)<=EMA(24) (-1)")
    # OI 行为
    if doi24 > 0 and ret24 > 0:
        score += 1; res["reasons"].append("OI↑ & price↑ (long build) (+1)")
    elif doi24 > 0 and ret24 < 0:
        score -= 1; res["reasons"].append("OI↑ & price↓ (short build) (-1)")
    elif doi24 < 0 and ret24 > 0:
        score += 0.5; res["reasons"].append("OI↓ & price↑ (short covering) (+0.5)")
    else:
        res["reasons"].append("OI/price mixed (+0)")
    if bool(df["short_build"].iloc[-1]):
        score -= 0.5; res["reasons"].append("Last hour short_build=True (-0.5)")
    # 资金费率
    if "fundingRate" in df.columns and df["fundingRate"].notna().any():
        fr_last = float(df["fundingRate"].ffill().iloc[-1])
        fr_ma24 = float(df["fundingRate"].ffill().tail(24).mean())
        if fr_last > 0 and fr_last > fr_ma24:
            score += 0.5; res["reasons"].append(f"Funding>0 & rising ({fr_last:.4%}>{fr_ma24:.4%}) (+0.5)")
        elif fr_last < 0 and fr_last < fr_ma24:
            score -= 0.5; res["reasons"].append(f"Funding<0 & falling ({fr_last:.4%}<{fr_ma24:.4%}) (-0.5)")
        else:
            res["reasons"].append("Funding neutral (+0)")
    # 基差（近6h vs 前6h）
    if "basisPct" in df.columns and df["basisPct"].notna().sum() >= 12:
        last6 = df["basisPct"].tail(6).mean()
        prev6 = df["basisPct"].tail(12).head(6).mean()
        if last6 > prev6 and last6 > 0:
            score += 1; res["reasons"].append("Basis positive & widening (+1)")
        elif last6 < prev6 and last6 < 0:
            score -= 1; res["reasons"].append("Basis negative & widening (-1)")
        else:
            res["reasons"].append("Basis neutral (+0)")
    # 成交量确认（近6h vs 前6h）
    if "fut_volume" in df.columns:
        v_last6 = df["fut_volume"].tail(6).sum()
        v_prev6 = df["fut_volume"].tail(12).head(6).sum()
        vol_up = v_last6 > v_prev6 * 1.05
        if not np.isnan(ret6):
            if vol_up and ret6 > 0:
                score += 0.5; res["reasons"].append("Rising volume with rising price (+0.5)")
            elif vol_up and ret6 < 0:
                score -= 0.5; res["reasons"].append("Rising volume with falling price (-0.5)")
            else:
                res["reasons"].append("Volume neutral (+0)")

    res["score"] = round(score, 3)
    res["label"] = "Bullish" if score >= 1.75 else ("Bearish" if score <= -1.75 else "Neutral")
    return res

# ---------- 自动判读（新增模块） ----------
def interpret_market_structure(df: pd.DataFrame) -> dict:
    """
    返回 {headline, bullets[]}：
    - headline：一句话总结（如“轻度贴水、偏空去杠杆”）
    - bullets ：理由列表
    依据：
      * Basis：最近6小时均值 last6 与 0 的关系（正=溢价/Contango，负=贴水/Backwardation）
      * Funding：最新 fundingRate 相对 24h 均值
      * OI 与 价格：24h 变动方向组合（去杠杆/空头加仓/多头加仓/空头回补）
      * Volume：近6h vs 前6h
    """
    out = {"headline":"Neutral structure", "bullets":[]}
    if df.empty or df["fut_close"].isna().all():
        out["headline"] = "Insufficient data"
        out["bullets"].append("Series missing.")
        return out

    # Basis
    basis_last6 = df["basisPct"].tail(6).mean()
    if pd.isna(basis_last6):
        basis_judg = "basis: n/a"
    elif basis_last6 > 0.0005:
        basis_judg = "contango (positive basis)"
    elif basis_last6 < -0.0005:
        basis_judg = "backwardation (negative basis)"
    else:
        basis_judg = "basis near flat"

    # Funding
    if "fundingRate" in df.columns and df["fundingRate"].notna().any():
        fr_last = float(df["fundingRate"].ffill().iloc[-1])
        fr_ma24 = float(df["fundingRate"].ffill().tail(24).mean())
        if fr_last > 0 and fr_last > fr_ma24:
            fr_judg = "funding positive & rising (bullish risk appetite)"
        elif fr_last < 0 and fr_last < fr_ma24:
            fr_judg = "funding negative & falling (bearish risk appetite)"
        elif fr_last > 0:
            fr_judg = "funding positive (mildly bullish)"
        elif fr_last < 0:
            fr_judg = "funding negative (mildly bearish)"
        else:
            fr_judg = "funding neutral"
    else:
        fr_judg = "funding unavailable"

    # OI vs Price（24h）
    c_last = float(df["fut_close"].iloc[-1]); c_24 = float(df["fut_close"].iloc[0])
    ret24 = (c_last/c_24 - 1) if c_24 else 0.0
    oi_last = float(df["sumOpenInterestValue"].iloc[-1]) if "sumOpenInterestValue" in df.columns else np.nan
    oi_24   = float(df["sumOpenInterestValue"].iloc[0])  if "sumOpenInterestValue" in df.columns else np.nan
    doi24 = (oi_last - oi_24) if (not np.isnan(oi_last) and not np.isnan(oi_24)) else np.nan

    if not np.isnan(doi24):
        if doi24 < 0 and ret24 < 0:
            oi_price = "deleverage down (long unwinding)"
        elif doi24 > 0 and ret24 < 0:
            oi_price = "short build (price down with OI up)"
        elif doi24 > 0 and ret24 > 0:
            oi_price = "long build (trend continuation up)"
        elif doi24 < 0 and ret24 > 0:
            oi_price = "short covering (squeeze/relief)"
        else:
            oi_price = "mixed OI/price"
    else:
        oi_price = "OI unavailable"

    # Volume（近6h vs 前6h）
    if "fut_volume" in df.columns:
        v_last6 = df["fut_volume"].tail(6).sum()
        v_prev6 = df["fut_volume"].tail(12).head(6).sum()
        vol_up = v_last6 > v_prev6 * 1.05
        vol_str = "rising volume" if vol_up else "neutral volume"
    else:
        vol_str = "volume unavailable"

    # Headline 组合
    # 规则优先级：basis → funding → OI/price
    if "backwardation" in basis_judg:
        head = "Negative basis (backwardation), bearish-leaning"
    elif "contango" in basis_judg:
        head = "Positive basis (contango), bullish-leaning"
    else:
        head = "Basis flat, mixed signals"

    # 若 OI/price 指向“deleverage down”，强调去杠杆
    if "deleverage down" in oi_price:
        head += ", deleveraging down"

    out["headline"] = head
    out["bullets"] = [
        f"Basis (last 6h mean): {basis_judg}",
        f"Funding: {fr_judg}",
        f"OI vs Price (24h): {oi_price}",
        f"Volume: {vol_str}",
    ]
    return out

# ---------- 画图 ----------
def plot_price_oi(df: pd.DataFrame, symbol: str, trend: dict = None):
    ts = df["timestamp"].dt.tz_convert("UTC")
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()
    ax1.plot(ts, df["fut_close"], label="Price (close)")
    ax2.plot(ts, df["sumOpenInterestValue"], label="OI Notional (USD)")
    ax1.set_xlabel("Time (UTC)"); ax1.set_ylabel("Price"); ax2.set_ylabel("Open Interest Notional (USD)")
    ax1.grid(True, alpha=0.3)
    L1, LB1 = ax1.get_legend_handles_labels()
    L2, LB2 = ax2.get_legend_handles_labels()
    ax1.legend(L1+L2, LB1+LB2, loc="upper left")
    title = f"{symbol} - 24h Price vs Open Interest (1h)"
    if trend: title += f" | Trend: {trend['label']} (score={trend['score']})"
    plt.title(title)
    if SAVE_PLOT: fig.savefig(f"{OUTPUT_DIR}{symbol}_price_oi_24h.png", dpi=150, bbox_inches="tight")
    if SHOW_PLOT: plt.show()
    else: plt.close(fig)

def plot_delta_oi(df: pd.DataFrame, symbol: str):
    ts = df["timestamp"].dt.tz_convert("UTC")
    fig, ax = plt.subplots(figsize=(10, 2.8))
    ax.bar(ts, df["dOIValue"].fillna(0))
    ax.set_title(f"{symbol} - Δ OI Notional (1h)")
    ax.set_xlabel("Time (UTC)"); ax.set_ylabel("Δ OI Notional (USD)"); ax.grid(True, axis="y", alpha=0.3)
    if SAVE_PLOT: fig.savefig(f"{OUTPUT_DIR}{symbol}_delta_oi_24h.png", dpi=150, bbox_inches="tight")
    if SHOW_PLOT: plt.show()
    else: plt.close(fig)

def plot_basis_funding(df: pd.DataFrame, symbol: str):
    ts = df["timestamp"].dt.tz_convert("UTC")
    fig, ax1 = plt.subplots(figsize=(10, 3.8))
    ax2 = ax1.twinx()
    ax1.plot(ts, df["basisPct"]*100.0, label="Basis (%)")
    ax2.step(ts, df["fundingRate"].ffill()*100.0, where="post", label="Funding Rate (%)")
    ax1.set_xlabel("Time (UTC)")
    ax1.set_ylabel("Basis (%)")
    ax2.set_ylabel("Funding Rate (%)")
    ax1.grid(True, alpha=0.3)
    L1, LB1 = ax1.get_legend_handles_labels()
    L2, LB2 = ax2.get_legend_handles_labels()
    ax1.legend(L1+L2, LB1+LB2, loc="upper left")
    plt.title(f"{symbol} - Basis & Funding (1h aligned)")
    if SAVE_PLOT: fig.savefig(f"{OUTPUT_DIR}{symbol}_basis_funding_24h.png", dpi=150, bbox_inches="tight")
    if SHOW_PLOT: plt.show()
    else: plt.close(fig)

# ---------- 主流程 ----------
def run_for_symbol(symbol: str):
    print(f"\n=== {symbol} ===")
    # 抓取
    fut = fetch_futures_klines(symbol, limit=LIMIT); time.sleep(SLEEP_SEC)
    oi  = fetch_oi_hist_um(symbol, limit=LIMIT);    time.sleep(SLEEP_SEC)
    spt = fetch_spot_klines(symbol, limit=LIMIT);   time.sleep(SLEEP_SEC)
    fr  = fetch_funding_rate(symbol, hours_back=72)

    # 调试概览
    def _rng(df, name):
        if df.empty: print(f"[{symbol}] {name}: EMPTY")
        else: print(f"[{symbol}] {name}: n={len(df)}, {df['timestamp'].min()} → {df['timestamp'].max()}")
    _rng(fut, "FUT_KLINE"); _rng(oi, "OI_1H"); _rng(spt, "SPOT_KLINE"); _rng(fr, "FUNDING")

    if fut.empty:
        print(f"[{symbol}] futures kline missing; skip.")
        return

    # 以期货K线为基准时间轴
    base = fut.rename(columns={"close":"fut_close","volume":"fut_volume"}).sort_values("timestamp")

    # asof 对齐 OI / 现货 / 资金费率
    if not oi.empty:
        base = pd.merge_asof(base, oi.sort_values("timestamp"),
                             on="timestamp", direction="nearest", tolerance=TOL)
    else:
        base["sumOpenInterest"] = np.nan; base["sumOpenInterestValue"] = np.nan

    if not spt.empty:
        base = pd.merge_asof(base, spt.sort_values("timestamp"),
                             on="timestamp", direction="nearest", tolerance=TOL)
    else:
        base["spot_close"] = np.nan

    if not fr.empty:
        base = pd.merge_asof(base, fr.sort_values("timestamp"),
                             on="timestamp", direction="backward", tolerance=pd.Timedelta("8h"))
        base["fundingRate"] = base["fundingRate"].ffill()
    else:
        base["fundingRate"] = np.nan

    # 仅保留最近 LIMIT 根
    base = base.tail(LIMIT).reset_index(drop=True)

    # 分析
    df = analyze_merged(base)

    # 自动判读 & 趋势打分
    structure = interpret_market_structure(df)
    trend = classify_trend(df)

    # 输出
    cols = ["timestamp","fut_close","spot_close","sumOpenInterestValue","basisPct","fundingRate",
            "dPrice","dOIValue","fut_volume","short_build"]
    print(df[cols].tail(6).to_string(index=False))
    print(f"> Market structure: {structure['headline']}")
    for b in structure["bullets"]:
        print(" -", b)
    print(f"> Trend decision: {trend['label']} (score={trend['score']})")
    for r in trend["reasons"]:
        print(" -", r)

    # 绘图
    plot_price_oi(df, symbol, trend=trend)
    plot_delta_oi(df, symbol)
    plot_basis_funding(df, symbol)

def main():
    for s in SYMBOLS:
        run_for_symbol(s)
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
