# -*- coding: utf-8 -*-
"""
Binance UM Perpetual - 24h Open Interest vs Price + Trend Classification
- 拉取过去24小时 1h OI
- 拉取过去24小时 1h K线收盘价
- 合并并对齐时间
- 画图（价格 + OI）
- 识别：OI↑ 且 价格↓ 的时段（疑似空头加仓）
- 新增：基于价格 & OI 的综合趋势判断（看涨 / 看跌 / 中性）
"""

from binance.um_futures import UMFutures
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime, timezone

# ----------- 可配置区 -----------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]  # 想分析的永续合约（UM本位）
LIMIT = 24                                   # 最近24小时
SLEEP_SEC = 0.15                              # 接口间隔，避免触发限频
SHOW_PLOT = True                              # 是否展示图像
SAVE_PLOT = True                              # 是否保存图像为 PNG
OUTPUT_DIR = "./"                             # 图像保存位置
# --------------------------------

um = UMFutures()  # 公共行情接口无需API Key

def fetch_oi_hist_um(symbol: str, limit: int = 24) -> pd.DataFrame:
    data = um.open_interest_hist(symbol=symbol, period="1h", limit=limit)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    for col in ["sumOpenInterest", "sumOpenInterestValue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df[["timestamp", "sumOpenInterest", "sumOpenInterestValue"]].sort_values("timestamp")
    df["time_iso"] = df["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%z")
    return df.reset_index(drop=True)

def fetch_price_klines_um(symbol: str, limit: int = 24) -> pd.DataFrame:
    kl = um.klines(symbol=symbol, interval="1h", limit=limit)
    cols = ["open_time","open","high","low","close","volume","close_time","qav","num_trades",
            "taker_base","taker_quote","ignore"]
    df = pd.DataFrame(kl, columns=cols[:len(kl[0])])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df[["close_time", "close"]].rename(columns={"close_time": "timestamp"})
    return df.sort_values("timestamp").reset_index(drop=True)

def analyze_oi_price(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["dOI"] = out["sumOpenInterest"].diff()
    out["dOIValue"] = out["sumOpenInterestValue"].diff()
    out["dPrice"] = out["close"].diff()
    out["ret"] = out["close"].pct_change()
    out["short_build"] = (out["dOI"] > 0) & (out["dPrice"] < 0)
    return out

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def classify_trend(df: pd.DataFrame) -> dict:
    """
    综合判定趋势：返回 {label, score, reasons}
    规则（可按需调权重）：
    +2  : 近24h收益率 > 0
    +1  : 近6h收益率  > 0
    +1  : EMA(6) > EMA(24)
    +1  : 近24h OI名义(USD) 增加 且 近24h价格上涨（资金随涨流入 = 多头加仓）
    -1  : 近24h OI名义增加 且 近24h价格下跌（资金逆价流入 = 疑似空头加仓）
    +0.5: 近24h OI名义减少 且 近24h价格上涨（空头回补/多头减仓→对价格有利）
    -0.5: 最近一小时 short_build=True（OI↑ & 价↓）
    阈值：score >= 1.5 -> Bullish；score <= -1.5 -> Bearish；否则 Neutral
    """
    res = {"label": "Neutral", "score": 0.0, "reasons": []}
    if df.empty or len(df) < 6:
        res["reasons"].append("Data too short; default Neutral.")
        return res

    df = df.copy()
    df["ema6"] = ema(df["close"], 6)
    df["ema24"] = ema(df["close"], 24)

    # 近24h与6h的价格变化（用最后一根与最早一根比较）
    c_last = float(df["close"].iloc[-1])
    c_24   = float(df["close"].iloc[0])
    ret24 = (c_last / c_24 - 1.0) if c_24 != 0 else 0.0

    # 6h窗口：最后6根与第 -6 根对比
    if len(df) >= 7:
        c_6 = float(df["close"].iloc[-7])
        ret6 = (c_last / c_6 - 1.0) if c_6 != 0 else 0.0
    else:
        ret6 = np.nan

    # OI名义变化（24h）
    oi_last = float(df["sumOpenInterestValue"].iloc[-1])
    oi_24   = float(df["sumOpenInterestValue"].iloc[0])
    doi24 = oi_last - oi_24

    score = 0.0

    # 价格动量
    if ret24 > 0:
        score += 2.0
        res["reasons"].append(f"24h price up: {ret24:.2%} (+2.0)")
    elif ret24 < 0:
        score -= 2.0
        res["reasons"].append(f"24h price down: {ret24:.2%} (-2.0)")
    else:
        res["reasons"].append("24h price flat (+0.0)")

    if not np.isnan(ret6):
        if ret6 > 0:
            score += 1.0
            res["reasons"].append(f"6h price up: {ret6:.2%} (+1.0)")
        elif ret6 < 0:
            score -= 1.0
            res["reasons"].append(f"6h price down: {ret6:.2%} (-1.0)")
        else:
            res["reasons"].append("6h price flat (+0.0)")

    # 多空排列
    if df["ema6"].iloc[-1] > df["ema24"].iloc[-1]:
        score += 1.0
        res["reasons"].append("EMA(6) > EMA(24) (+1.0)")
    else:
        score -= 1.0
        res["reasons"].append("EMA(6) <= EMA(24) (-1.0)")

    # 资金行为（24h OI名义）
    if doi24 > 0 and ret24 > 0:
        score += 1.0
        res["reasons"].append("OI↑ with price↑ (long build) (+1.0)")
    elif doi24 > 0 and ret24 < 0:
        score -= 1.0
        res["reasons"].append("OI↑ with price↓ (short build) (-1.0)")
    elif doi24 < 0 and ret24 > 0:
        score += 0.5
        res["reasons"].append("OI↓ with price↑ (short covering) (+0.5)")
    else:
        res["reasons"].append("OI & price mixed/flat (+0.0)")

    # 最近1小时若出现 short_build 提示，略减分
    if bool(df["short_build"].iloc[-1]):
        score -= 0.5
        res["reasons"].append("Last hour short_build=True (-0.5)")

    res["score"] = round(score, 3)
    if score >= 1.5:
        res["label"] = "Bullish"
    elif score <= -1.5:
        res["label"] = "Bearish"
    else:
        res["label"] = "Neutral"

    return res

def summarize(pattern_df: pd.DataFrame) -> dict:
    d = {}
    sign_oi = np.sign(pattern_df["dOI"].fillna(0))
    sign_p = np.sign(pattern_df["dPrice"].fillna(0))
    d["long_build"] = int(((sign_oi > 0) & (sign_p > 0)).sum())
    d["short_build"] = int(((sign_oi > 0) & (sign_p < 0)).sum())
    d["short_cover_or_long_close"] = int(((sign_oi < 0) & (sign_p > 0)).sum())
    d["long_cover_or_short_close"] = int(((sign_oi < 0) & (sign_p < 0)).sum())
    last_row = pattern_df.tail(1)
    d["last_hour_flag"] = "short_build" if (not last_row.empty and bool(last_row["short_build"].iloc[0])) else "other"
    return d

def plot_symbol(df: pd.DataFrame, symbol: str, trend: dict = None, save: bool = True, show: bool = True):
    if df.empty:
        print(f"[{symbol}] no data to plot.")
        return

    ts = df["timestamp"].dt.tz_convert("UTC")
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()

    ax1.plot(ts, df["close"], label="Price (close)")
    ax1.set_xlabel("Time (UTC)")
    ax1.set_ylabel("Price")
    ax1.grid(True, alpha=0.3)

    ax2.plot(ts, df["sumOpenInterestValue"], label="OI Notional (USD)")
    ax2.set_ylabel("Open Interest Notional (USD)")

    # 合并图例
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    title = f"{symbol} - 24h Price vs Open Interest (1h)"
    if trend:
        title += f" | Trend: {trend['label']} (score={trend['score']})"
    plt.title(title)

    # 第二张：柱状图显示 OI 名义变化
    fig2, ax3 = plt.subplots(figsize=(10, 2.8))
    ax3.bar(ts, df["dOIValue"].fillna(0))
    ax3.set_title(f"{symbol} - Δ OI Notional (1h)")
    ax3.set_xlabel("Time (UTC)")
    ax3.set_ylabel("Δ OI Notional (USD)")
    ax3.grid(True, axis="y", alpha=0.3)

    if save:
        p1 = f"{OUTPUT_DIR}{symbol}_price_oi_24h.png"
        p2 = f"{OUTPUT_DIR}{symbol}_delta_oi_24h.png"
        plt.tight_layout()
        fig.savefig(p1, dpi=150, bbox_inches="tight")
        fig2.savefig(p2, dpi=150, bbox_inches="tight")
        print(f"Saved: {p1}\nSaved: {p2}")

    if show:
        plt.show()
    else:
        plt.close(fig)
        plt.close(fig2)

def ensure_um_perp(symbols):
    try:
        info = um.exchange_info()
        um_perps = {s["symbol"] for s in info.get("symbols", [])
                    if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"}
        valid = [s for s in symbols if s in um_perps]
        if not valid:
            print("Warning: none of the provided symbols are active UM perpetuals.")
        return valid
    except Exception as e:
        print("exchange_info() failed, skip validation:", e)
        return symbols

def run_for_symbol(symbol: str):
    print(f"\n=== {symbol} ===")
    oi = fetch_oi_hist_um(symbol, limit=LIMIT)
    time.sleep(SLEEP_SEC)
    px = fetch_price_klines_um(symbol, limit=LIMIT)

    if oi.empty or px.empty:
        print(f"[{symbol}] OI or Kline empty.")
        return

    # 合并并对齐到小时结束时间
    df = pd.merge_asof(
        oi.sort_values("timestamp"),
        px.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("5min")
    )

    # 分析、打标签、作图
    df = analyze_oi_price(df)
    stats = summarize(df)
    trend = classify_trend(df)

    # 打印最近6行 & 统计 & 趋势
    show_cols = ["timestamp", "close", "sumOpenInterest", "sumOpenInterestValue", "dPrice", "dOI", "dOIValue", "short_build"]
    print(df[show_cols].tail(6).to_string(index=False))
    print("Pattern counts (last 24h 1h bars):", stats)
    print(f"Trend decision: {trend['label']} (score={trend['score']})")
    for r in trend["reasons"]:
        print(" -", r)

    plot_symbol(df, symbol, trend=trend, save=SAVE_PLOT, show=SHOW_PLOT)

def main():
    syms = ensure_um_perp(SYMBOLS)
    for s in syms:
        run_for_symbol(s)
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
