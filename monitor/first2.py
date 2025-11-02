# -*- coding: utf-8 -*-
"""
Binance UM Perpetual - 24h Open Interest vs Price
- 拉取过去24小时 1h OI
- 拉取过去24小时 1h K线收盘价
- 合并并对齐时间
- 画图（价格 + OI）
- 识别：OI↑ 且 价格↓ 的时段（疑似空头加仓）
"""

from binance.um_futures import UMFutures
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import time
from datetime import datetime, timezone

# ----------- 可配置区 -----------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT","BLESSUSDT","COAIUSDT","SLERFUSDT"]  # 想分析的永续合约（UM本位）
LIMIT = 24                                   # 最近24小时
SLEEP_SEC = 0.15                              # 接口间隔，避免触发限频
SHOW_PLOT = True                              # 是否展示图像
SAVE_PLOT = True                              # 是否保存图像为 PNG
OUTPUT_DIR = "./"                             # 图像保存位置
# --------------------------------

um = UMFutures()  # 公共行情接口无需API Key

def fetch_oi_hist_um(symbol: str, limit: int = 24) -> pd.DataFrame:
    """
    获取 U本位 永续合约 1h OI 历史（最近 limit 条）
    返回 DataFrame: [timestamp, sumOpenInterest, sumOpenInterestValue]
    """
    data = um.open_interest_hist(symbol=symbol, period="1h", limit=limit)
    # 返回是按时间升序，确保为 DataFrame
    df = pd.DataFrame(data)
    if df.empty:
        return df
    # 统一列类型
    for col in ["sumOpenInterest", "sumOpenInterestValue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    # 用小时结束时间（Binance返回的就是小时结束）
    df = df[["timestamp", "sumOpenInterest", "sumOpenInterestValue"]].sort_values("timestamp")
    df["time_iso"] = df["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%z")
    return df.reset_index(drop=True)

def fetch_price_klines_um(symbol: str, limit: int = 24) -> pd.DataFrame:
    """
    获取 U本位 永续合约 1h K线（最近 limit 根）
    返回 DataFrame: [open_time, close_time, close]
    """
    kl = um.klines(symbol=symbol, interval="1h", limit=limit)
    # kline: [open_time, open, high, low, close, volume, close_time, ...]
    cols = ["open_time","open","high","low","close","volume","close_time","qav","num_trades",
            "taker_base","taker_quote","ignore"]
    df = pd.DataFrame(kl, columns=cols[:len(kl[0])])  # 兼容字段数
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    # 用小时结束时间（close_time）对齐到 OI 的 timestamp
    df = df[["close_time", "close"]].rename(columns={"close_time": "timestamp"})
    return df.sort_values("timestamp").reset_index(drop=True)

def analyze_oi_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    在合并后的 df（含 price & OI）上计算：
    - ΔOI（张数变化）
    - ΔPrice（价格差/或收益率）
    - 标记 OI↑ & Price↓ 的时段
    """
    if df.empty:
        return df
    out = df.copy()
    out["dOI"] = out["sumOpenInterest"].diff()
    out["dOIValue"] = out["sumOpenInterestValue"].diff()
    out["dPrice"] = out["close"].diff()
    out["ret"] = out["close"].pct_change()

    # 条件：OI增加 且 价格下跌 → 疑似空头加仓
    out["short_build"] = (out["dOI"] > 0) & (out["dPrice"] < 0)
    return out

def summarize(pattern_df: pd.DataFrame) -> dict:
    """
    统计各类组合出现次数：
    - OI↑ & 价↑（多头加仓倾向）
    - OI↑ & 价↓（疑似空头加仓）
    - OI↓ & 价↑（空头减仓/多头平仓）
    - OI↓ & 价↓（多头减仓/空头平仓）
    """
    d = {}
    sign_oi = np.sign(pattern_df["dOI"].fillna(0))
    sign_p = np.sign(pattern_df["dPrice"].fillna(0))
    d["long_build"] = int(((sign_oi > 0) & (sign_p > 0)).sum())
    d["short_build"] = int(((sign_oi > 0) & (sign_p < 0)).sum())
    d["short_cover_or_long_close"] = int(((sign_oi < 0) & (sign_p > 0)).sum())
    d["long_cover_or_short_close"] = int(((sign_oi < 0) & (sign_p < 0)).sum())
    # 最近一小时的标签（如果有）
    last_row = pattern_df.tail(1)
    if not last_row.empty:
        last_flag = "short_build" if bool(last_row["short_build"].iloc[0]) else "other"
    else:
        last_flag = "n/a"
    d["last_hour_flag"] = last_flag
    return d

def plot_symbol(df: pd.DataFrame, symbol: str, save: bool = True, show: bool = True):
    """
    画图：价格（左轴） + OI名义价值（右轴）
    下面再画一张 OI 变化柱状图（可直观看资金流入/流出）
    """
    if df.empty:
        print(f"[{symbol}] no data to plot.")
        return

    # 用 matplotlib 的默认配色（不强制设置颜色）
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

    plt.title(f"{symbol} - 24h Price vs Open Interest (1h)")

    # 第二张：柱状图显示 OI 名义变化（资金净流入/流出）
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
    """
    校验这些 symbol 是否是 UM 本位的 PERPETUAL 合约（可选：为了稳妥）。
    """
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
    """
    单个 symbol 的完整流程：抓OI、抓K线、对齐、分析、作图、打印统计
    """
    print(f"\n=== {symbol} ===")
    oi = fetch_oi_hist_um(symbol, limit=LIMIT)
    time.sleep(SLEEP_SEC)
    px = fetch_price_klines_um(symbol, limit=LIMIT)

    if oi.empty or px.empty:
        print(f"[{symbol}] OI or Kline empty.")
        return

    # 合并：按 timestamp 精确对齐（两者都在小时结束时间）
    df = pd.merge_asof(
        oi.sort_values("timestamp"),
        px.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("5min")  # 容忍小偏差（通常不需要）
    )

    # 分析并画图
    df = analyze_oi_price(df)
    stats = summarize(df)

    # 打印最近几行数据
    show_cols = ["timestamp", "close", "sumOpenInterest", "sumOpenInterestValue", "dPrice", "dOI", "dOIValue", "short_build"]
    print(df[show_cols].tail(6).to_string(index=False))

    # 概览统计
    print("Pattern counts (last 24h 1h bars):", stats)

    # 作图
    plot_symbol(df, symbol, save=SAVE_PLOT, show=SHOW_PLOT)

def main():
    syms = ensure_um_perp(SYMBOLS)
    for s in syms:
        run_for_symbol(s)
        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    main()
