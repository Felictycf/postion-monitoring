# -*- coding: utf-8 -*-
"""
monitor_binance_oi_bot.py

Binance UM Perpetual 监控机器人（控制台打印版）
- 监控时间级别：15m / 1h / 4h / 8h
- 周期性拉取：Open Interest（OI）与 K 线收盘价
- 计算变化幅度，识别“多头加仓 / 空头加仓 / 平仓”等倾向
- 触发阈值时打印【异常提醒】
- 打印结构化中文报告（接近截图风格）

依赖:
    pip install binance-connector pandas numpy matplotlib pytz

说明:
- OI历史接口 period 不支持 8h，因此 4h/8h 通过 1h OI 聚合得到；
- 15m 使用 5m OI 聚合得到；
- 价格 K线直接使用对应 interval（Binance 支持 15m/1h/4h/8h）。
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from binance.um_futures import UMFutures

# ========================= 配置区 =========================
SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "ZECUSDT",
]

# 监控的时间级别
TIMEFRAMES: List[str] = ["15m", "1h", "4h", "8h"]

# 每次拉取的K线/历史条数（越大越安全；至少要覆盖到“比较窗口”）
LIMITS = {
    "5m": 200,    # 用于生成 15m OI
    "15m": 200,
    "1h": 200,
    "4h": 200,
    "8h": 200,
}

# 异常阈值（相对于“对比点”）
THRESHOLDS = {
    "price_pct": 0.05,     # 价格涨跌超过 5%
    "oi_pct": 0.10,        # OI 变化超过 10%
    "lr_pct": 0.05,        # 多空比（Long/Short Ratio）变化超过 5%（若使用）
}

# 轮询间隔秒（建议 >= 60）
POLL_SECONDS = 60

# 是否在终端同时打印最近一段数据（调试时可设 True）
VERBOSE_TAIL = 0
# ========================================================


# -------- Binance 客户端（无需 Key 的公共行情调用即可） --------
um = UMFutures()

# --------- 工具函数 ---------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def fmt_ts(dt: pd.Timestamp) -> str:
    if pd.isna(dt):
        return "n/a"
    return dt.tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S (北京时间)")

def pct(a: float) -> str:
    if pd.isna(a):
        return "n/a"
    s = f"{a*100:.2f}%"
    return ("+" + s) if a >= 0 else s

def safe_pct_change(cur: float, prev: float) -> float:
    if prev in (0, None) or pd.isna(prev) or pd.isna(cur):
        return np.nan
    return (cur - prev) / prev

# --------- 数据抓取 ---------
def fetch_oi_hist(symbol: str, period: str, limit: int) -> pd.DataFrame:
    """
    U本位OI历史。period 支持: 5m, 15m, 1h, 4h（官方不提供 8h）
    返回列: [timestamp, sumOpenInterest, sumOpenInterestValue]
    """
    data = um.open_interest_hist(symbol=symbol, period=period, limit=limit)
    df = pd.DataFrame(data)
    if df.empty:
        return df
    for col in ["sumOpenInterest", "sumOpenInterestValue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df[["timestamp", "sumOpenInterest", "sumOpenInterestValue"]].sort_values("timestamp")
    return df.reset_index(drop=True)

def fetch_klines_close(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    """
    获取收盘价时间序列，使用 Binance kline。
    返回列: [timestamp(close_time), close]
    """
    kl = um.klines(symbol=symbol, interval=interval, limit=limit)
    cols = ["open_time","open","high","low","close","volume","close_time","qav",
            "num_trades","taker_base","taker_quote","ignore"]
    df = pd.DataFrame(kl, columns=cols[:len(kl[0])])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    df = df[["timestamp", "close"]].sort_values("timestamp").reset_index(drop=True)
    return df

# --------- 频率对齐 / 聚合 ---------
def align_price_oi(price_df: pd.DataFrame, oi_df: pd.DataFrame) -> pd.DataFrame:
    """
    左连接合并到同一时间戳（以价格为基准），并补齐差分列。
    """
    if price_df.empty or oi_df.empty:
        return pd.DataFrame()
    df = pd.merge_asof(
        price_df.sort_values("timestamp"),
        oi_df.sort_values("timestamp"),
        on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("5min"),
    )
    # 计算差分
    df["dPrice"] = df["close"].diff()
    df["ret"] = df["close"].pct_change()
    df["dOI"] = df["sumOpenInterest"].diff()
    df["dOIValue"] = df["sumOpenInterestValue"].diff()
    # 组合信号
    df["long_build"]  = (df["dOI"] > 0) & (df["dPrice"] > 0)
    df["short_build"] = (df["dOI"] > 0) & (df["dPrice"] < 0)
    df["long_cover_or_short_close"] = (df["dOI"] < 0) & (df["dPrice"] < 0)
    df["short_cover_or_long_close"] = (df["dOI"] < 0) & (df["dPrice"] > 0)
    return df

def build_oi_for_tf(symbol: str, tf: str) -> pd.DataFrame:
    """
    为目标时间级别构造 OI 时间序列：
    - 15m: 用 5m OI 取每 3 根的最后一根
    - 1h,4h,8h: 用 1h OI，并取每 1/4/8 小时的最后一根
    """
    if tf == "15m":
        raw = fetch_oi_hist(symbol, "5m", LIMITS["5m"])
        step = 3
    else:
        raw = fetch_oi_hist(symbol, "1h", LIMITS["1h"])
        step = {"1h": 1, "4h": 4, "8h": 8}[tf]

    if raw.empty:
        return raw

    # 取“每 step 根的最后一根”（等价于对齐到 tf 的收盘时刻）
    raw = raw.copy().reset_index(drop=True)
    sel = raw.iloc[::step, :].copy()
    # 如果不是对齐末根，可改为 groupby+last，这里为了简单可读性
    return sel

# --------- 报告与异常判定 ---------
def generate_report(symbol: str, tf: str, df: pd.DataFrame) -> Tuple[str, Dict]:
    """
    生成报告字符串 & 关键数值字典（便于后续接入推送）
    比较“当前”vs“对比点”（上一个同级别时间点）
    """
    if df.empty or len(df) < 2:
        return f"[{symbol}][{tf}] 数据不足，无法生成报告。", {}

    cur = df.iloc[-1]
    prev = df.iloc[-2]

    # 计算变化
    price_cur, price_prev = float(cur["close"]), float(prev["close"])
    oi_cur, oi_prev = float(cur["sumOpenInterest"]), float(prev["sumOpenInterest"])
    oiv_cur, oiv_prev = float(cur["sumOpenInterestValue"]), float(prev["sumOpenInterestValue"])

    price_chg = price_cur - price_prev
    price_pct = safe_pct_change(price_cur, price_prev)

    oi_chg = oi_cur - oi_prev
    oi_pct = safe_pct_change(oi_cur, oi_prev)

    oiv_chg = oiv_cur - oiv_prev
    oiv_pct = safe_pct_change(oiv_cur, oiv_prev)

    # 简单建议逻辑（可根据需要调整）
    if (oi_chg > 0) and (price_chg < 0):
        bias = "主力疑似加空（OI↑ & 价↓）"
        suggest = "谨慎偏空/反弹再空"
        badge = "📉"
    elif (oi_chg > 0) and (price_chg > 0):
        bias = "主力疑似加多（OI↑ & 价↑）"
        suggest = "考虑逢低做多"
        badge = "📈"
    elif (oi_chg < 0) and (price_chg > 0):
        bias = "空头回补或多头平仓（OI↓ & 价↑）"
        suggest = "短线偏多观察"
        badge = "🔁"
    else:
        bias = "多头回补或空头平仓（OI↓ & 价↓）"
        suggest = "观望为主"
        badge = "🔂"

    report = []
    report.append(f"🔷 {symbol} 多空比/持仓监控报告（{tf}）")
    report.append(f"当前时间：{fmt_ts(pd.Timestamp(cur['timestamp']))}")
    report.append("| 指标 | 对比点 | 当前 | 变化幅度 |")
    report.append("|------|--------|------|----------|")
    report.append(f"| 价格 | ${price_prev:.4f} | ${price_cur:.4f} | {pct(price_pct)} |")
    report.append(f"| 持仓量(OI) | {oi_prev:.4f} | {oi_cur:.4f} | {pct(oi_pct)} |")
    report.append(f"| OI名义(USD) | ${oiv_prev:,.2f} | ${oiv_cur:,.2f} | {pct(oiv_pct)} |")
    report.append("")
    report.append(f"{badge} 结论：{bias}")
    report.append(f"✅ 建议：{suggest}")
    report.append(f"对比时间：{fmt_ts(pd.Timestamp(prev['timestamp']))}")
    text = "\n".join(report)

    keyvals = {
        "symbol": symbol,
        "timeframe": tf,
        "price_pct": price_pct,
        "oi_pct": oi_pct,
        "oiv_pct": oiv_pct,
        "bias": bias,
        "suggest": suggest,
    }
    return text, keyvals

def is_abnormal(keys: Dict) -> bool:
    """
    简单异常规则：价格或 OI 的变化超过阈值；
    同时给“加空/加多”这类强信号直接提示异常。
    """
    if not keys:
        return False
    if (not pd.isna(keys.get("price_pct"))) and abs(keys["price_pct"]) >= THRESHOLDS["price_pct"]:
        return True
    if (not pd.isna(keys.get("oi_pct"))) and abs(keys["oi_pct"]) >= THRESHOLDS["oi_pct"]:
        return True
    # 强倾向也提示
    bias = (keys.get("bias") or "")
    if "加空" in bias or "加多" in bias:
        return True
    return False

# --------- 主流程 ---------
def run_once_for_symbol_tf(symbol: str, tf: str):
    """单次拉取 + 计算 + 打印"""
    # 构造目标时间级别的 OI
    oi_df = build_oi_for_tf(symbol, tf)
    if oi_df.empty:
        print(f"[{symbol}][{tf}] OI 数据为空。")
        return

    # 构造价格时间序列
    price_df = fetch_klines_close(symbol, tf, LIMITS.get(tf, 200))
    if price_df.empty:
        print(f"[{symbol}][{tf}] 价格数据为空。")
        return

    # 对齐 & 报告
    df = align_price_oi(price_df, oi_df)
    if df.empty or len(df) < 2:
        print(f"[{symbol}][{tf}] 对齐后数据不足。")
        return

    if VERBOSE_TAIL:
        print(df.tail(VERBOSE_TAIL).to_string(index=False))

    text, keys = generate_report(symbol, tf, df)
    print("\n" + text + "\n")

    if is_abnormal(keys):
        print(f"⚠️ 异常提醒 [{symbol}][{tf}] 价格变化: {pct(keys.get('price_pct'))}, OI变化: {pct(keys.get('oi_pct'))}，信号：{keys.get('bias')}\n")

def main_loop():
    print("=== Binance UM Perpetual 监控机器人启动 ===")
    print("监控交易对：", ", ".join(SYMBOLS))
    print("时间级别：", ", ".join(TIMEFRAMES))
    print(f"轮询间隔：{POLL_SECONDS}s\n")

    while True:
        start = time.time()
        for sym in SYMBOLS:
            for tf in TIMEFRAMES:
                try:
                    run_once_for_symbol_tf(sym, tf)
                    # 小睡避免触发限频（按需调整）
                    time.sleep(0.2)
                except Exception as e:
                    print(f"[{sym}][{tf}] 发生异常：{e}")
        # 控制整体循环节奏
        dt = time.time() - start
        sleep_left = max(0.0, POLL_SECONDS - dt)
        time.sleep(sleep_left)

if __name__ == "__main__":
    main_loop()
