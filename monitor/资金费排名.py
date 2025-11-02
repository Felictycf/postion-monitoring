# -*- coding: utf-8 -*-
"""
Binance UM Perpetual — Funding Rate Ranking & High Basis Scanner (robust)
- 兼容旧版 binance-connector：
  * 优先使用 UMFutures.mark_price(symbol) 读取 indexPrice & lastFundingRate（对应 /fapi/v1/premiumIndex）
  * 回退：Spot.ticker_price(symbol) 近似现货价 + UMFutures.funding_rate(symbol, limit=1)
  * 永续最新价：UMFutures.ticker_price(symbol)
- 输出：
  * 资金费率 Top 正/负
  * 基差 Top 正/负（Basis% = FuturesLast / IndexPrice - 1）
  * 绝对基差 Top（现货与合约差价高）

依赖：
    pip install -U binance-connector pandas
"""

from __future__ import annotations
import math, random, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
from binance.um_futures import UMFutures
from binance.spot import Spot

# ================= 配置 =================
TOP_N = 20                    # 榜单显示前 N 名
BASIS_ABS_PCT_MIN = 0.20      # 绝对基差阈值（%），用于高差价清单
EXPORT_CSV = False            # 是否导出 CSV
CSV_PATH = "um_perp_funding_basis_snapshot.csv"

MAX_WORKERS = 8               # 并发线程数（过大易限频）
MAX_RETRY = 3                 # 单请求最大重试次数
BASE_SLEEP = 0.06             # 请求间基础 sleep（秒）
JITTER = 0.03                 # 抖动（秒）
# ======================================

um = UMFutures()
spot = Spot()

def _sleep_a_bit():
    time.sleep(BASE_SLEEP + random.random() * JITTER)

def call_with_retry(func, *args, **kwargs):
    for i in range(1, MAX_RETRY + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == MAX_RETRY:
                return None
            time.sleep((BASE_SLEEP * (2 ** (i - 1))) + random.random() * JITTER)

def get_um_perp_symbols() -> list[str]:
    """所有 U 本位永续（PERPETUAL & TRADING）的 symbol。"""
    info = call_with_retry(um.exchange_info)
    symbols = []
    for s in (info.get("symbols", []) if info else []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    return sorted(set(symbols))

def fetch_futures_last(symbol: str) -> float:
    """合约最新价 /fapi/v1/ticker/price。"""
    _sleep_a_bit()
    d = call_with_retry(um.ticker_price, symbol=symbol)
    try:
        return float(d.get("price")) if d else math.nan
    except Exception:
        return math.nan

def fetch_index_and_funding_via_mark(symbol: str):
    """
    优先用 mark_price()（/fapi/v1/premiumIndex）拿 indexPrice + lastFundingRate。
    返回：(index_price, index_time_iso, funding_rate, funding_time_iso)
    """
    _sleep_a_bit()
    d = call_with_retry(um.mark_price, symbol=symbol)  # 旧版常有该方法
    if not d:
        return math.nan, None, math.nan, None

    # 期望字段：indexPrice, lastFundingRate, time, nextFundingTime
    try:
        idx = float(d.get("indexPrice")) if d.get("indexPrice") is not None else math.nan
    except Exception:
        idx = math.nan

    try:
        fr = float(d.get("lastFundingRate")) if d.get("lastFundingRate") is not None else math.nan
    except Exception:
        fr = math.nan

    # 时间：优先 funding 最近时间（若返回），否则取 time
    iso_idx = None
    if d.get("time") is not None:
        try:
            iso_idx = datetime.fromtimestamp(int(d["time"]) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            iso_idx = None

    iso_fr = None
    if d.get("nextFundingTime") is not None:
        try:
            iso_fr = datetime.fromtimestamp(int(d["nextFundingTime"]) / 1000, tz=timezone.utc).isoformat()
        except Exception:
            iso_fr = None

    return idx, iso_idx, fr, iso_fr

def fetch_index_price_fallback(symbol: str):
    """回退：用现货最新价近似指数价（仅在 mark_price 无 indexPrice 时用）。"""
    _sleep_a_bit()
    d = call_with_retry(spot.ticker_price, symbol=symbol)
    try:
        px = float(d.get("price")) if d else math.nan
    except Exception:
        px = math.nan
    return px

def fetch_last_funding_fallback(symbol: str):
    """回退：从 funding_rate(limit=1) 拿最近资金费率及时间。"""
    _sleep_a_bit()
    arr = call_with_retry(um.funding_rate, symbol=symbol, limit=1)
    if not arr:
        return math.nan, None
    try:
        fr = float(arr[-1]["fundingRate"])
    except Exception:
        fr = math.nan
    try:
        iso = datetime.fromtimestamp(int(arr[-1]["fundingTime"]) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        iso = None
    return fr, iso

def build_row(symbol: str) -> dict:
    """
    生成单个 symbol 的快照行：
      - futures last
      - index (via mark_price, fallback to spot)
      - funding (via mark_price, fallback to funding_rate)
    """
    fut_last = fetch_futures_last(symbol)

    idx, idx_iso, fr, fr_iso = fetch_index_and_funding_via_mark(symbol)

    # 如果 mark_price 没给 index 或 funding，用回退方案补齐
    if math.isnan(idx):
        idx = fetch_index_price_fallback(symbol)
    if math.isnan(fr):
        fr, fr_iso = fetch_last_funding_fallback(symbol)

    basis_pct = (fut_last / idx - 1.0) * 100.0 if (not math.isnan(fut_last) and not math.isnan(idx) and idx != 0.0) else math.nan

    return {
        "symbol": symbol,
        "fundingRatePct": (fr * 100.0) if not math.isnan(fr) else math.nan,
        "basisPct": basis_pct,
        "lastPrice": fut_last,
        "indexPrice": idx,
        "indexTime": idx_iso,
        "fundingTime": fr_iso
    }

def build_snapshot_df() -> pd.DataFrame:
    symbols = get_um_perp_symbols()
    rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_map = {ex.submit(build_row, sym): sym for sym in symbols}
        for fut in as_completed(fut_map):
            row = fut.result()
            if row:
                rows.append(row)
    df = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)
    return df

def fmt_pct(x: float) -> str:
    return "nan" if pd.isna(x) else f"{x:.4f}%"

def print_rankings(df: pd.DataFrame, top_n: int = TOP_N):
    if df.empty:
        print("No data."); return

    fr_desc = df.dropna(subset=["fundingRatePct"]).sort_values("fundingRatePct", ascending=False)
    fr_asc  = df.dropna(subset=["fundingRatePct"]).sort_values("fundingRatePct", ascending=True)

    basis_pos = df.dropna(subset=["basisPct"]).sort_values("basisPct", ascending=False)
    basis_neg = df.dropna(subset=["basisPct"]).sort_values("basisPct", ascending=True)

    basis_abs = df.dropna(subset=["basisPct"]).copy()
    basis_abs["absBasis"] = basis_abs["basisPct"].abs()
    basis_abs = basis_abs.sort_values("absBasis", ascending=False)

    print(f"\nTotal UM perpetuals: {len(df)}")

    print("\n=== Funding Rate Top (Positive) ===")
    print(fr_desc.head(top_n)[["symbol","fundingRatePct","basisPct","lastPrice","indexPrice","fundingTime"]]
          .to_string(index=False, formatters={"fundingRatePct": fmt_pct, "basisPct": fmt_pct}))

    print("\n=== Funding Rate Top (Negative) ===")
    print(fr_asc.head(top_n)[["symbol","fundingRatePct","basisPct","lastPrice","indexPrice","fundingTime"]]
          .to_string(index=False, formatters={"fundingRatePct": fmt_pct, "basisPct": fmt_pct}))

    print("\n=== Basis Top (Positive, Futures>Index) ===")
    print(basis_pos.head(top_n)[["symbol","basisPct","fundingRatePct","lastPrice","indexPrice","indexTime"]]
          .to_string(index=False, formatters={"fundingRatePct": fmt_pct, "basisPct": fmt_pct}))

    print("\n=== Basis Top (Negative, Futures<Index) ===")
    print(basis_neg.head(top_n)[["symbol","basisPct","fundingRatePct","lastPrice","indexPrice","indexTime"]]
          .to_string(index=False, formatters={"fundingRatePct": fmt_pct, "basisPct": fmt_pct}))

    print(f"\n=== High Basis by Absolute |basis| >= {BASIS_ABS_PCT_MIN:.3f}% ===")
    hb = basis_abs[basis_abs["absBasis"] >= BASIS_ABS_PCT_MIN]
    if hb.empty:
        print("(none above threshold)")
    else:
        print(hb.head(top_n)[["symbol","basisPct","fundingRatePct","lastPrice","indexPrice","indexTime","absBasis"]]
              .to_string(index=False, formatters={"fundingRatePct": fmt_pct, "basisPct": fmt_pct, "absBasis": fmt_pct}))

def main():
    df = build_snapshot_df()
    if df.empty:
        print("Empty snapshot. Try again later."); return

    print_rankings(df, top_n=TOP_N)

    if EXPORT_CSV:
        out = df.copy()
        out["fundingRatePct"] = out["fundingRatePct"].map(lambda v: v if pd.isna(v) else round(v, 6))
        out["basisPct"] = out["basisPct"].map(lambda v: v if pd.isna(v) else round(v, 6))
        out.to_csv(CSV_PATH, index=False, encoding="utf-8")
        print(f"\nSaved snapshot CSV: {CSV_PATH}")

if __name__ == "__main__":
    main()
