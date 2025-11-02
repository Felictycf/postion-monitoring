# pip install binance-connector pandas
from binance.um_futures import UMFutures
from binance.cm_futures import CMFutures
import pandas as pd
from datetime import datetime, timezone

def get_um_perpetual_symbols():
    """U本位：筛出所有 PERPETUAL 的交易对（如 BTCUSDT、ETHUSDT）"""
    um = UMFutures()
    info = um.exchange_info()
    symbols = []
    for s in info.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    return symbols

def get_cm_perpetual_pairs():
    """币本位：筛出有 PERPETUAL 合约的 pair（如 BTCUSD、ETHUSD）"""
    cm = CMFutures()
    info = cm.exchange_info()
    pairs = set()
    for s in info.get("symbols", []):
        # COIN-M 的 exchange_info 里同一个 pair 可能有不同交割合约，这里只要标记有 PERPETUAL 即记录该 pair
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            pairs.add(s["pair"])  # 注意是 pair 而不是 symbol
    return sorted(pairs)

def fetch_um_oi_1h(symbols):
    """拉取 U本位永续的 1h OI（最近一条）"""
    um = UMFutures()
    rows = []
    for sym in symbols:
        try:
            # limit=1 取最近一条；返回为列表
            data = um.open_interest_hist(symbol=sym, period="1h", limit=1)
            if data:
                d = data[-1]
                rows.append({
                    "market": "UM_PERP",
                    "instrument": sym,
                    "sumOpenInterest": float(d.get("sumOpenInterest", 0.0)),
                    "sumOpenInterestValue": float(d.get("sumOpenInterestValue", 0.0)),
                    "timestamp": int(d.get("timestamp")),
                })
        except Exception as e:
            # 可以根据需要打印或记录
            pass
    return rows

def fetch_cm_oi_1h(pairs):
    """拉取 币本位永续 的 1h OI（最近一条）"""
    cm = CMFutures()
    rows = []
    for pair in pairs:
        try:
            data = cm.open_interest_hist(pair=pair, contractType="PERPETUAL", period="1h", limit=1)
            if data:
                d = data[-1]
                rows.append({
                    "market": "CM_PERP",
                    "instrument": pair,  # COIN-M 用 pair 表示标的（如 BTCUSD）
                    "sumOpenInterest": float(d.get("sumOpenInterest", 0.0)),
                    "sumOpenInterestValue": float(d.get("sumOpenInterestValue", 0.0)),
                    "timestamp": int(d.get("timestamp")),
                })
        except Exception as e:
            pass
    return rows

def main():
    # 1) 列出所有永续合约
    um_symbols = get_um_perpetual_symbols()
    cm_pairs = get_cm_perpetual_pairs()

    # 2) 分别抓取 1h 合约持仓量（最近一条）
    um_rows = fetch_um_oi_1h(um_symbols)
    cm_rows = fetch_cm_oi_1h(cm_pairs)

    # 3) 合并 & 美化
    rows = um_rows + cm_rows
    df = pd.DataFrame(rows)
    if not df.empty:
        df["time_iso"] = df["timestamp"].apply(lambda t: datetime.fromtimestamp(t/1000, tz=timezone.utc).isoformat())
        # 按名义价值从大到小排序
        df = df.sort_values("sumOpenInterestValue", ascending=False).reset_index(drop=True)

    # 展示前 20 条
    print(df.head(20).to_string(index=False))

    # 如需保存成 CSV：
    # df.to_csv("perpetual_oi_1h_latest.csv", index=False)

if __name__ == "__main__":
    main()
