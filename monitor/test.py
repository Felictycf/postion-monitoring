import ccxt
import pandas as pd
import sys
import json
from datetime import datetime, timezone

# 定义用于存储数据快照的文件名
SNAPSHOT_FILE = 'oi_snapshot.json'


def get_current_snapshot(exchange):
    """
    获取所有U本位永续合约的当前持仓量快照。
    使用 fetch_tickers 接口，高效且稳定。
    (修复版: 获取所有tickers后在本地进行过滤，以提高稳定性)
    """
    print("正在加载市场列表以筛选U本位永续合约...")
    try:
        markets = exchange.load_markets()
        # 1. 先从市场列表中筛选出所有活跃的U本位永续合约的 symbol，并放入一个Set中以便快速查找
        usdt_swap_symbols_set = {
            market['symbol'] for market in markets.values()
            if market.get('swap') and market.get('quote') == 'USDT' and market.get('active')
        }

        if not usdt_swap_symbols_set:
            print("错误：无法从交易所市场列表中找到任何活跃的U本位永续合约。")
            return None

        print(f"市场加载完毕，发现 {len(usdt_swap_symbols_set)} 个目标合约。正在一次性获取所有市场数据...")

        # 2. 获取所有可用的 Tickers，不传入特定合约列表
        all_tickers = exchange.fetch_tickers()
        snapshot = {}

        # 3. 遍历获取到的所有 Tickers，并与我们的目标列表进行匹配
        for symbol, ticker in all_tickers.items():
            if symbol in usdt_swap_symbols_set:
                open_interest_coins = ticker.get('info', {}).get('openInterest')
                price = ticker.get('last')

                if open_interest_coins and price:
                    open_interest_value = float(open_interest_coins) * float(price)
                    snapshot[symbol] = open_interest_value

        print(f"成功获取 {len(snapshot)} 个合约的快照。")
        return snapshot
    except Exception as e:
        print(f"获取市场数据时出错: {e}")
        return None


def save_snapshot(snapshot):
    """将快照数据和当前UTC时间戳保存到文件。"""
    data_to_save = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'snapshot': snapshot
    }
    with open(SNAPSHOT_FILE, 'w') as f:
        json.dump(data_to_save, f, indent=4)
    print(f"当前快照已成功保存到 {SNAPSHOT_FILE}")


def load_previous_snapshot():
    """从文件加载上一次保存的快照。"""
    try:
        with open(SNAPSHOT_FILE, 'r') as f:
            data = json.load(f)
            print(f"成功加载上一次的快照 (时间: {data['timestamp']})")
            return data
    except FileNotFoundError:
        print("未找到旧的快照文件。首次运行将只保存当前快照。")
        return None
    except Exception as e:
        print(f"加载快照文件时出错: {e}")
        return None


def compare_snapshots():
    """
    主函数：加载旧快照，获取新快照，进行对比并显示结果。
    """
    # 1. 初始化 ccxt
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    # (可选) 如果你需要代理，请取消下面这几行的注释
    # proxy_url = 'http://127.0.0.1:7890'
    # exchange.proxies = {'http': proxy_url, 'https': proxy_url}

    # 2. 加载上一次的快照
    previous_data = load_previous_snapshot()

    # 3. 获取当前的最新快照
    current_snapshot = get_current_snapshot(exchange)
    if not current_snapshot:
        return

    # 4. 如果没有旧快照（首次运行），则只保存新快照并退出
    if not previous_data:
        save_snapshot(current_snapshot)
        print("\n请在一小时或更长时间后再次运行此脚本以查看持仓量变化。")
        return

    # 5. 如果有旧快照，则进行对比
    previous_snapshot = previous_data['snapshot']
    changes = []

    for symbol, current_value in current_snapshot.items():
        if symbol in previous_snapshot:
            previous_value = previous_snapshot[symbol]
            if previous_value > 0:
                change_usd = current_value - previous_value
                change_percent = (change_usd / previous_value) * 100
                changes.append({
                    'symbol': symbol,
                    'prev_oi_usd': previous_value,
                    'curr_oi_usd': current_value,
                    'change_usd': change_usd,
                    'change_percent': change_percent
                })

    if not changes:
        print("没有可对比的数据。")
        save_snapshot(current_snapshot)  # 仍然用最新快照覆盖
        return

    # 6. 使用 Pandas 显示对比结果
    df = pd.DataFrame(changes)
    df_sorted = df.sort_values(by='change_percent', ascending=False).reset_index(drop=True)

    pd.set_option('display.float_format', '{:,.2f}'.format)
    pd.set_option('display.max_rows', 50)

    top_gainers = df_sorted.head(15)
    top_losers = df_sorted.tail(15).sort_values(by='change_percent', ascending=True)

    # 计算两次快照的时间差
    time_diff = datetime.fromisoformat(datetime.now(timezone.utc).isoformat()) - datetime.fromisoformat(
        previous_data['timestamp'])
    hours_diff = time_diff.total_seconds() / 3600

    print("\n" + "=" * 85)
    print(f"✅ 对比完成！时间间隔: {hours_diff:.2f} 小时")
    print(f"🚀 持仓量增长最多的 Top 15 合约 (按百分比排名):")
    print("-" * 85)
    print(top_gainers.to_string())

    print("\n" + "=" * 85)
    print(f"📉 持仓量减少最多的 Top 15 合约 (按百分比排名):")
    print("-" * 85)
    print(top_losers.to_string())
    print("=" * 85)

    # 7. 最后，用当前快照覆盖旧文件，为下一次运行做准备
    save_snapshot(current_snapshot)


if __name__ == "__main__":
    compare_snapshots()

