import ccxt
import asyncio
from datetime import datetime

async def test_binance_connection():
    try:
        print("测试币安连接...")
        exchange = ccxt.binance()
        proxy_url = 'http://127.0.0.1:7890'
        exchange.proxies = {
            'http': proxy_url,
            'https': proxy_url,
        }

        
        print("加载市场数据...")
        markets = exchange.load_markets()
        print(f"成功加载 {len(markets)} 个市场")
        
        print("获取ticker数据...")
        tickers = exchange.fetch_tickers()
        print(f"成功获取 {len(tickers)} 个ticker")
        
        usdt_pairs = []
        for symbol, ticker in tickers.items():
            if symbol.endswith('/USDT') and ticker['quoteVolume']:
                usdt_pairs.append({
                    'symbol': symbol,
                    'volume': ticker['quoteVolume'],
                    'price': ticker['last']
                })
        
        print(f"找到 {len(usdt_pairs)} 个USDT交易对")
        
        sorted_pairs = sorted(usdt_pairs, key=lambda x: x['volume'], reverse=True)
        top_10 = sorted_pairs[:10]
        
        print("\n市值前10的USDT交易对:")
        print("-" * 60)
        for i, pair in enumerate(top_10):
            print(f"{i+1:2d}. {pair['symbol']:<15} "
                  f"价格: ${pair['price']:<12.6f} "
                  f"成交量: ${pair['volume']:>15,.0f}")
        
        print("\n✅ 币安API连接测试成功!")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_binance_connection())