import ccxt
import time
import asyncio
from datetime import datetime
import pandas as pd
import os

class BinanceMonitor:
    def __init__(self):
        self.exchange = ccxt.binance({
            'sandbox': False,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot'
            }
        })
        proxy_url = 'http://127.0.0.1:7890'
        self.exchange.proxies = {
            'http': proxy_url,
            'https': proxy_url,
        }
        
    async def get_top_100_pairs(self):
        try:
            self.exchange.load_markets()
            tickers = self.exchange.fetch_tickers()
            
            usdt_pairs = []
            for symbol, ticker in tickers.items():
                if symbol.endswith('/USDT') and ticker['last'] and ticker['quoteVolume']:
                    # 计算市值 = 价格 × 流通量(用交易量作为近似)
                    market_cap = ticker['last'] * ticker['baseVolume'] if ticker['baseVolume'] else 0
                    if market_cap > 0:
                        usdt_pairs.append({
                            'symbol': symbol,
                            'volume': ticker['quoteVolume'],
                            'price': ticker['last'],
                            'market_cap': market_cap
                        })
            
            # 按市值排序
            sorted_pairs = sorted(usdt_pairs, key=lambda x: x['market_cap'], reverse=True)
            return sorted_pairs[:100]
        except Exception as e:
            print(f"获取交易对数据时出错: {e}")
            return []
    
    def get_5min_price_change(self, symbol):
        try:
            # 获取最近的5分钟K线数据 (最近2根5分钟K线)
            klines = self.exchange.fetch_ohlcv(symbol, '5m', limit=2)
            if len(klines) < 2:
                return 0
            
            # 获取5分钟前的开盘价和当前价格
            five_min_ago_price = klines[0][1]  # 5分钟前的开盘价
            current_price = klines[-1][4]      # 最新的收盘价
            
            if five_min_ago_price and current_price:
                change_percent = ((current_price - five_min_ago_price) / five_min_ago_price) * 100
                return change_percent
            return 0
        except Exception as e:
            print(f"获取 {symbol} K线数据出错: {e}")
            return 0
    
    async def monitor_markets(self):
        print("开始监控币安市值前100交易对...")
        print("=" * 80)
        
        while True:
            try:
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print(f"币安市值前100交易对 5分钟涨跌监控 (按市值排名)")
                print(f"更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 80)
                
                top_pairs = await self.get_top_100_pairs()
                if not top_pairs:
                    print("无法获取交易对数据，5秒后重试...")
                    await asyncio.sleep(5)
                    continue
                
                changes = []
                for pair in top_pairs:
                    symbol = pair['symbol']
                    price = pair['price']
                    change = self.get_5min_price_change(symbol)
                    
                    changes.append({
                        'symbol': symbol,
                        'price': price,
                        'change': change,
                        'volume': pair['volume'],
                        'market_cap': pair['market_cap']
                    })
                
                # 按涨跌幅排序所有数据
                changes.sort(key=lambda x: x['change'], reverse=True)
                
                # 分离正负变化
                positive_changes = [c for c in changes if c['change'] > 0]
                negative_changes = [c for c in changes if c['change'] < 0]
                zero_changes = [c for c in changes if c['change'] == 0]
                
                print(f"数据统计: 上涨{len(positive_changes)}个, 下跌{len(negative_changes)}个, 无变化{len(zero_changes)}个")
                print()
                
                print("🚀 5分钟涨幅最大的前10个交易对 (按市值前100排名):")
                print("-" * 80)
                display_gains = positive_changes[:10] if positive_changes else changes[:10]
                if not display_gains:
                    print("暂无涨幅数据 (首次运行需要等待5分钟建立价格历史)")
                else:
                    for i, item in enumerate(display_gains):
                        if item['change'] >= 0:
                            print(f"{i+1:2d}. {item['symbol']:<15} "
                                  f"价格: ${item['price']:<12.6f} "
                                  f"涨幅: +{item['change']:<6.2f}% "
                                  f"市值: ${item['market_cap']:>15,.0f}")
                        else:
                            print(f"{i+1:2d}. {item['symbol']:<15} "
                                  f"价格: ${item['price']:<12.6f} "
                                  f"变化: {item['change']:<7.2f}% "
                                  f"市值: ${item['market_cap']:>15,.0f}")
                
                print("\n📉 5分钟跌幅最大的前10个交易对 (按市值前100排名):")
                print("-" * 80)
                display_losses = negative_changes[-10:] if negative_changes else changes[-10:]
                if not display_losses:
                    print("暂无跌幅数据 (首次运行需要等待5分钟建立价格历史)")
                else:
                    display_losses.reverse()
                    for i, item in enumerate(display_losses):
                        if item['change'] <= 0:
                            print(f"{i+1:2d}. {item['symbol']:<15} "
                                  f"价格: ${item['price']:<12.6f} "
                                  f"跌幅: {item['change']:<7.2f}% "
                                  f"市值: ${item['market_cap']:>15,.0f}")
                        else:
                            print(f"{i+1:2d}. {item['symbol']:<15} "
                                  f"价格: ${item['price']:<12.6f} "
                                  f"变化: +{item['change']:<6.2f}% "
                                  f"市值: ${item['market_cap']:>15,.0f}")
                
                print("\n" + "=" * 80)
                print("30秒后自动刷新... (按 Ctrl+C 停止监控)")
                
                await asyncio.sleep(30)
                
            except KeyboardInterrupt:
                print("\n监控已停止")
                break
            except Exception as e:
                print(f"监控过程中出错: {e}")
                await asyncio.sleep(10)

async def main():
    monitor = BinanceMonitor()
    await monitor.monitor_markets()

if __name__ == "__main__":
    asyncio.run(main())