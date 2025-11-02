import requests
import time
import json
from datetime import datetime, timedelta
import os
from typing import List, Dict, Any
from collections import defaultdict, deque

class CoinGeckoMonitor:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Python/3.x CoinGecko Monitor'
        })
        
        # 存储历史价格数据用于计算短期涨跌幅
        self.price_history = defaultdict(lambda: deque(maxlen=3600))  # 保存最近1小时的数据(每分钟一个点)
        self.last_update = None
        self.start_time = datetime.now()  # 记录程序启动时间
        
    def get_market_data(self, vs_currency='usd', per_page=100, page=1) -> List[Dict[Any, Any]]:
        """
        获取市场数据，按市值排序
        """
        url = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': per_page,
            'page': page,
            'sparkline': False,
            'price_change_percentage': '1h,24h,7d,14d,30d,1y'
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # 存储当前价格到历史记录
            current_time = datetime.now()
            for coin in data:
                coin_id = coin['id']
                price = coin.get('current_price', 0)
                self.price_history[coin_id].append({
                    'timestamp': current_time,
                    'price': price
                })
            
            self.last_update = current_time
            return data
            
        except requests.RequestException as e:
            print(f"API请求错误: {e}")
            return []
    
    def format_percentage(self, value: float) -> str:
        """
        格式化百分比显示
        """
        if value is None:
            return "N/A"
        
        color_code = ""
        if value > 0:
            color_code = "\033[92m"  # 绿色
            symbol = "+"
        elif value < 0:
            color_code = "\033[91m"  # 红色
            symbol = ""
        else:
            color_code = "\033[37m"  # 白色
            symbol = ""
        
        return f"{color_code}{symbol}{value:.2f}%\033[0m"
    
    def calculate_price_change(self, coin_id: str, minutes_ago: int) -> float:
        """
        计算指定分钟数前的价格变化百分比
        """
        if coin_id not in self.price_history:
            return None
            
        history = self.price_history[coin_id]
        if len(history) < max(2, minutes_ago + 1):  # 需要足够的历史数据点
            return None
            
        current_price = history[-1]['price']
        
        # 根据分钟数找到对应的历史价格点
        # 由于我们每分钟存储一次数据，可以通过索引直接访问
        if minutes_ago >= len(history):
            return None
            
        # 从最新数据点向前查找指定分钟数的数据点
        target_index = -1 - minutes_ago
        if abs(target_index) > len(history):
            return None
            
        old_record = history[target_index]
        old_price = old_record['price']
        
        # 检查时间间隔是否合理（允许一些误差）
        time_diff = abs((history[-1]['timestamp'] - old_record['timestamp']).total_seconds())
        expected_diff = minutes_ago * 60
        if abs(time_diff - expected_diff) > 120:  # 允许2分钟误差
            return None
        
        if old_price == 0 or current_price == 0:
            return None
            
        change_percent = ((current_price - old_price) / old_price) * 100
        
        # 过滤掉异常的变化值（可能是数据错误）
        if abs(change_percent) > 50:  # 短期内变化超过50%认为异常
            return None
            
        return change_percent
    
    def display_gainers_losers(self, data: List[Dict[Any, Any]]):
        """
        显示涨幅和跌幅榜单
        """
        # 为每个币种添加短期涨跌幅数据
        for coin in data:
            coin_id = coin['id']
            coin['change_1m'] = self.calculate_price_change(coin_id, 1)
            coin['change_3m'] = self.calculate_price_change(coin_id, 3) 
            coin['change_5m'] = self.calculate_price_change(coin_id, 5)
            coin['change_15m'] = self.calculate_price_change(coin_id, 15)
            coin['change_30m'] = self.calculate_price_change(coin_id, 30)
            coin['change_60m'] = self.calculate_price_change(coin_id, 60)
        
        timeframes = {
            'change_1m': '1分钟',
            'change_3m': '3分钟', 
            'change_5m': '5分钟',
            'change_15m': '15分钟',
            'change_30m': '30分钟',
            'change_60m': '60分钟'
        }
        
        print("\n" + "="*120)
        print(f"{'':^120}")
        print(f"{'CoinGecko 实时加密货币短期涨跌幅监控 - 市值前100':^120}")
        print(f"{'更新时间: ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'):^120}")
        running_time = datetime.now() - self.start_time
        print(f"{'运行时长: ' + str(running_time).split('.')[0]:^120}")
        
        # 显示数据点数量
        if self.price_history:
            avg_data_points = sum(len(hist) for hist in self.price_history.values()) // len(self.price_history)
            print(f"{'历史数据点: ' + str(avg_data_points) + ' 个':^120}")
        print(f"{'':^120}")
        print("="*120)
        
        for timeframe_key, timeframe_name in timeframes.items():
            minutes = int(timeframe_key.split('_')[1].replace('m', ''))
            print(f"\n🔥 【{timeframe_name}涨幅榜 TOP 10】")
            print("-" * 80)
            
            # 按涨幅排序 - 过滤掉没有数据的币种
            valid_coins = [coin for coin in data if coin.get(timeframe_key) is not None]
            if not valid_coins:
                avg_data_points = sum(len(hist) for hist in self.price_history.values()) // len(self.price_history) if self.price_history else 0
                print(f"暂无数据 - 需要至少{minutes + 1}个数据点 (当前平均: {avg_data_points})")
                continue
                
            sorted_gainers = sorted(
                valid_coins,
                key=lambda x: x.get(timeframe_key, 0),
                reverse=True
            )[:10]
            
            print(f"{'排名':<4} {'币种':<15} {'价格(USD)':<12} {'市值排名':<8} {'涨幅':<12}")
            print("-" * 80)
            
            for i, coin in enumerate(sorted_gainers, 1):
                price = coin.get('current_price', 0)
                market_cap_rank = coin.get('market_cap_rank', 'N/A')
                change = coin.get(timeframe_key, 0)
                
                print(f"{i:<4} {coin['symbol'].upper():<15} ${price:<11.6f} #{market_cap_rank:<7} {self.format_percentage(change)}")
            
            print(f"\n📉 【{timeframe_name}跌幅榜 TOP 10】")
            print("-" * 80)
            
            # 按跌幅排序
            sorted_losers = sorted(
                valid_coins,
                key=lambda x: x.get(timeframe_key, 0),
                reverse=False
            )[:10]
            
            print(f"{'排名':<4} {'币种':<15} {'价格(USD)':<12} {'市值排名':<8} {'跌幅':<12}")
            print("-" * 80)
            
            for i, coin in enumerate(sorted_losers, 1):
                price = coin.get('current_price', 0)
                market_cap_rank = coin.get('market_cap_rank', 'N/A')
                change = coin.get(timeframe_key, 0)
                
                print(f"{i:<4} {coin['symbol'].upper():<15} ${price:<11.6f} #{market_cap_rank:<7} {self.format_percentage(change)}")
            
            print()
    
    def run_monitor(self):
        """
        运行监控程序，每分钟更新一次
        """
        print("🚀 CoinGecko 短期涨跌幅监控程序启动...")
        print("📊 正在获取市值前100的加密货币数据...")
        print("⏱️  监控时间段: 1分钟、3分钟、5分钟、15分钟、30分钟、60分钟")
        print("⏰ 每60秒自动更新一次数据")
        print("💡 注意: 程序需要运行一段时间才能积累足够的价格历史数据")
        print("\n按 Ctrl+C 退出程序")
        
        try:
            while True:
                # 清屏
                os.system('cls' if os.name == 'nt' else 'clear')
                
                # 获取数据
                market_data = self.get_market_data()
                
                if market_data:
                    self.display_gainers_losers(market_data)
                    print(f"\n⏰ 下次更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (60秒后)")
                else:
                    print("❌ 获取数据失败，请检查网络连接或API状态")
                
                # 等待60秒
                time.sleep(60)
                
        except KeyboardInterrupt:
            print("\n\n👋 程序已停止运行")
        except Exception as e:
            print(f"\n❌ 程序运行错误: {e}")

def main():
    monitor = CoinGeckoMonitor()
    monitor.run_monitor()

if __name__ == "__main__":
    main()