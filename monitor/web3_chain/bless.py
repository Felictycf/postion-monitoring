from web3 import Web3
from web3._utils.events import get_event_data
import json, time
from collections import deque

# -------- RPC (HTTP) --------
BSC_HTTP = "https://rpc.ankr.com/bsc/635e22937933250f5438ec849f91d7dc8bea865edeaef6bad094edcc42fde5c3"
w3 = Web3(Web3.HTTPProvider(BSC_HTTP))

# -------- 地址（务必完整！）--------
# ✅ 推荐使用这个官方池地址（和你截图一致）
POOL_ADDRESS = Web3.to_checksum_address("0x49986efbeEdEa3Ab962CA95caF86919860ECc9DB")
# 你刚才贴的这个地址 `0x49986e...60ECc9DB` 不是同一个，建议改回上面这个
BLESS_TOKEN  = Web3.to_checksum_address("0x7C8217517ed4711fe2DECCdFefFE8d906b9Ae11F")

# -------- V3池 ABI（只留我们要的）--------
V3_POOL_ABI = json.loads(r'''
[
  {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
  {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
  {"anonymous":false,"inputs":[
    {"indexed":true,"internalType":"address","name":"sender","type":"address"},
    {"indexed":true,"internalType":"address","name":"recipient","type":"address"},
    {"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},
    {"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},
    {"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},
    {"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},
    {"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],
   "name":"Swap","type":"event"}
]
''')

pool = w3.eth.contract(address=POOL_ADDRESS, abi=V3_POOL_ABI)
swap_event_abi = [e for e in pool.events.__dict__['_events'] if e['name']=='Swap'][0]

# ✅ 用 HexBytes，不要 .hex() 转成字符串
SWAP_TOPIC = w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)")

print("Connected:", w3.is_connected(), "Block:", w3.eth.block_number)

WINDOW = 50
recent = deque(maxlen=WINDOW); streak_b = streak_s = 0; net_bless = 0

def classify(args):
    global streak_b, streak_s, net_bless
    a0 = int(args['amount0'])              # BLESS 是 token0
    side = "BUY" if a0 > 0 else ("SELL" if a0 < 0 else "FLAT")
    amt  = abs(a0)
    recent.append(side)
    if side == "BUY":
        streak_b = (streak_b + 1) if (len(recent) >= 2 and recent[-2] == "BUY") else 1
        streak_s = 0; net_bless += amt
    elif side == "SELL":
        streak_s = (streak_s + 1) if (len(recent) >= 2 and recent[-2] == "SELL") else 1
        streak_b = 0; net_bless -= amt
    buy_ratio = recent.count("BUY") / len(recent) if recent else 0
    print(f"[V3][{side}] amt={amt} | buy_ratio(last{WINDOW})={buy_ratio:.2f} | streak(B={streak_b},S={streak_s}) | net={net_bless}")

# -------- 轮询（把区块号当“整数”传入 get_logs）--------
print("Polling every 3 sec...")
latest_checked = max(0, w3.eth.block_number - 1)

while True:
    try:
        new_block = w3.eth.block_number
        if new_block > latest_checked:
            # ✅ 关键点：fromBlock / toBlock 传 “int”，让 web3 自己转 hex
            logs = w3.eth.get_logs({
                "fromBlock": latest_checked + 1,   # int
                "toBlock":   new_block,            # int
                "address":   POOL_ADDRESS,         # ChecksumAddress
                "topics":   [SWAP_TOPIC]           # HexBytes
            })
            for lg in logs:
                decoded = get_event_data(w3.codec, swap_event_abi, lg)
                classify(decoded['args'])
            latest_checked = new_block
        time.sleep(3)
    except Exception as e:
        print("Error:", e)
        time.sleep(5)
