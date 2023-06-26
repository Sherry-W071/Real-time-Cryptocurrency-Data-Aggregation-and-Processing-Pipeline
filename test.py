import argparse
import requests

#https://www.binance.com/api/v1/klines?symbol=BTCUSDT&interval=1m&startTime=1523577600000&endTime=1523664000000

# parser = argparse.ArgumentParser()
# print(parser)
# parser.add_argument("--name", help="what is the name", required=True)
# parser.add_argument("--age", help="what is the name", required=True)
# args = parser.parse_args()
# print(args)
# name = args.name
# print("hello " + name)

#TODO SYMBOL, STARTIME, ENDTIME are all hard coded
#  we want to pass those parameters via CLI
#  https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
#  return list of object
#  create an AWS account
res = requests.get("https://www.binance.com/api/v1/klines?symbol=BTCUSDT&interval=1m&startTime=1523577600000&endTime=1523664000000")
# print(res.json())

parser = argparse.ArgumentParser()
parser.add_argument('--symbol', type=str)
parser.add_argument('--startTime', type=str)
parser.add_argument('--endTime', type=str)
args = parser.parse_args()

url = f"https://www.binance.com/api/v1/klines?symbol={args.symbol}&interval=1m&startTime={args.startTime}&endTime={args.endTime}"
res = requests.get(url)
print(res.json())

#python test.py --symbol BTCUSDT --startTime 1523577600000 --endTime 1523664000000

# Object
class BinanceData:
    def __init__(self, symbol, data):
        self.symbol = symbol
        self.open_time = data[0]
        self.open_price = data[1]
        self.high_price = data[2]
        self.low_price = data[3]
        self.close_price = data[4]
        self.volume = data[5]
        self.close_time = data[6]
        self.quote_asset_volume = data[7]
        self.number_of_trades = data[8]
        self.taker_buy_base_asset_volume = data[9]
        self.taker_buy_quote_asset_volume = data[10]
        self.unused = data[11]

data = res.json()
binance_data_objects = [BinanceData(sublist) for sublist in data]

for obj in binance_data_objects:
    print(obj.open_time)
