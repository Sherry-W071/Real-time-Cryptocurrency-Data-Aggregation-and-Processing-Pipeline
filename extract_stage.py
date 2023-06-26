import argparse
import requests
import config
import mysql.connector
from typing import List

from binance_data import BinanceData

# TODO: resume 1. ETL job 2. transform 3. batch insert reduce insertion time 4. Git organize code (unit test)

# TODO  code no red line and can insert data from CLI
# TODO create a github repo
# TODO create the REQUIREMENT.TXT
# TODO .gitignore *.idea/* venv/* *.iml
# TODO git commands
#  git clone url -> github public url
#  git checkout -b <name> -> master branch
#  git add -A -> stage all unstaged file
#  git commit -m "<message>" -> commit staged file to local repository
#  git push

# TODO git actions (CI/CD)
# TODO unit test + mock unit test
#  _convert, etc.


def extract_from_binance(symbol: str, startTime: int, endTime: int, limit:int) -> List['BinanceData']:
    '''
    Get a list of binance data from binance data source
    :param symbol: coin symbol, quote-base, for example BTCUSDT
    :param startTime: unix time
    :param endTime: unix time
    :return: a list of BinanceData
    '''
    url = config.base_url % (symbol, startTime, endTime, limit)
    response = requests.get(url)
    data = response.json()
    return [_convert(symbol, sublist) for sublist in data]

# TODO complete
def _convert(symbol, sublist):
    data = BinanceData(symbol,
                       open_time=sublist[0],
                       open_price=sublist[1],
                       high_price=sublist[2],
                       low_price=sublist[3],
                       close_price=sublist[4],
                       volume=sublist[5],
                       close_time=sublist[6],
                       quote_asset_volume=sublist[7],
                       number_of_trades=sublist[8],
                       taker_buy_base_asset_volume=sublist[9],
                       taker_buy_quote_asset_volume=sublist[10],
                       unused=sublist[11])

    return data


def stage(data: List['BinanceData']) -> None:
    '''
    save a list of binance data into database
    :param data: a list of binance data
    :return: None
    '''
    cnx = mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.passwd,
        database="binance_source_data")

    cursor = cnx.cursor()
    insert_statement = (
        "INSERT INTO binance_data (symbol, open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, unused) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    data_values = [(obj.symbol, obj.open_time, obj.open_price, obj.high_price, obj.low_price, obj.close_price,
                    obj.volume, obj.close_time, obj.quote_asset_volume, obj.number_of_trades,
                    obj.taker_buy_base_asset_volume, obj.taker_buy_quote_asset_volume, obj.unused) for obj in data]

    cursor.executemany(insert_statement, data_values)

    cnx.commit()
    cursor.close()
    cnx.close()


def load(symbol: str, startTime: int, endTime: int):
    # for-loop/while-loop
    # 1. first 500
    # 2. second 500
    # ...
    #  last <=500

    limit = 500
    interval_duration = limit * 60 * 1000

    current_time = startTime

    while current_time < endTime:
        next_time = min(current_time + interval_duration, endTime)

        print(f"Loading data from {current_time} to {next_time}")
        data: List['BinanceData'] = extract_from_binance(symbol=symbol, startTime=current_time, endTime=next_time, limit=limit)
        print('finished loading the data')
        stage(data)
        print('finished staging the data')

        current_time = next_time


def main():
    args = parse_args()
    symbol, startTime, endTime = args.symbol, int(args.startTime), int(args.endTime)

    if startTime >= endTime:
        raise ValueError(f"startTime returns trades from that time and endTime returns trades starting from the endTime"
                         f"including all trades before that time. startTime must be smaller than endTime")

    load(symbol=symbol, startTime=startTime, endTime=endTime)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', type=str)
    parser.add_argument('--startTime', type=int)
    parser.add_argument('--endTime', type=int)
    return parser.parse_args()


if __name__ == '__main__':
    print(__name__)
    main() # call invoke trigger
