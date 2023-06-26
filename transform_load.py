import argparse
from typing import List
import config
import mysql.connector
from collections import OrderedDict

from binance_data import BinanceData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def load(data: List['BinanceData']):
    # db connection
    # db batch insert new table
    # TODO: cnx
    # TODO organize it using bulk_insert_mappings
    # TODO: learn with clause

    cnx = mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.passwd,
        database="binance_transformed_data")

    cursor = cnx.cursor()

    # Define insert statement
    insert_statement = (
        "INSERT INTO binance_transformed_data (symbol, open_time, close_time, open_price, close_price, low_price, high_price, volume, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, unused) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    # Prepare the values to be inserted
    data_values = [(d.symbol, d.open_time, d.close_time, d.open_price, d.close_price, d.low_price, d.high_price, d.volume, d.quote_asset_volume, d.number_of_trades, d.taker_buy_base_asset_volume, d.taker_buy_quote_asset_volume, d.unused) for d in data]

    # Execute the batch insert
    cursor.executemany(insert_statement, data_values)

    cnx.commit()
    cursor.close()
    cnx.close()

    # engine = create_engine('mysql+mysqlconnector://username:password@localhost/dbname')
    # Session = sessionmaker(bind=engine)
    # session = Session()
    # for d in data:
    #     session.add(d)
    # session.commit()
    # session.close()

    # engine = create_engine('mysql+mysqlconnector://username:password@localhost/dbname')
    # Session = sessionmaker(bind=engine)
    # session = Session()
    #
    # try:
    #     # Prepare data for batch insert
    #     insert_data = [
    #         {
    #             'symbol': obj.symbol,
    #             'open_time': obj.open_time,
    #             'open_price': obj.open_price,
    #             'high_price': obj.high_price,
    #             'low_price': obj.low_price,
    #             'close_price': obj.close_price,
    #             'volume': obj.volume,
    #             'close_time': obj.close_time,
    #             'quote_asset_volume': obj.quote_asset_volume,
    #             'number_of_trades': obj.number_of_trades,
    #             'taker_buy_base_asset_volume': obj.taker_buy_base_asset_volume,
    #             'taker_buy_quote_asset_volume': obj.taker_buy_quote_asset_volume,
    #             'unused': obj.unused
    #         }
    #         for obj in data
    #     ]
    #
    #     # Perform batch insert
    #     session.bulk_insert_mappings(binance_data, insert_data)
    #     session.commit()
    # except:
    #     session.rollback()
    #     raise
    # finally:
    #     session.close()


def transform(data: List['BinanceData'], interval:int) ->  List['BinanceData']:
    # interval combine, e.g.: combine five 1min data into one 5min data
    # str -> double
    if interval <= 0:
        raise ValueError() #TODO
    transformed_data = []
    for i in range(0, len(data), interval):
        chunk = data[i:min(i+interval, len(data))]
        first = chunk[0]
        last = chunk[-1]
        low_price = high_price = first.low_price
        volume = quote_asset_volume = number_of_trades = taker_buy_base_asset_volume = taker_buy_quote_asset_volume = 0

        for d in chunk:
            low_price = min(low_price, d.low_price)
            high_price = max(high_price, d.high_price)
            volume += d.volume
            quote_asset_volume += d.quote_asset_volume
            number_of_trades += d.number_of_trades
            taker_buy_base_asset_volume += d.taker_buy_base_asset_volume
            taker_buy_quote_asset_volume += d.taker_buy_quote_asset_volume

        combined = BinanceData(
            symbol=first.symbol,
            open_time=first.open_time,
            close_time=last.close_time,
            open_price=first.open_price,
            close_price=last.close_price,
            low_price=low_price,
            high_price=high_price,
            volume=volume,
            quote_asset_volume=quote_asset_volume,
            number_of_trades=number_of_trades,
            taker_buy_base_asset_volume=taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume=taker_buy_quote_asset_volume,
            unused=last.unused
        )
        combined.open_price = first.open_price
        transformed_data.append(combined)
    return transformed_data


def get_in_time_range(symbol, startTime: int, endTime: int, limit=500) -> List['BinanceData']:
    '''
    Get a list of BinanceData from the database within a specified time range.
    :param start_time: The start of the time range (unix time).
    :param end_time: The end of the time range (unix time).
    :return: a list of BinanceData
    '''
    # session = Session()

    cnx = mysql.connector.connect(
        host=config.host,
        user=config.user,
        password=config.passwd,
        database="binance_source_data")

    cursor = cnx.cursor(dictionary=True)

    offset = 0
    data = []
    # while True:
        # try:
        #     # Query the database for BinanceData objects within the specified time range
        #     chunk = session.query(BinanceData).filter(BinanceData.open_time >= startTime,
        #                                               BinanceData.open_time <= endTime) \
        #         .order_by(BinanceData.open_time) \
        #         .offset(offset) \
        #         .limit(limit) \
        #         .all()
        # finally:
        #     session.close()
    query = ("SELECT * FROM binance_data "
             "WHERE open_time >= %s AND open_time <= %s AND symbol = %s "
             "ORDER BY open_time ASC "
             "LIMIT %s OFFSET %s")

    while True:
        cursor.execute(query, (startTime, endTime, symbol, limit, offset))
        chunk = cursor.fetchall()

        if not chunk:
            break

        for row in chunk:
            binance_data_dict = OrderedDict([
                ('symbol', row['symbol']),
                ('open_time', row['open_time']),
                ('open_price', row['open_price']),
                ('high_price', row['high_price']),
                ('low_price', row['low_price']),
                ('close_price', row['close_price']),
                ('volume', row['volume']),
                ('close_time', row['close_time']),
                ('quote_asset_volume', row['quote_asset_volume']),
                ('number_of_trades', row['number_of_trades']),
                ('taker_buy_base_asset_volume', row['taker_buy_base_asset_volume']),
                ('taker_buy_quote_asset_volume', row['taker_buy_quote_asset_volume']),
                ('unused', row['unused'])
            ])

            binance_data_obj = BinanceData(**binance_data_dict)
            data.append(binance_data_obj)

        offset += limit

    cnx.close()

    return data

    #     if not chunk:
    #         break
    #
    #     data.extend(chunk)
    #     offset += limit
    #
    # return data

    # try:
    #     # Query the database for BinanceData objects within the specified time range
    #     data = session.query(BinanceData).filter(BinanceData.open_time >= startTime,
    #                                              BinanceData.open_time <= endTime).all()

    # finally:
    #     session.close()
    #
    # return data

def transform_job(symbol, startTime, endTime, interval):
    # TODO: complete this logic
    # get data from database List of binance data
    # for-loop/while-loop
    # 1.1000
    # 2.1000
    data = get_in_time_range(symbol, startTime, endTime)

    transformed_data = transform(data, interval)
    load(transformed_data)

    # for i in range(0, len(data), 1000):
    #     chunk = data[i:i + 1000]  # TODO min(i+1000, len(data))
    #     transformed_data = transform(chunk, interval)
    #     load(transformed_data)

    # resListOfData = transform(listOfData, interval)
    # load(resListOfData)
    # pass


def main():
    args = parse_args()
    symbol, startTime, endTime, interval = args.symbol, int(args.startTime), int(args.endTime), int(args.interval)

    if startTime >= endTime:
        raise ValueError(f"startTime returns trades from that time and endTime returns trades starting from the endTime"
                         f"including all trades before that time. startTime must be smaller than endTime")

    # if endTime - startTime > 500mins, 1D = 24*60 = 1440mins
    transform_job(symbol=symbol, startTime=startTime, endTime=endTime, interval=interval)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', type=str)
    parser.add_argument('--startTime', type=int)
    parser.add_argument('--endTime', type=int)
    parser.add_argument('--interval', type=int)
    return parser.parse_args()


if __name__ == '__main__':
    main()