class BinanceData:
    def __init__(self, symbol, open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume, unused): # TODO
        self.symbol = symbol
        self.open_time = open_time
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.volume = volume
        self.close_time = close_time
        self.quote_asset_volume = quote_asset_volume
        self.number_of_trades = number_of_trades
        self.taker_buy_base_asset_volume = taker_buy_base_asset_volume
        self.taker_buy_quote_asset_volume = taker_buy_quote_asset_volume
        self.unused = unused

    def __str__(self):
        return f'BinanceData:symbol={self.symbol},open_time={self.open_time},open_price={self.open_price},high_price={self.high_price},low_price={self.low_price},close_price={self.close_price},volume={self.volume},close_time={self.close_time},quote_asset_volume={self.quote_asset_volume},number_of_trades={self.number_of_trades},taker_buy_base_asset_volume={self.taker_buy_base_asset_volume},taker_buy_quote_asset_volume={self.taker_buy_quote_asset_volume},unused={self.unused}'