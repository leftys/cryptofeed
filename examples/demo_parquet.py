'''
Copyright (C) 2018-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed import FeedHandler
from cryptofeed.backends.parquet import BookParquet, CandlesParquet, FundingParquet, TickerParquet, TradeParquet, BookDeltaParquet
from cryptofeed.defines import CANDLES, FUNDING, L2_BOOK, TICKER, TRADES
from cryptofeed.exchanges import FTX, Binance

S3_PATH = 's3://qnt.data/market-data/cryptofeed/'

def main():
    config = {'log': {'filename': 'demo.log', 'level': 'DEBUG', 'disabled': False}}
    f = FeedHandler(config=config)
    # f.add_feed(Bitmex(channels=[FUNDING, L2_BOOK], symbols=['BTC-USD-PERP'], callbacks={FUNDING: FundingParquet(host=Parquet_HOST, port=Parquet_PORT), L2_BOOK: BookParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    # f.add_feed(Binance(channels=[TRADES, L2_BOOK, CANDLES], symbols=['BTC-USDT', 'ETH-BTC'], callbacks={TRADES: TradeParquet(path = S3_PATH), L2_BOOK: [BookParquet(path = S3_PATH), BookDeltaParquet(path = S3_PATH)], CANDLES: CandlesParquet(path = S3_PATH)}, max_depth = 20))
    f.add_feed(Binance(channels=[TRADES, L2_BOOK, CANDLES], symbols=['BTC-USDT', 'ETH-BTC'], callbacks={TRADES: TradeParquet(path = S3_PATH), L2_BOOK: BookParquet(path = S3_PATH), CANDLES: CandlesParquet(path = S3_PATH)}, max_depth = 20))
    f.add_feed(FTX(channels=[TRADES, L2_BOOK], symbols=['ETH-USD'], callbacks={TRADES: TradeParquet(path = S3_PATH), L2_BOOK: BookParquet(path = S3_PATH)}, max_depth = 20))
    # f.add_feed(Coinbase(channels=[L2_BOOK], symbols=['BTC-USD'], callbacks={L2_BOOK: BookParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    # f.add_feed(Coinbase(channels=[TICKER], symbols=['BTC-USD'], callbacks={TICKER: TickerParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    # f.add_feed(Binance(candle_closed_only=False, channels=[CANDLES], symbols=['BTC-USDT'], callbacks={CANDLES: CandlesParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    f.run()


if __name__ == '__main__':
    main()
