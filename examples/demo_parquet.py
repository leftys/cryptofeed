'''
Copyright (C) 2018-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed import FeedHandler
from cryptofeed.backends.parquet import BookParquet, CandlesParquet, FundingParquet, TickerParquet, TradeParquet, BookDeltaParquet, LiquidationsParquet, OpenInterestParquet
from cryptofeed.defines import CANDLES, FUNDING, L2_BOOK, TICKER, TRADES, OPEN_INTEREST, LIQUIDATIONS
from cryptofeed.exchanges import FTX, Binance, KuCoin, Gateio, BinanceFutures, Serum

def main():
    config = {
        'log': {'filename': 'demo.log', 'level': 'DEBUG', 'disabled': False},
        'kucoin': {
            'key_id': '607b082c4758710006703e87',
            'key_secret': 'e07a1c92-fffa-4442-95cd-f4bce550ec82',
            'key_passphrase': '147852369',
        },
    }
    f = FeedHandler(config=config)
    # f.add_feed(Bitmex(channels=[FUNDING, L2_BOOK], symbols=['BTC-USD-PERP'], callbacks={FUNDING: FundingParquet(host=Parquet_HOST, port=Parquet_PORT), L2_BOOK: BookParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    f.add_feed(Binance(channels=[TRADES, L2_BOOK, CANDLES], symbols=['BTC-USDT', 'SOL-USDT'], callbacks={TRADES: TradeParquet(), L2_BOOK: [BookDeltaParquet()], CANDLES: CandlesParquet()}, max_depth = 5000))
    # f.add_feed(Binance(channels=[TRADES, L2_BOOK, CANDLES], symbols=['BTC-USDT', 'ETH-BTC'], callbacks={TRADES: TradeParquet(), L2_BOOK: BookParquet(), CANDLES: CandlesParquet()}, max_depth = 20))
    # f.add_feed(Gateio(channels=[TRADES, L2_BOOK], symbols=['BTC-USDT', 'ETH-USDT'], callbacks={TRADES: TradeParquet(), L2_BOOK: [BookParquet()]}))
    # f.add_feed(Binance(channels=[TRADES], symbols=['BTC-USDT', 'ETH-USDT'], callbacks={TRADES: TradeParquet()}))
    # f.add_feed(FTX(channels=[TRADES, L2_BOOK], symbols=['ETH-USD', 'BTC-USD'], callbacks={TRADES: TradeParquet(), L2_BOOK: BookParquet()}, max_depth = 20))
    # f.add_feed(KuCoin(config = config, channels=[TRADES, L2_BOOK], symbols=['BTC-USDT', 'ETH-USDT'], callbacks={TRADES: TradeParquet(), L2_BOOK: BookParquet()}, max_depth = 20))
    # f.add_feed(KuCoin(channels=[TRADES], symbols=['BTC-USDT'], callbacks={TRADES: TradeParquet(), L2_BOOK: BookParquet()}, max_depth = 20))
    # f.add_feed(Coinbase(channels=[L2_BOOK], symbols=['BTC-USD'], callbacks={L2_BOOK: BookParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    # f.add_feed(Coinbase(channels=[TICKER], symbols=['BTC-USD'], callbacks={TICKER: TickerParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    # f.add_feed(Binance(candle_closed_only=False, channels=[CANDLES], symbols=['BTC-USDT'], callbacks={CANDLES: CandlesParquet(host=Parquet_HOST, port=Parquet_PORT)}))
    # f.add_feed(BinanceFutures(symbols=['BTC-USDT-PERP'], channels=[TRADES, L2_BOOK, OPEN_INTEREST, FUNDING, LIQUIDATIONS], callbacks={TRADES: TradeParquet(), OPEN_INTEREST: OpenInterestParquet(), FUNDING: FundingParquet(), LIQUIDATIONS: LiquidationsParquet(), L2_BOOK: BookParquet(max_depth = 20)}, max_depth = 20))
    # f.add_feed(Serum(symbols=['SOL-USDC', 'ETH-USDC'], channels=[TRADES, L2_BOOK], callbacks={TRADES: TradeParquet(key = 'trades_mpid'), L2_BOOK: BookParquet(max_depth = 20)}, max_depth = 20))
    f.run()


if __name__ == '__main__':
    main()
