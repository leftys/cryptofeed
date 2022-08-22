'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed import FeedHandler
from cryptofeed.defines import COINBASE, TRADES, L2_BOOK
from cryptofeed.exchanges import Coinbase
from cryptofeed.backends.quest import BookQuest, TradeQuest


async def trade(feed, symbol, order_id, timestamp, side, amount, price, receipt_timestamp, order_type):
    pass


async def book(feed, symbol, book, timestamp, receipt_timestamp):
    pass


def main():
    config = {'log': {'filename': 'demo.log', 'level': 'INFO'}}
    f = FeedHandler(config=config)
    kwargs = {'host': '172.17.0.2', 'port':9009}
    QUEST_HOST = '127.17.0.2'
    QUEST_PORT = 9009
    # f.add_feed(COINBASE, subscription={L2_BOOK: ['BTC-USD', 'ETH-USD']}, callbacks={L2_BOOK: BookQuest(**kwargs)})
    f.add_feed(Coinbase(channels=[L2_BOOK], symbols=['BTC-USD'], callbacks={L2_BOOK: BookQuest(host=QUEST_HOST, port=QUEST_PORT)}))

    f.run()


if __name__ == '__main__':
    import cProfile
    cProfile.run('main()', sort='cumulative')
