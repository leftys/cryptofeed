import logging
from collections import defaultdict
from decimal import Decimal
import datetime
import time
from typing import Dict, Union, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import ASK,  BID, SERUM, BUY, L2_BOOK,SELL, SPOT, TRADES
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.types import TradeMPID, OrderBook

REFRESH_SNAPSHOT_MIN_INTERVAL_SECONDS = 60

LOG = logging.getLogger('feedhandler')


class Serum(Feed):
    id = SERUM
    websocket_endpoints = [WebsocketEndpoint('wss://vial.mngo.cloud/v1/ws')]
    rest_endpoints = [RestEndpoint('https://vial.mngo.cloud', routes=Routes('/v1/markets'))]

    websocket_channels = {
        L2_BOOK: 'level2',
        TRADES: 'trades',

    }
    request_limit = 20

    @classmethod
    def timestamp_normalize(cls, ts: datetime.datetime) -> float:
        return ts.timestamp()

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)
        for symbol in data:
            if symbol.get('deprecated'):
                continue

            expiration = None
            stype = SPOT

            s = Symbol(symbol['baseCurrency'], symbol['quoteCurrency'], type=stype, expiry_date=expiration)
            ret[s.normalized] = symbol['name']
            info['tick_size'][s.normalized] = symbol['tickSize']
            info['instrument_type'][s.normalized] = stype
        return ret, info

    def __init__(self, max_depth = 1000, **kwargs):
        super().__init__(**kwargs)
        self._open_interest_cache = {}
        self.max_depth = max_depth
        self._reset()

    def _address(self) -> Union[str, Dict]:
        return self.address

    def _reset(self):
        self._l2_book = {}
        self.last_update_id = {}

    async def _trade(self, msg: dict, timestamp: float):
        t = TradeMPID(self.id,
                  self.exchange_symbol_to_std_symbol(msg['market']),
                  SELL if msg['side'] == 'sell' else BUY,
                  Decimal(msg['size']),
                  Decimal(msg['price']),
                  self.timestamp_normalize(msg['timestamp']),
                  id='0',
                  takerAccount = msg['takerAccount'],
                  makerAccount = msg['makerAccount'],
                  takerOrderId = msg['takerOrderId'],
                  makerOrderId = msg['makerOrderId'],
                  raw=msg)
        await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, resp: Dict) -> None:

        timestamp = resp['timestamp'].timestamp()

        std_pair = self.exchange_symbol_to_std_symbol(resp['market'])
        self.last_update_id[std_pair] = resp['slot'] # ???
        self._l2_book[std_pair] = OrderBook(self.id, std_pair, max_depth=self.max_depth, bids={Decimal(u[0]): Decimal(u[1]) for u in resp['bids']}, asks={Decimal(u[0]): Decimal(u[1]) for u in resp['asks']})
        await self.book_callback(L2_BOOK, self._l2_book[std_pair], time.time(), timestamp=timestamp, raw=resp, sequence_number=self.last_update_id[std_pair])

    async def _book(self, msg: dict, timestamp: float):
        pair = self.exchange_symbol_to_std_symbol(msg['market'])

        delta = {BID: [], ASK: []}

        for s, side in (('bids', BID), ('asks', ASK)):
            for update in msg[s]:
                price = Decimal(update[0])
                amount = Decimal(update[1])
                delta[side].append((price, amount))

                if amount == 0:
                    if price in self._l2_book[pair].book[side]:
                        del self._l2_book[pair].book[side][price]
                else:
                    self._l2_book[pair].book[side][price] = amount

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=msg['timestamp'].timestamp(), raw=msg, delta=delta, sequence_number=self.last_update_id[pair])

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        # LOG.info('Msg = %s', msg)

        msg_type = msg['type']
        if msg_type == 'l2update':
            await self._book(msg, timestamp)
        elif msg_type == 'trade':
            await self._trade(msg, timestamp)
        elif msg_type == 'recent_trades':
            for trade in msg['trades']:
                await self._trade(trade, trade['timestamp'].timestamp())
        elif msg_type == 'subscribed':
            pass
        elif msg_type == 'l2snapshot':
            await self._snapshot(msg)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self._reset()
        print(self.subscription)

        for chan in self.subscription:
            await conn.write(json.dumps({"op": "subscribe", "channel": chan, "markets": self.subscription[chan]}))

