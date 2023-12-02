'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from asyncio import create_task, sleep
from collections import defaultdict
from decimal import Decimal
import requests
import time
from typing import Dict, Union, Tuple
from urllib.parse import urlencode

from yapic import json

from cryptofeed.connection import AsyncConnection, HTTPPoll, HTTPConcurrentPoll, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import ASK, BALANCES, BID, COINMATE, BUY, CANDLES, FUNDING, FUTURES, L2_BOOK, LIMIT, LIQUIDATIONS, MARKET, OPEN_INTEREST, ORDER_INFO, PERPETUAL, SELL, SPOT, TICKER, TRADES, FILLED, UNFILLED
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.types import Trade, Ticker, Candle, Liquidation, Funding, OrderBook, OrderInfo, Balance

REFRESH_SNAPSHOT_MIN_INTERVAL_SECONDS = 60

LOG = logging.getLogger('feedhandler')


class Coinmate(Feed):
    id = COINMATE
    websocket_endpoints = [
        WebsocketEndpoint(
            'wss://coinmate.io/api/websocket/channel',
            options = {'ping_interval': None, 'ping_timeout': None}
        )
    ]
    rest_endpoints = [RestEndpoint('https://coinmate.io', routes=Routes(
        '/api/tradingPairs',
        l2book='/api/orderBook?currencyPair={}&groupByPriceLimit=false&Limit={}',
        authentication='/api/v3/userDataStream'
    ))]

    valid_depths = [5, 10, 20, 50, 100, 500, 1000, 5000]
    # m -> minutes; h -> hours; d -> days; w -> weeks; M -> months
    # valid_candle_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'}
    # valid_depth_intervals = {'100ms', '1000ms'}
    websocket_channels = {
        L2_BOOK: 'order-book',
        TRADES: 'trades',
        # TICKER: 'bookTicker',
        # CANDLES: 'kline_',
        BALANCES: BALANCES,
        ORDER_INFO: ORDER_INFO
    }
    request_limit = 20

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)
        for symbol in data['data']:
            expiration = None
            stype = SPOT
            s = Symbol(symbol['firstCurrency'], symbol['secondCurrency'], type=stype, expiry_date=expiration)
            ret[s.normalized] = symbol['name']
            info['tick_size'][s.normalized] = 10**-int(symbol['priceDecimals'])
            info['instrument_type'][s.normalized] = stype
        return ret, info

    def __init__(self, depth_interval='100ms', **kwargs):
        """
        depth_interval: str
            time between l2_book/delta updates {'100ms', '1000ms'} (different from Coinmate_FUTURES & Coinmate_DELIVERY)
        """
        # if depth_interval is not None and depth_interval not in self.valid_depth_intervals:
            # raise ValueError(f"Depth interval must be one of {self.valid_depth_intervals}")

        super().__init__(**kwargs)
        # self.depth_interval = depth_interval
        self._open_interest_cache = {}
        self._reset()

    def _address(self) -> Union[str, Dict]:
        if self.requires_authentication:
            raise NotImplementedError("Coinmate does not support authenticated feeds")
        else:
            address = self.address
            address += '/'
        subs = []

        is_any_private = any(self.is_authenticated_channel(chan) for chan in self.subscription)
        is_any_public = any(not self.is_authenticated_channel(chan) for chan in self.subscription)
        if is_any_private and is_any_public:
            raise ValueError("Private channels should be subscribed in separate feeds vs public channels")
        if all(self.is_authenticated_channel(chan) for chan in self.subscription):
            return address

        for chan in self.subscription:
            normalized_chan = self.exchange_channel_to_std(chan)
            if self.is_authenticated_channel(normalized_chan):
                continue

            stream = chan

            for pair in self.subscription[chan]:
                subs.append(f"{stream}/{pair}")

        def split_list(_list: list, n: int):
            for i in range(0, len(_list), n):
                yield _list[i:i + n]

        return [address + '/'.join(chunk) for chunk in split_list(subs, 1)]

    def _reset(self):
        self._l2_book = {}
        self.last_update_id = {}

    async def _trade(self, msg: dict, pair: str, timestamp: float):
        t = Trade(self.id,
                  self.exchange_symbol_to_std_symbol(pair),
                  SELL if msg['type'] == 'SELL' else BUY,
                  Decimal(msg['amount']),
                  Decimal(msg['price']),
                  self.timestamp_normalize(msg['date']),
                  id=str(msg['buyOrderId']) + str(msg['sellOrderId']),
                  raw=msg)
        await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, pair: str) -> None:
        max_depth = self.max_depth if self.max_depth else self.valid_depths[-1]
        if max_depth not in self.valid_depths:
            for d in self.valid_depths:
                if d > max_depth:
                    max_depth = d
                    break

        url = self.rest_endpoints[0].route('l2book', self.sandbox).format(pair, max_depth)
        resp = await self.http_conn.read(url)
        resp = json.loads(resp, parse_float=Decimal)
        timestamp = resp['data']['timestamp']
        assert not resp['error'], f"Error in snapshot: {resp} from {url}"

        std_pair = self.exchange_symbol_to_std_symbol(pair)
        self.last_update_id[std_pair] = 0 #resp['lastUpdateId']
        self._l2_book[std_pair] = OrderBook(self.id, std_pair, max_depth=self.max_depth,
            bids={Decimal(u['price']): Decimal(u['amount']) for u in resp['data']['bids']},
            asks={Decimal(u['price']): Decimal(u['amount']) for u in resp['data']['asks']}
        )
        await self.book_callback(L2_BOOK, self._l2_book[std_pair], time.time(), timestamp=timestamp, raw=resp['data'], sequence_number=self.last_update_id[std_pair])

    async def _book(self, msg: dict, pair: str, timestamp: float):
        exchange_pair = pair
        pair = self.exchange_symbol_to_std_symbol(pair)

        if pair not in self._l2_book:
            await self._snapshot(exchange_pair)

        # Coinmate only sends snapshots
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth,
            bids={Decimal(u['price']): Decimal(u['amount']) for u in msg['bids']},
            asks={Decimal(u['price']): Decimal(u['amount']) for u in msg['asks']}
        )

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=timestamp, raw=msg, delta=None, sequence_number=self.last_update_id[pair])

    async def message_handler(self, msg: str, conn, timestamp: float):
        try:
            msg = json.loads(msg, parse_float=Decimal)

            # Handle account updates from User Data Stream
            if self.requires_authentication:
                raise NotImplementedError("Coinmate does not support authenticated feeds")
            # print(conn, msg)
            if msg['event'] == 'ping':
                # await conn.write(json.dumps({'event': 'pong'}))
                LOG.debug('Ping received on %s', conn.id)
                return
            channel, pair = msg['channel'].split('-', 1)
            payload = msg['payload']
            if channel == 'order_book':
                await self._book(payload, pair, timestamp)
            elif channel == 'trades':
                for trade in payload:
                    await self._trade(trade, pair, timestamp)
            else:
                LOG.warning("%s: Unexpected message received: %s", self.id, msg)
        except Exception:
            LOG.warning("%s: Exception in parsing message %s", self.id, msg, exc_info=True)

    async def subscribe(self, conn: AsyncConnection):
        # Coinmate does not have a separate subscribe message, the
        # subscription information is included in the
        # connection endpoint
        if isinstance(conn, (HTTPPoll, HTTPConcurrentPoll)):
            self._open_interest_cache = {}
        else:
            self._reset()
        if self.requires_authentication:
            create_task(self._refresh_token())
