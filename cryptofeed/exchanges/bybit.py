'''
Copyright (C) 2018-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import hmac
import time
from collections import defaultdict
from cryptofeed.symbols import Symbol, str_to_symbol
import logging
from decimal import Decimal
from typing import Dict, Tuple, Union
from datetime import datetime as dt

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, BUY, BYBIT, CANCELLED, CANCELLING, CANDLES, FAILED, FILLED, FUNDING, L2_BOOK, LIMIT, LIQUIDATIONS, MAKER, MARKET, OPEN, PARTIAL, SELL, SUBMITTING, TAKER, TRADES, OPEN_INTEREST, INDEX, ORDER_INFO, FILLS, FUTURES, PERPETUAL, SPOT
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Index, OpenInterest, Funding, OrderInfo, Fill, Candle, Liquidation


LOG = logging.getLogger('feedhandler')


class Bybit(Feed):
    id = BYBIT
    websocket_channels = {
        L2_BOOK: 'orderbook.50',
        TRADES: 'publicTrade',
        FILLS: 'execution',
        ORDER_INFO: 'order',
        INDEX: 'instrument_info.100ms',
        OPEN_INTEREST: 'instrument_info.100ms',
        FUNDING: 'instrument_info.100ms',
        CANDLES: 'kline',
        LIQUIDATIONS: 'liquidation'
    }
    websocket_endpoints = [
        WebsocketEndpoint('wss://stream.bybit.com/v5/public/linear', channel_filter=(websocket_channels[L2_BOOK], websocket_channels[TRADES], websocket_channels[INDEX], websocket_channels[OPEN_INTEREST], websocket_channels[FUNDING], websocket_channels[CANDLES], websocket_channels[LIQUIDATIONS]), instrument_filter=('QUOTE', ('USDT','USD')), sandbox='wss://stream-testnet.bybit.com/v5/public/linear', options={'compression': None}),
        # WebsocketEndpoint('wss://stream.bybit.com/realtime_public', channel_filter=(websocket_channels[L2_BOOK], websocket_channels[TRADES], websocket_channels[INDEX], websocket_channels[OPEN_INTEREST], websocket_channels[FUNDING], websocket_channels[CANDLES], websocket_channels[LIQUIDATIONS]), instrument_filter=('QUOTE', ('USDT',)), sandbox='wss://stream-testnet.bybit.com/realtime_public', options={'compression': None}),
        WebsocketEndpoint('wss://stream.bybit.com/v5/private', channel_filter=(websocket_channels[ORDER_INFO], websocket_channels[FILLS]), instrument_filter=('QUOTE', ('USDT',)), sandbox='wss://stream-testnet.bybit.com/v5/private', options={'compression': None}),
    ]
    rest_endpoints = [
        RestEndpoint('https://api.bybit.com', routes=Routes(['/v5/market/instruments-info?&category=linear&status=Trading&limit=1000', '/v5/market/instruments-info?&category=spot&status=Trading&limit=1000']))
    ]
    valid_candle_intervals = {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '1d', '1w', '1M'}
    candle_interval_map = {'1m': '1', '3m': '3', '5m': '5', '15m': '15', '30m': '30', '1h': '60', '2h': '120', '4h': '240', '6h': '360', '1d': 'D', '1w': 'W', '1M': 'M'}

    @classmethod
    def timestamp_normalize(cls, ts: Union[int, dt]) -> float:
        if isinstance(ts, int):
            return ts / 1000.0
        else:
            return ts.timestamp()

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for msg in data:
            if isinstance(msg['result'], dict):
                for symbol in msg['result']['list']:

                    if 'contractType' not in symbol:
                        stype = SPOT
                    elif 'contractType' in symbol:
                        if symbol['contractType'] == 'LinearPerpetual':
                            stype = PERPETUAL
                        elif symbol['contractType'] == 'LinearFutures':
                            stype = FUTURES

                    base = symbol['baseCoin']
                    quote = symbol['quoteCoin']

                    expiry = None

                    if stype is FUTURES:
                        if not symbol['symbol'].endswith(quote):
                            # linear futures
                            if '-' in symbol['symbol']:
                                expiry = symbol['symbol'].split('-')[-1]

                    s = Symbol(base, quote, type=stype, expiry_date=expiry)

                    # Bybit spot and USDT perps share the same symbol name, so
                    # here it is formed using the base and quote coins, separated
                    # by a slash. This is consistent with the UI.
                    # https://bybit-exchange.github.io/docs/v5/enum#symbol
                    if stype == SPOT:
                        ret[s.normalized] = f'{base}/{quote}'
                    elif stype == PERPETUAL and symbol['symbol'].endswith('PERP'):
                        ret[s.normalized] = symbol['symbol']
                    elif stype == PERPETUAL:
                        ret[s.normalized] = f'{base}{quote}'
                    elif stype == FUTURES:
                        ret[s.normalized] = symbol['symbol']

                    info['tick_size'][s.normalized] = Decimal(symbol['priceFilter']['tickSize'])
                    info['instrument_type'][s.normalized] = stype

        return ret, info

    def __reset(self, conn: AsyncConnection):
        if self.std_channel_to_exchange(L2_BOOK) in conn.subscription:
            for pair in conn.subscription[self.std_channel_to_exchange(L2_BOOK)]:
                std_pair = self.exchange_symbol_to_std_symbol(pair)

                if std_pair in self._l2_book:
                    del self._l2_book[std_pair]

        self._instrument_info_cache = {}

    async def _candle(self, msg: dict, timestamp: float):
        '''
        {
            "topic": "klineV2.1.BTCUSD",                //topic name
            "data": [{
                "start": 1572425640,                    //start time of the candle
                "end": 1572425700,                      //end time of the candle
                "open": 9200,                           //open price
                "close": 9202.5,                        //close price
                "high": 9202.5,                         //max price
                "low": 9196,                            //min price
                "volume": 81790,                        //volume
                "turnover": 8.889247899999999,          //turnover
                "confirm": False,                       //snapshot flag (indicates if candle is closed or not)
                "cross_seq": 297503466,
                "timestamp": 1572425676958323           //cross time
            }],
            "timestamp_e6": 1572425677047994            //server time
        }
        '''
        symbol = self.exchange_symbol_to_std_symbol(msg['topic'].split(".")[-1])
        ts = msg['ts'] / 1_000

        for entry in msg['data']:
            if self.candle_closed_only and not entry['confirm']:
                continue
            c = Candle(self.id,
                       symbol,
                       entry['start'],
                       entry['end'],
                       self.candle_interval,
                       entry['confirm'],
                       Decimal(entry['open']),
                       Decimal(entry['close']),
                       Decimal(entry['high']),
                       Decimal(entry['low']),
                       Decimal(entry['volume']),
                       None,
                       ts,
                       raw=entry)
            await self.callback(CANDLES, c, timestamp)

    async def _liquidation(self, msg: dict, timestamp: float):
        '''
        {
            'topic': 'liquidation.EOSUSDT',
            'data': {
                'symbol': 'EOSUSDT',
                'side': 'Buy',
                'price': '3.950',
                'qty': '58.0',
                'time': 1632586154956
            }
        }
        '''
        liq = Liquidation(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['data']['symbol']),
            BUY if msg['data']['side'] == 'Buy' else SELL,
            Decimal(msg['data']['size']),
            Decimal(msg['data']['price']),
            None,
            None,
            self.timestamp_normalize(msg['data']['updatedTime']),
            raw=msg
        )
        await self.callback(LIQUIDATIONS, liq, timestamp)

    async def message_handler(self, msg: str, conn, timestamp: float):

        msg = json.loads(msg, parse_float=Decimal)

        try:
            if "success" in msg:
                if msg['success']:
                    # LOG.info("Success from exchange %s", msg)
                    if msg['op'] == 'auth':
                        LOG.debug("%s: Authenticated successful", conn.uuid)
                    elif msg['op'] == 'subscribe':
                        LOG.debug("%s: Subscribed", conn.uuid)
                    else:
                        LOG.warning("%s: Unhandled 'successs' message received", conn.uuid)
                else:
                    LOG.error("%s: Error from exchange %s", conn.uuid, msg)
            elif msg["topic"].startswith('publicTrade'):
                await self._trade(msg, timestamp)
            elif msg["topic"].startswith('orderbook'):
                await self._book(msg, timestamp)
            elif msg['topic'].startswith('liquidation'):
                await self._liquidation(msg, timestamp)
            elif "instrument_info" in msg["topic"]:
                await self._instrument_info(msg, timestamp)
            elif "order" in msg["topic"]:
                await self._order(msg, timestamp)
            elif "execution" in msg["topic"]:
                await self._execution(msg, timestamp)
            elif 'kline' in msg['topic'] or 'candle' in msg['topic']:
                await self._candle(msg, timestamp)
            # elif "position" in msg["topic"]:
            #     await self._balances(msg, timestamp)
            else:
                LOG.warning("%s: Unhandled message type %s", conn.uuid, msg)
        except Exception:
            LOG.exception('Weird message %s', msg)

    async def subscribe(self, connection: AsyncConnection):
        self.__reset(connection)
        for chan in connection.subscription:
            if not self.is_authenticated_channel(self.exchange_channel_to_std(chan)):
                for pair in connection.subscription[chan]:
                    sym = str_to_symbol(self.exchange_symbol_to_std_symbol(pair))

                    if self.exchange_channel_to_std(chan) == CANDLES:
                        c = chan if sym.quote in ('USDT', 'USD') else 'kline'
                        sub = [f"{c}.{self.candle_interval_map[self.candle_interval]}.{pair}"]
                    else:
                        sub = [f"{chan}.{pair}"]

                    LOG.info('Subscribing to public channel = %s', sub)
                    await connection.write(json.dumps({"op": "subscribe", "args": sub}))
            else:
                await connection.write(json.dumps(
                    {
                        "op": "subscribe",
                        "args": [f"{chan}"]
                    }
                ))

    async def _instrument_info(self, msg: dict, timestamp: float):
        """
        ### Snapshot type update
        {
        "topic": "instrument_info.100ms.BTCUSD",
        "type": "snapshot",
        "data": {
            "id": 1,
            "symbol": "BTCUSD",                           //instrument name
            "last_price_e4": 81165000,                    //the latest price
            "last_tick_direction": "ZeroPlusTick",        //the direction of last tick:PlusTick,ZeroPlusTick,MinusTick,
                                                          //ZeroMinusTick
            "prev_price_24h_e4": 81585000,                //the price of prev 24h
            "price_24h_pcnt_e6": -5148,                   //the current last price percentage change from prev 24h price
            "high_price_24h_e4": 82900000,                //the highest price of prev 24h
            "low_price_24h_e4": 79655000,                 //the lowest price of prev 24h
            "prev_price_1h_e4": 81395000,                 //the price of prev 1h
            "price_1h_pcnt_e6": -2825,                    //the current last price percentage change from prev 1h price
            "mark_price_e4": 81178500,                    //mark price
            "index_price_e4": 81172800,                   //index price
            "open_interest": 154418471,                   //open interest quantity - Attention, the update is not
                                                          //immediate - slowest update is 1 minute
            "open_value_e8": 1997561103030,               //open value quantity - Attention, the update is not
                                                          //immediate - the slowest update is 1 minute
            "total_turnover_e8": 2029370141961401,        //total turnover
            "turnover_24h_e8": 9072939873591,             //24h turnover
            "total_volume": 175654418740,                 //total volume
            "volume_24h": 735865248,                      //24h volume
            "funding_rate_e6": 100,                       //funding rate
            "predicted_funding_rate_e6": 100,             //predicted funding rate
            "cross_seq": 1053192577,                      //sequence
            "created_at": "2018-11-14T16:33:26Z",
            "updated_at": "2020-01-12T18:25:16Z",
            "next_funding_time": "2020-01-13T00:00:00Z",  //next funding time
                                                          //the rest time to settle funding fee
            "countdown_hour": 6                           //the remaining time to settle the funding fee
        },
        "cross_seq": 1053192634,
        "timestamp_e6": 1578853524091081                  //the timestamp when this information was produced
        }

        ### Delta type update
        {
        "topic": "instrument_info.100ms.BTCUSD",
        "type": "delta",
        "data": {
            "delete": [],
            "update": [
                {
                    "id": 1,
                    "symbol": "BTCUSD",
                    "prev_price_24h_e4": 81565000,
                    "price_24h_pcnt_e6": -4904,
                    "open_value_e8": 2000479681106,
                    "total_turnover_e8": 2029370495672976,
                    "turnover_24h_e8": 9066215468687,
                    "volume_24h": 735316391,
                    "cross_seq": 1053192657,
                    "created_at": "2018-11-14T16:33:26Z",
                    "updated_at": "2020-01-12T18:25:25Z"
                }
            ],
            "insert": []
        },
        "cross_seq": 1053192657,
        "timestamp_e6": 1578853525691123
        }
        """
        update_type = msg['type']

        if update_type == 'snapshot':
            updates = [msg['data']]
        else:
            updates = msg['data']['update']

        for info in updates:
            if msg['topic'] in self._instrument_info_cache and self._instrument_info_cache[msg['topic']] == updates:
                continue
            else:
                self._instrument_info_cache[msg['topic']] = updates

            ts = int(msg['timestamp_e6']) / 1_000_000

            if 'open_interest' in info:
                oi = OpenInterest(
                    self.id,
                    self.exchange_symbol_to_std_symbol(info['symbol']),
                    Decimal(info['open_interest']),
                    ts,
                    raw=info
                )
                await self.callback(OPEN_INTEREST, oi, timestamp)

            if 'index_price_e4' in info:
                i = Index(
                    self.id,
                    self.exchange_symbol_to_std_symbol(info['symbol']),
                    Decimal(info['index_price_e4']) * Decimal('1e-4'),
                    ts,
                    raw=info
                )
                await self.callback(INDEX, i, timestamp)

            if 'funding_rate_e6' in info:
                f = Funding(
                    self.id,
                    self.exchange_symbol_to_std_symbol(info['symbol']),
                    None,
                    Decimal(info['funding_rate_e6']) * Decimal('1e-6'),
                    info['next_funding_time'].timestamp() if 'next_funding_time' in info else None,
                    ts,
                    predicted_rate=Decimal(info['predicted_funding_rate_e6']) * Decimal('1e-6'),
                    raw=info
                )
                await self.callback(FUNDING, f, timestamp)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {"topic":"trade.BTCUSD",
        "data":[
            {
                "timestamp":"2019-01-22T15:04:33.461Z",
                "symbol":"BTCUSD",
                "side":"Buy",
                "size":980,
                "price":3563.5,
                "tick_direction":"PlusTick",
                "trade_id":"9d229f26-09a8-42f8-aff3-0ba047b0449d",
                "cross_seq":163261271}]}
        """
        data = msg['data']
        for trade in data:
            if isinstance(trade['T'], str):
                ts = int(trade['T'])
            else:
                ts = trade['T']

            t = Trade(
                self.id,
                self.exchange_symbol_to_std_symbol(trade['s']),
                BUY if trade['S'] == 'Buy' else SELL,
                Decimal(trade['v']),
                Decimal(trade['p']),
                self.timestamp_normalize(ts),
                id=trade['i'],
                raw=trade
            )
            await self.callback(TRADES, t, timestamp)

    async def _book(self, msg: dict, timestamp: float):
        pair = self.exchange_symbol_to_std_symbol(msg['topic'].split('.')[-1])
        update_type = msg['type']
        data = msg['data']
        delta = {BID: [], ASK: []}

        if update_type == 'snapshot':
            delta = None
            self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth)
            # the USDT perpetual data is under the order_book key
            # if 'order_book' in data:
            #     data = data['order_book']
            seq_no = data['seq']
            for side, side_str in ((BID, 'b'), (ASK, 'a')):
                side_data = data[side_str]
                for update in side_data:
                    self._l2_book[pair].book[side][Decimal(update[0])] = Decimal(update[1])
        else:
            seq_no = data['seq']
            for side, side_str in ((BID, 'b'), (ASK, 'a')):
                side_data = data[side_str]
                for level in side_data:
                    price = Decimal(level[0])
                    amount = Decimal(level[1])
                    if amount == 0:
                        delta[side].append((price, 0))
                        del self._l2_book[pair].book[side][price]
                    else:
                        delta[side].append((price, amount))
                        self._l2_book[pair].book[side][price] = amount

        # timestamp is in ms
        ts = msg['ts']
        if isinstance(ts, str):
            ts = int(ts)
        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=ts / 1000, sequence_number=seq_no, raw=data, delta=delta)

    async def _order(self, msg: dict, timestamp: float):
        """
        {
            "topic": "order",
            "action": "",
            "data": [
                {
                    "order_id": "xxxxxxxx-xxxx-xxxx-9a8f-4a973eb5c418",
                    "order_link_id": "",
                    "symbol": "BTCUSDT",
                    "side": "Buy",
                    "order_type": "Limit",
                    "price": 11000,
                    "qty": 0.001,
                    "leaves_qty": 0.001,
                    "last_exec_price": 0,
                    "cum_exec_qty": 0,
                    "cum_exec_value": 0,
                    "cum_exec_fee": 0,
                    "time_in_force": "GoodTillCancel",
                    "create_type": "CreateByUser",
                    "cancel_type": "UNKNOWN",
                    "order_status": "New",
                    "take_profit": 0,
                    "stop_loss": 0,
                    "trailing_stop": 0,
                    "reduce_only": false,
                    "close_on_trigger": false,
                    "create_time": "2020-08-12T21:18:40.780039678Z",
                    "update_time": "2020-08-12T21:18:40.787986415Z"
                }
            ]
        }
        """
        order_status = {
            'Created': SUBMITTING,
            'Rejected': FAILED,
            'New': OPEN,
            'PartiallyFilled': PARTIAL,
            'Filled': FILLED,
            'Cancelled': CANCELLED,
            'PendingCancel': CANCELLING
        }

        for i in range(len(msg['data'])):
            data = msg['data'][i]

            oi = OrderInfo(
                self.id,
                self.exchange_symbol_to_std_symbol(data['symbol']),
                data["order_id"],
                BUY if data["side"] == 'Buy' else SELL,
                order_status[data["order_status"]],
                LIMIT if data['order_type'] == 'Limit' else MARKET,
                Decimal(data['price']),
                Decimal(data['qty']),
                Decimal(data['qty']) - Decimal(data['cum_exec_qty']),
                self.timestamp_normalize(data.get('update_time') or data.get('O') or data.get('timestamp')),
                raw=data,
            )
            await self.callback(ORDER_INFO, oi, timestamp)

    async def _execution(self, msg: dict, timestamp: float):
        '''
        {
            "topic": "execution",
            "data": [
                {
                    "symbol": "BTCUSD",
                    "side": "Buy",
                    "order_id": "xxxxxxxx-xxxx-xxxx-9a8f-4a973eb5c418",
                    "exec_id": "xxxxxxxx-xxxx-xxxx-8b66-c3d2fcd352f6",
                    "order_link_id": "",
                    "price": "8300",
                    "order_qty": 1,
                    "exec_type": "Trade",
                    "exec_qty": 1,
                    "exec_fee": "0.00000009",
                    "leaves_qty": 0,
                    "is_maker": false,
                    "trade_time": "2020-01-14T14:07:23.629Z" // trade time
                }
            ]
        }
        '''
        for entry in msg['data']:
            symbol = self.exchange_symbol_to_std_symbol(entry['symbol'])
            f = Fill(
                self.id,
                symbol,
                BUY if entry['side'] == 'Buy' else SELL,
                Decimal(entry['exec_qty']),
                Decimal(entry['price']),
                Decimal(entry['exec_fee']),
                entry['exec_id'],
                entry['order_id'],
                None,
                MAKER if entry['is_maker'] else TAKER,
                entry['trade_time'].timestamp(),
                raw=entry
            )
            await self.callback(FILLS, f, timestamp)

    async def authenticate(self, conn: AsyncConnection):
        if any(self.is_authenticated_channel(self.exchange_channel_to_std(chan)) for chan in conn.subscription):
            auth = self._auth(self.key_id, self.key_secret)
            LOG.debug(f"{conn.uuid}: Sending authentication request with message {auth}")
            await conn.write(auth)

    def _auth(self, key_id: str, key_secret: str) -> str:
        # https://bybit-exchange.github.io/docs/inverse/#t-websocketauthentication

        expires = int((time.time() + 60)) * 1000
        signature = str(hmac.new(bytes(key_secret, 'utf-8'), bytes(f'GET/realtime{expires}', 'utf-8'), digestmod='sha256').hexdigest())
        return json.dumps({'op': 'auth', 'args': [key_id, expires, signature]})
