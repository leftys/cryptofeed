import logging
import collections

from cryptofeed.backends.backend import BackendCallback, BackendQueue
from cryptofeed.util.dumper import Dumper


LOG = logging.getLogger('feedhandler')


class ParquetCallback(BackendQueue):
    def __init__(self, key=None, **kwargs):
        self.key = key if key else self.default_key
        self.numeric_type = float
        self.none_to = None
        self.running = True
        self._dumpers: dict[str, dict[str, Dumper]] = collections.defaultdict(dict)
        ''' keys: exchange, symbol '''

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                # print(self.key, len(updates))
                for data in updates:
                    try:
                        dumper = self._dumpers[data['exchange']][data['symbol']]
                    except KeyError:
                        # TODO data['exchange']?
                        # TODO upload to s3
                        dumper = self._dumpers[data['exchange']][data['symbol']] = Dumper(data['symbol'], self.key, data['exchange'])
                    del data['symbol']
                    del data['exchange']
                    dumper.dump(data)
            if not updates:
                break

    @staticmethod
    def _format_timestamps(data):
        data["receipt_timestamp"] = int(data["receipt_timestamp"] * 1_000_000_000)
        data["timestamp"] = int(data["timestamp"] * 1_000_000_000) if data['timestamp'] is not None else None
        return data

    async def write(self, data):
        await self.queue.put(self._format_timestamps(data))

    async def stop(self):
        for exchange_dumpers in self._dumpers.values():
            for dumper in exchange_dumpers.values():
                dumper.close()
        await super().stop()


class TradeParquet(ParquetCallback, BackendCallback):
    default_key = 'trades'

    async def write(self, data):
        # Parquet dumper cannot handle Nones
        if data['type'] is None:
            del data['type']
        # TODO trade id can be str or int on exchanges and strs got converted to float
        await self.queue.put(self._format_timestamps(data))

class FundingParquet(ParquetCallback, BackendCallback):
    default_key = 'funding'


class BookParquet(ParquetCallback):
    default_key = 'book'

    def __init__(self, max_depth = 10, snapshot_interval_s = 0.1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_depth = max_depth
        self.snapshot_interval_ns = snapshot_interval_s * 1_000_000_000
        self.last_book = None
        self.last_receipt_timestamp = 0
        self._saved = 0
        self._dropped = 0
        self._postponed = 0
        self._last_postponed = None

    async def __call__(self, book, receipt_timestamp: float):
        data = {}
        data['symbol'] = book.symbol
        data['exchange'] = book.exchange
        data['timestamp'] = int(book.timestamp * 1_000_000_000) if book.timestamp else 0
        data["receipt_timestamp"] = int(receipt_timestamp * 1_000_000_000)

        if self._last_postponed:
            since_postponed = data['receipt_timestamp'] - self._last_postponed['receipt_timestamp']
            if since_postponed > self.snapshot_interval_ns / 2:
                await self.queue.put(self._last_postponed)
                self._saved += 1
                self._last_receipt_timestamp = self._last_postponed['receipt_timestamp']
                self._last_postponed = None

        within_snapshot_interval = data['receipt_timestamp'] - self.last_receipt_timestamp < self.snapshot_interval_ns
        within_quarter_snapshot_interval = data['receipt_timestamp'] - self.last_receipt_timestamp < self.snapshot_interval_ns // 4
        if within_quarter_snapshot_interval:
            # Drop ultra high frequency snapshots immediately
            self._dropped += 1
            return

        data['sequence_number'] = book.sequence_number
        for side_name, side in (('bid', book.book.bids), ('ask', book.book.asks)):
            depth = -1
            for depth, (price, size) in enumerate(side.to_list(self.max_depth)):
                data[f'{side_name}_{depth}_price'] = float(price)
                data[f'{side_name}_{depth}_size'] = float(size)
            for i in range(depth+1, self.max_depth):
                # Those levels are not present
                data[f'{side_name}_{i}_price'] = float('nan')
                data[f'{side_name}_{i}_size'] = float('nan')

        if not within_snapshot_interval:
            # print(book.exchange, book.delta)
            await self.queue.put(data)
            self._saved += 1
            self.last_receipt_timestamp = data['receipt_timestamp']
        else:
            self._postponed += 1
            self._last_postponed = data

    # async def stop(self):
    #     await super().stop()
    #     print(self._saved)
    #     print(self._dropped)
    #     print(self._postponed)

class BookDeltaParquet(ParquetCallback):
    default_key = 'book_delta_v2'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __call__(self, book, receipt_timestamp: float):
        data = {}
        data['symbol'] = book.symbol
        data['exchange'] = book.exchange
        data['timestamp'] = int(book.timestamp * 1_000_000_000) if book.timestamp else 0
        data["receipt_timestamp"] = int(receipt_timestamp * 1_000_000_000)
        data['sequence_number'] = book.sequence_number
        if 'result' in book.raw:
            # gateio
            raw = book.raw['result']
        elif 'changes' in book.raw:
            raw = book.raw['changes']
        else:
            raw = book.raw
        try:
            data["bids"] = raw['b']
            data["asks"] = raw['a']
        except KeyError:
            # Snapshot:
            data["bids"] = raw['bids']
            data["asks"] = raw['asks']
        await self.queue.put(data)


class TickerParquet(ParquetCallback, BackendCallback):
    default_key = 'ticker'


class OpenInterestParquet(ParquetCallback, BackendCallback):
    default_key = 'open_interest'


class LiquidationsParquet(ParquetCallback, BackendCallback):
    default_key = 'liquidations'


class CandlesParquet(ParquetCallback, BackendCallback):
    default_key = 'candles'

    async def write(self, data):
        del data['interval']
        del data['closed']
        # data['start'] = int(data['start'])
        # data['stop'] = int(data['stop'])
        await self.queue.put(self._format_timestamps(data))


class OrderInfoParquet(ParquetCallback, BackendCallback):
    default_key = 'order_info'


class TransactionsParquet(ParquetCallback, BackendCallback):
    default_key = 'transactions'


class BalancesParquet(ParquetCallback, BackendCallback):
    default_key = 'balances'


class FillsParquet(ParquetCallback, BackendCallback):
    default_key = 'fills'
