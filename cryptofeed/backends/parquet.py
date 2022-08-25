import logging
import collections

from cryptofeed.backends.backend import BackendCallback, BackendQueue
from cryptofeed.util.dumper import Dumper


LOG = logging.getLogger('feedhandler')


class ParquetCallback(BackendQueue):
    def __init__(self, path: str, key=None, **kwargs):
        self.path = path
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
                        dumper = self._dumpers[data['exchange']][data['symbol']] = Dumper(self.path, data['symbol'], self.key, data['exchange'])
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __call__(self, book, receipt_timestamp: float):
        # TODO only save every 100ms or snapshot_interval or so?
        data = {}
        data['symbol'] = book.symbol
        data['exchange'] = book.exchange
        data['timestamp'] = int(book.timestamp * 1_000_000_000) if book.timestamp else 0
        data["receipt_timestamp"] = int(receipt_timestamp * 1_000_000_000)
        data['sequence_number'] = book.sequence_number
        for side_name, side in (('bid', book.book.bids), ('ask', book.book.asks)):
            for i in range(book.book.max_depth):
                try:
                    # TODO: this is slow. update order-book library, cythonize this loop, or both
                    level = side.index(i)
                except KeyError:
                    data[f'{side_name}_{i}_price'] = float('nan')
                    data[f'{side_name}_{i}_size'] = float('nan')
                else:
                    data[f'{side_name}_{i}_price'] = float(level[0])
                    data[f'{side_name}_{i}_size'] = float(level[1])
        # print(book.exchange, book.delta)
        await self.queue.put(data)

class BookDeltaParquet(ParquetCallback):
    default_key = 'book-delta'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def __call__(self, book, receipt_timestamp: float):
        data = {}
        data['symbol'] = book.symbol
        data['exchange'] = book.exchange
        data['timestamp'] = int(book.timestamp * 1_000_000_000) if book.timestamp else 0
        data["receipt_timestamp"] = int(receipt_timestamp * 1_000_000_000)
        data['sequence_number'] = book.sequence_number
        for side_name in ('bid', 'ask'):
            for update in book.delta[side_name]:
                data[f'{side_name}_price'] = float(update[0])
                data[f'{side_name}_size'] = float(update[1])
        print(book.exchange, book.delta)
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
