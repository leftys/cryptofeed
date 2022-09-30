from typing import Dict, Optional, Any

import datetime
import os
import os.path
import threading
import logging
import random
import gc

import pyarrow as pa
import pyarrow.parquet as pq


class Dumper:
	'''
	Dump flat dictionaries with fixed set of keys into binary parquet file.

	Keeps dataframe buffer of by-default 1_000 rows and appends the to open parquet file every time the buffer is full.

	In case the file exists at open time, reads the file and prepends those existing data to a new file, as parquet
	format doesn't supports appending to already finished files.

	Also parquet doesn't support flushing and reading unfinished file, so we have to exist cleanly and call `close()`
	for the resulting file to be readable.
	'''

	# TODO: remove path (s3)
	def __init__(self, path: str, symbol: str, event_type: str, exchange: str, buffer_len: int = 500) -> None:
		self.path = path
		self.symbol = symbol
		self.event_type = event_type
		self.exchange = exchange
		self._store: Optional[pq.ParquetWriter] = None
		self._store_date = datetime.date.today()
		self._column_data: Dict[str, Any] = {}
		self._schema: Optional[pa.Schema] = None
		self.buffer_max_len = buffer_len
		self._buffer_position = 0
		self._row_group_size = buffer_len
		self._data_lock = threading.Lock()
		self._terminating = False
		# Custom loggers dont work due to some bug in cryptofeed
		# self._logger = logging.getLogger(f'Dumper({self.symbol}@{self.event_type})')
		self._logger = logging.getLogger('feedhandler')

	def dump(self, msg: Dict) -> None:
		date = datetime.date.today()
		if date != self._store_date:
			self._flush()
			self._store.close()
			self._store = None

		with self._data_lock:
			if not self._column_data:
				schema_fields = []
				for name, value in msg.items():
					self._column_data[name] = []
					if type(value) == int and value < 1e9:
						t = pa.int64()  # TODO: can we safely use int32 with smarter type detection or explicitly?
					elif type(value) == int:
						t = pa.int64()
					elif type(value) == float:
						t = pa.float64()
					elif type(value) == str and self._is_int(value):
						t = pa.int64()
					elif type(value) == str and self._is_hex(value):
						t = pa.binary(len(value))
					elif type(value) == str and self._is_float(value):
						t = pa.float64()
					elif type(value) == str:
						t = pa.string()  # TODO: can we safely use categories with smarter type detection or explicitly?
					elif type(value) == bool:
						t = pa.bool_()
					else:
						raise TypeError('Unknown data type', value, type(value), msg)
					schema_fields.append(pa.field(name, t))
				self._schema = pa.schema(schema_fields)
				# self._logger.info('Schema = %s', self._schema)

			for i, (key, value) in enumerate(msg.items()):
				if type(value) == str and self._is_float(value):
					value = float(value)
				self._column_data[key].append(value)

			self._buffer_position += 1

		if self._buffer_position == self.buffer_max_len:
			self._flush()

	def _reopen(self) -> None:
		'''
		Open new parquet file, either on startup or beginning of new day.

		Note that parquet doesn't support appending, so if the file already exists, we have to read it and write it
		at the beginning of the new file.
		'''
		if self._store is not None:
			self._store.close()
			self._store = None

		self._store_date = datetime.date.today()
		today_date_str = self._store_date.strftime('%Y-%m-%d')
		# today_file_name = today_date_dir + '/' + self.symbol + '@' + self.event_type + '.parquet'
		today_dir = f'data/{self.event_type}/exchange={self.exchange}/symbol={self.symbol}/dt={today_date_str}'
		today_file_name = f'{today_dir}/1.snappy.parquet'
		os.makedirs(today_dir, exist_ok = True)

		original_table: Optional[pa.Table] = None
		if os.path.exists(today_file_name):
			try:
				old_file_name = today_file_name + '.bak'
				os.rename(today_file_name, old_file_name)
				original_file = pq.ParquetFile(old_file_name)
				original_table: pa.Table = original_file.read_row_group(i = 0, use_threads = False)
			except Exception as ex:
				self._logger.warning(f'Cannot append to the existing file for dumper = {self.symbol}@{self.event_type}! Ex = %s', ex)

		self._logger.debug(f'Opening {today_file_name}')
		self._schema = self._update_store_metadata(self._schema, existed = original_table is not None)
		page_size_guess = self.buffer_max_len * (len(self._schema.names) + 10) * 8
		actual_table_size = original_table.nbytes
		self._logger.info('Page size guess = %d, actual table size = %d', page_size_guess, actual_table_size)
		page_size_guess = max(page_size_guess, actual_table_size)
		self._store = pq.ParquetWriter(
			where = today_file_name,
			schema = self._schema,
			# compression = 'brotli',
			# compression_level = 6,
			compression = 'snappy',
			version = '2.6',
			data_page_version = '2.0',
			write_batch_size = self.buffer_max_len,
			data_page_size = page_size_guess,
			dictionary_pagesize_limit = page_size_guess,
		)
		pool = pa.default_memory_pool()
		pool.release_unused()
		self._logger.info('Allocated = %d %d %d',pool.bytes_allocated(), pool.max_memory(), pa.total_allocated_bytes())

		if original_table is not None:
			del original_table
			for i in range(0, original_file.num_row_groups):
				original_table = original_file.read_row_group(i, use_threads = False)
				self._store.write_table(original_table, row_group_size = self._row_group_size)
				del original_table
			if random.random() > 0.9:
				# Trigger GC after 10% re-opens to better fit into memory
				self._logger.debug('GC')
				gc.collect()
			os.unlink(old_file_name)

	def _update_store_metadata(self, schema: pa.Schema, existed: bool) -> pa.Schema:
		custom_metadata = {
			b'date': self._store_date.isoformat().encode('ascii'),
			b'contains_gaps': b'Yes' if existed else b'No',
			b'symbol': self.symbol.encode('ascii'),
			b'event_type': self.event_type.encode('ascii'),
			b'exchange': self.exchange.encode('ascii'),
		}
		merged_metadata = {**(schema.metadata or {}), **custom_metadata}
		return schema.with_metadata(merged_metadata)

	def close(self) -> None:
		self._logger.debug(f'Closing {self.symbol}@{self.event_type}')
		self._flush()
		self._terminating = True
		if self._store is not None:
			self._store.close()

	def _flush(self) -> None:
		if not self._column_data or self._terminating:
			return

		if self._buffer_position == 0:
			return
		if self._store is None:
			self._reopen()

		self._logger.debug(f'Flushing {self.symbol}@{self.event_type}')
		with self._data_lock:
			pa_table = pa.Table.from_pydict(self._column_data, schema = self._schema)
			self._buffer_position = 0
			self._column_data.clear()
			self._store.write_table(pa_table, row_group_size = self._row_group_size)
		if random.random() > 0.99: # TODO
			# Trigger GC after 1% flushes to better fit into memory
			self._logger.debug('GC')
			del pa_table
			gc.collect()

	@staticmethod
	def _is_float(val):
		try:
			float(val)
			return True
		except ValueError:
			return False

	@staticmethod
	def _is_int(val):
		try:
			f = float(val)
			return f == int(f)
		except ValueError:
			return False

	@staticmethod
	def _is_hex(val):
		if len(val) < 8:
			return False
		try:
			f = int(val, 16)
			return True
		except ValueError:
			return False
