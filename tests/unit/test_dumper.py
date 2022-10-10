import datetime
import os

import freezegun
import shutil

from cryptofeed.util.dumper import Dumper
from tools import read_parquet


def test_dumper():
	todays_filename = f'data/event/exchange=ex/symbol=test_dumper/dt={datetime.date.today().isoformat()}/1.snappy.parquet'
	try:
		os.unlink(todays_filename)
	except OSError:
		pass

	dumper = Dumper('test', 'test_dumper', 'event', 'ex', buffer_len = 3)
	dumper.dump({'a': '1', 'b': 'asd'})
	dumper.dump({'a': '2', 'b': 'qwe'})
	dumper.dump({'a': '3', 'b': 'rty'})
	dumper.dump({'a': '4', 'b': 'dfi'})
	dumper.close()

	pf, df, meta = read_parquet.read(todays_filename)
	assert df.shape[0] % 4 == 0
	assert df.shape[1] == 2
	assert df.iloc[1, 1] == 'qwe'


def test_dumper_reopen():
	shutil.rmtree('data/event/exchange=ex/symbol=test_dumper_reopen', ignore_errors = True)

	with freezegun.freeze_time('2021-01-01 23:59:00', tick = True):
		dumper = Dumper('test', 'test_dumper_reopen', 'event', 'ex', buffer_len = 3)
		dumper.dump({'a': '1', 'b': 'asd'})
		dumper.dump({'a': '2', 'b': 'qwe'})
		dumper.dump({'a': '3', 'b': 'rty'})
		dumper.dump({'a': '4', 'b': 'dfi'})

	with freezegun.freeze_time('2021-01-02 00:01:00', tick = True):
		dumper.dump({'a': '5', 'b': 'dfg'})
		dumper.dump({'a': '6', 'b': 'ghj'})
		dumper.dump({'a': '7', 'b': 'ert'})
		dumper.dump({'a': '8', 'b': 'yui'})
		dumper.close()

	pf, df, meta = read_parquet.read('data/event/exchange=ex/symbol=test_dumper_reopen/dt=2021-01-01/1.snappy.parquet')
	assert df.shape[0] == 4
	assert df.iloc[3, 1] == 'dfi'

	pf, df, meta = read_parquet.read('data/event/exchange=ex/symbol=test_dumper_reopen/dt=2021-01-02/1.snappy.parquet')
	assert df.shape[0] == 4
	assert df.iloc[3, 1] == 'yui'


def test_dumper_memory():
	shutil.rmtree('data/event/exchange=benchmark', ignore_errors = True)
	with freezegun.freeze_time('1991-01-01 00:00') as fronzen_time:
		dumpers = [
			Dumper(f'test', f'test_dumper_memory_{i}', 'event', 'benchmark', buffer_len = 500)
			for i in range(10)
		]
		for i in range(10*60*60*24*3):
			dumpers[i % 10].dump({'a': i, 'b': 'asd', 'c': 6, 'd': 862.5, 'e': 'asdwuu'})
			fronzen_time.tick(datetime.timedelta(seconds = 0.1))
		for i in range(10):
			dumpers[i].close()
