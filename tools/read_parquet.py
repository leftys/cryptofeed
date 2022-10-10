from typing import Tuple, Dict

import pandas as pd
import pyarrow.parquet as pq
import sys
from pprint import pprint

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 150)


def read(filename: str) -> Tuple[pq.ParquetFile, pd.DataFrame, Dict[bytes, bytes]]:
	pf = pq.ParquetFile(filename)
	df = pf.read().to_pandas()
	try:
		df['R'] = pd.to_datetime(df['R'], unit = 'ns')
		df['E'] = pd.to_datetime(df['E'], unit = 'ms')
		df['T'] = pd.to_datetime(df['T'], unit = 'ms')
	except KeyError:
		pass
	metadata = dict(pf.schema_arrow.metadata)
	try:
		del metadata[b'pandas']
	except KeyError:
		pass
	return pf, df, metadata


if __name__ == '__main__':
	pf, df, metadata = read(sys.argv[1])
	print('Dataframe')
	print(df)
	if len(sys.argv) >= 3 and sys.argv[2] == '-v':
		print('Meta-data:')
		print(pf.metadata)
		print('Common meta-data:')
		print(pf.common_metadata)
		print('Dtypes')
		print(df.dtypes)
	print('Schema_arrow metadata (except pandas-specific)')
	pprint(metadata)
