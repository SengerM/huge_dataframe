from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper
import pandas
import numpy
from pathlib import Path

with SQLiteDataFrameDumper(
	Path('df.sqlite'),
	dump_after_n_appends = 10e3, # Use this to limit the amount of RAM memory consumed.
	dump_after_seconds = 10, # Use this to ensure the data is stored after some time.
) as df_dumper:
	for i in range(99):
		for j in range(99):
			df = pandas.DataFrame(
				{
					'i': i,
					'j': j,
					'data': numpy.random.randn(),
				},
				index = [0],
			)
			df_dumper.append(df)
