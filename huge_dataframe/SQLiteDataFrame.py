import sqlite3
import pandas
from pathlib import Path
import sys
import warnings
import time

class SQLiteDataFrameDumper:
	def __init__(self, path_to_sqlite_database:Path, dump_after_n_appends:int, dump_after_seconds:float=600, delete_database_if_already_exists:bool=True):
		"""Class to easily dump to disk "ever growing dataframes" on the 
		fly within a loop.
		
		Parameters
		----------
		path_to_sqlite_database: Path
			Path to an SQLite database file. If it does not exist, it will
			be created.
		dump_after_n_appends: int
			Number of calls to `append` before the data is dumped to the
			SQLite file. The bigger this number the less dumps (so faster)
			but also the higher the required RAM memory to handle all the
			data.
		dump_after_seconds: float, default 60
			Number of seconds before automatically dumping to disk when
			`append` is called.
		delete_database_if_already_exists: bool, default True
			If `True`, the SQLite database file will be deleted if it
			exists beforehand.
		"""
		if not isinstance(path_to_sqlite_database, Path):
			raise TypeError(f'`sqlite_database` must be an instance of {Path}, received object of type {type(path_to_sqlite_database)}')
		
		self._path_to_sqlite_database = path_to_sqlite_database
		self.dump_after_n_appends = dump_after_n_appends
		self._delete_database_if_already_exists = delete_database_if_already_exists
		self.dump_after_seconds = dump_after_seconds
		
		if self._delete_database_if_already_exists == True:
			self._path_to_sqlite_database.unlink(missing_ok=True)
		self.sqlite_connection = sqlite3.connect(self._path_to_sqlite_database)
		
		self.dump() # To initialize.
		
	def append(self, dataframe:pandas.DataFrame):
		"""Append new data to the ever growing dataframe. Data will be 
		automatically dumped to disk and deleted from memory if any of 
		the following conditions is true:
		- `append` has been called more than `dump_after_n_appends` since
		last dump to disk.
		- The number of seconds elapsed since the last dump and the current
		call to `append` is greater than `dump_after_seconds`.
		
		Parameters
		----------
		dataframe: pandas.DataFrame
			The dataframe to append.
		"""
		if not isinstance(dataframe, pandas.DataFrame):
			raise TypeError(f'`dataframe` must be an instance of {pandas.DataFrame}, received object of type {type(dafaframe)}')
		if not hasattr(self, '_original_dataframe'):
			self._original_dataframe = dataframe
		# Check that the `dataframe` is compatible with what we have received before...
		if set(self._original_dataframe.columns) != set(dataframe.columns):
			raise ValueError(f'The columns of `dataframe` do not match the columns of the previous dataframes')
		if self._original_dataframe.index.names != dataframe.index.names:
			raise ValueError(f'The index of `dataframe` does not match the index of the previous dataframes')
		
		self._list_of_dataframes.append(dataframe)
		self._n_appends_since_last_dump += 1
		
		if self._n_appends_since_last_dump >= self.dump_after_n_appends or time.time()-self._time_when_last_dump >= self.dump_after_seconds:
			self.dump()
	
	def dump(self):
		"""Dump the data to disk and remove it from RAM.
		"""
		if hasattr(self, '_list_of_dataframes') and len(self._list_of_dataframes) > 0:
			print('Dumping...')
			df = pandas.concat(self._list_of_dataframes)
			with warnings.catch_warnings():
				warnings.filterwarnings("ignore", message="The spaces in these column names will not be changed. In pandas versions < 0.14, spaces were converted to underscores.")
				df.to_sql('dataframe', self.sqlite_connection, if_exists='append')
		self._list_of_dataframes = list()
		self._n_appends_since_last_dump = 0
		self._time_when_last_dump = time.time()
		
	def __enter__(self):
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.dump()
