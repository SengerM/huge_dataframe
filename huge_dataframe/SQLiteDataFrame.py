import sqlite3
import pandas
from pathlib import Path
import sys
import warnings

class SQLiteDataFrameDumper:
	def __init__(self, path_to_sqlite_database:Path, dump_after_n_appends:int, delete_database_if_already_exists:bool=True):
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
		delete_database_if_already_exists: bool, default True
			If `True`, the SQLite database file will be deleted if it
			exists beforehand.
		"""
		if not isinstance(path_to_sqlite_database, Path):
			raise TypeError(f'`sqlite_database` must be an instance of {Path}, received object of type {type(path_to_sqlite_database)}')
		
		self._path_to_sqlite_database = path_to_sqlite_database
		self.dump_after_n_appends = dump_after_n_appends
		self._delete_database_if_already_exists = delete_database_if_already_exists
		
	def append(self, dataframe:pandas.DataFrame):
		"""Append new data to the ever growing dataframe. After a number
		of consecutive calls to this function, the data will be automatically
		dumped to disk and deleted from memory (by a call to the `dump` 
		method), leaving space for new data. Such number is given by 
		`dump_after_n_appends` in the `__init__` method.
		
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
		
		if self._n_appends_since_last_dump >= self.dump_after_n_appends:
			self.dump()
	
	def dump(self):
		"""Dump the data to disk and remove it from RAM.
		"""
		if len(self._list_of_dataframes) > 0:
			df = pandas.concat(self._list_of_dataframes)
			with warnings.catch_warnings():
				warnings.filterwarnings("ignore", message="The spaces in these column names will not be changed. In pandas versions < 0.14, spaces were converted to underscores.")
				df.to_sql('my_table', self.sqlite_connection, if_exists='append')
			self._list_of_dataframes = list()
			self._n_appends_since_last_dump = 0
	
	def __enter__(self):
		if self._delete_database_if_already_exists == True:
			self._path_to_sqlite_database.unlink(missing_ok=True)
		self._list_of_dataframes = list()
		self._n_appends_since_last_dump = 0
		self.sqlite_connection = sqlite3.connect(self._path_to_sqlite_database)
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.dump()
		self.sqlite_connection.close()
