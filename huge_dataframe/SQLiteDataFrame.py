import sqlite3
import pandas
from pathlib import Path
import sys
import warnings
import time

NAME_OF_TABLE_IN_SQLITE_DATABASE = 'dataframe_table'
NAME_OF_INDEX_IN_SQLITE_DATABASE = 'dataframe_index'

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
			df = pandas.concat(self._list_of_dataframes)
			with warnings.catch_warnings():
				warnings.filterwarnings("ignore", message="The spaces in these column names will not be changed. In pandas versions < 0.14, spaces were converted to underscores.")
				df.to_sql(
					NAME_OF_TABLE_IN_SQLITE_DATABASE, 
					self.sqlite_connection, 
					if_exists = 'append',
					index = list(self._original_dataframe.index.names) != [None], # If there is an index by the user, use it, otherwise don't use it.
				)
		self._list_of_dataframes = list()
		self._n_appends_since_last_dump = 0
		self._time_when_last_dump = time.time()
		
	def __enter__(self):
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.dump()
		
		if hasattr(self, '_original_dataframe'): # If the attribute isn't there it means nothing was ever appended.
			# Rename the index to something known, it seems it is not possible to rename but you have to create a new one and delete the old one https://stackoverflow.com/questions/42530689/how-to-rename-an-index-in-sqlite
			if list(self._original_dataframe.index.names) != [None]: # If there is an index...
				name_of_index_to_rename = pandas.read_sql(f'PRAGMA index_list({NAME_OF_TABLE_IN_SQLITE_DATABASE});', self.sqlite_connection)['name'][0]
				index_columns = ''
				for i in self._original_dataframe.index.names:
					index_columns += i
					index_columns += ', '
				index_columns = index_columns[:-2]
				self.sqlite_connection.cursor().execute(f'CREATE INDEX "{NAME_OF_INDEX_IN_SQLITE_DATABASE}" ON "{NAME_OF_TABLE_IN_SQLITE_DATABASE}" ({index_columns});')
				self.sqlite_connection.cursor().execute(f'DROP INDEX {name_of_index_to_rename};')
		
		self.sqlite_connection.close()

def load_whole_dataframe(path_to_sqlite_file:Path):
	"""Loads the whole dataframe stored in the SQLite file into memory 
	(make sure it fits!). 
	
	Parameters
	----------
	path_to_sqlite_file: Path
		Path to a file produced by `SQLiteDataFrameDumper`.
	"""
	connection = sqlite3.connect(path_to_sqlite_file)
	df = pandas.read_sql(f'SELECT * from {NAME_OF_TABLE_IN_SQLITE_DATABASE}', connection)
	if len(pandas.read_sql(f'PRAGMA index_list({NAME_OF_TABLE_IN_SQLITE_DATABASE});', connection)) != 0: # This means there is an index.
		names_of_index_columns = list(pandas.read_sql(f'PRAGMA index_info({NAME_OF_INDEX_IN_SQLITE_DATABASE});', connection)['name'])
		df.set_index(names_of_index_columns, inplace=True)
	return df
