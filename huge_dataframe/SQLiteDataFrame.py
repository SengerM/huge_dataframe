import sqlite3
import pandas
from pathlib import Path

class SQLiteDataFrameDumper:
	def __init__(self, prototype_data_frame:pandas.DataFrame, path_to_sqlite_database:Path, dump_after_n_appends:int, delete_database_if_already_exists:bool=True):
		if not isinstance(prototype_data_frame, pandas.DataFrame):
			raise TypeError(f'`prototype_data_frame` must be an instance of {pandas.DataFrame}, received object of type {type(prototype_data_frame)}')
		if not isinstance(path_to_sqlite_database, Path):
			raise TypeError(f'`sqlite_database` must be an instance of {Path}, received object of type {type(path_to_sqlite_database)}')
		
		self._path_to_sqlite_database = path_to_sqlite_database
		self._prototype_data_frame = prototype_data_frame
		self.dump_after_n_appends = dump_after_n_appends
		self._delete_database_if_already_exists = delete_database_if_already_exists
		
	def append(self, data_frame:pandas.DataFrame):
		self._list_of_dataframes.append(data_frame)
		self._n_appends_since_last_dump += 1
		if self._n_appends_since_last_dump >= self.dump_after_n_appends:
			self.dump()
	
	def dump(self):
		if len(self._list_of_dataframes) > 0:
			df = pandas.concat(self._list_of_dataframes, ignore_index=True)
			df.set_index(self._prototype_data_frame.index.names, inplace=True)
			df.to_sql('my_table', self.sqlite_connection, if_exists='append')
			self._list_of_dataframes = list()
			self._n_appends_since_last_dump = 0
	
	def __enter__(self):
		if self._delete_database_if_already_exists == True:
			self._path_to_sqlite_database.unlink()
		self._list_of_dataframes = list()
		self._n_appends_since_last_dump = 0
		self.sqlite_connection = sqlite3.connect(self._path_to_sqlite_database)
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.dump()
		self.sqlite_connection.close()
