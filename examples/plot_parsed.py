import sqlite3
import pandas
from pathlib import Path
from bureaucrat.SmarterBureaucrat import SmarterBureaucrat # https://github.com/SengerM/bureaucrat
import plotly.express as px
from huge_dataframe.SQLiteDataFrame import load_whole_dataframe

def read_sqlite(dbfile, table):
	# https://stackoverflow.com/a/67938218/8849755
	with sqlite3.connect(dbfile) as dbcon:
		return pandas.read_sql_query(f"SELECT * from {table}", dbcon)

Raúl = SmarterBureaucrat(
	Path(input('Path? ')),
	_locals = locals(),
)

Raúl.check_required_scripts_were_run_before('parse.py')

with Raúl.do_your_magic():
	parsed_data_df = load_whole_dataframe(Raúl.path_to_output_directory_of_script_named('parse.py')/Path('parsed_data.sqlite'))
	parsed_data_df = parsed_data_df.reset_index()
	
	for col in parsed_data_df.columns:
		if col in {'n_event','device_name','n_waveform'}:
			continue
		fig = px.histogram(
			parsed_data_df,
			x = col,
			facet_row = 'device_name',
		)
		fig.write_html(
			str(Raúl.path_to_default_output_directory/Path(f'{col}.html')),
			include_plotlyjs = 'cdn',
		)
