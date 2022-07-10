import sqlite3
from pathlib import Path
from bureaucrat.SmarterBureaucrat import SmarterBureaucrat # https://github.com/SengerM/bureaucrat
import numpy
import pandas
from signals.PeakSignal import PeakSignal, draw_in_plotly
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper

def script_core(path_to_measurement:Path):
	Norberto = SmarterBureaucrat(
		path_to_measurement,
		_locals = locals(),
	)

	Norberto.check_required_scripts_were_run_before('measure.py')

	with Norberto.do_your_magic():
		sqlite3_connection_waveforms = sqlite3.connect(Norberto.path_to_output_directory_of_script_named('measure.py')/Path('waveforms.sqlite'))
		
		print(f'Reading the total number of waveforms to process...')
		sqlite3_cursor_waveforms = sqlite3_connection_waveforms.cursor()
		sqlite3_cursor_waveforms.execute('SELECT max(n_waveform) from dataframe')
		number_of_waveforms_to_process = sqlite3_cursor_waveforms.fetchone()[0]+1
		
		with SQLiteDataFrameDumper(Norberto.path_to_default_output_directory/Path('parsed_data.sqlite'), dump_after_n_appends=1e3) as ever_growing_dataframe:
			for n_waveform in range(number_of_waveforms_to_process):
				print(f'Processing n_waveform = {n_waveform}/{number_of_waveforms_to_process-1}')
				waveform_df = pandas.read_sql_query(f'SELECT * from dataframe where n_waveform=={n_waveform}', sqlite3_connection_waveforms)
				signal = PeakSignal(
					time = waveform_df['Time (s)'],
					samples = waveform_df['Amplitude (V)'],
				)
				
				this_waveform_data = dict()
				for col in ['n_event','device_name','Temperature (°C)','n_waveform']:
					this_waveform_data[col] = list(set(waveform_df[col]))[0]
				this_waveform_data['Amplitude (V)'] = signal.amplitude
				this_waveform_data['Collected charge (V s)'] = signal.peak_integral
				this_waveform_data['Noise (V)'] = signal.noise
				this_waveform_data['Time over noise (s)'] = signal.time_over_noise
				
				this_waveform_data_df = pandas.DataFrame(
					this_waveform_data,
					index=[0],
				)
				this_waveform_data_df.set_index(['n_waveform','n_event','device_name'], inplace=True)
				
				ever_growing_dataframe.append(this_waveform_data_df)

if __name__ == '__main__':
	script_core(Path(input('Path? ')))
