import pandas
from pathlib import Path
from bureaucrat.SmarterBureaucrat import SmarterBureaucrat # https://github.com/SengerM/bureaucrat
import numpy
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper
import time
import datetime

SIGNALS_N_SAMPLES = 4444
N_EVENTS = 999

def gaussian(x, mu, sigma, amplitude=1):
	return amplitude/sigma/(2*numpy.pi)**.5*numpy.exp(-((x-mu)/sigma)**2/2)

def script_core(path_to_new_measurement:Path):
	Quique = SmarterBureaucrat(
		path_to_new_measurement,
		_locals = locals(),
		new_measurement = True,
	)

	with Quique.do_your_magic():
		with SQLiteDataFrameDumper(
			Quique.path_to_default_output_directory/Path('waveforms.sqlite'), 
			dump_after_n_appends = 1e3, # Use this to limit the amount of RAM memory consumed.
			dump_after_seconds = 10, # Use this to ensure the data is stored after some time.
		) as ever_growing_dataframe:
			n_waveform = 0
			for n_event in range(N_EVENTS):
				print(f'n_event={n_event}/{N_EVENTS-1}')
				
				time.sleep(numpy.random.exponential(scale=.3)) # Say your data acquisition takes this time (for example measuring events from a radioactive source).
				
				time_array = numpy.linspace(0,10e-9,SIGNALS_N_SAMPLES)
				for device_name in {'LGAD','PMT'}:
					sigma = (time_array.max()-time_array.mean())/10
					sigma *= numpy.random.rand() + .5
					if device_name == 'PMT':
						sigma /= 2
					signal = gaussian(time_array, mu=time_array.mean(), sigma=sigma)
					signal /= signal.max()
					signal *= numpy.random.rand()*2
					signal += numpy.random.randn(len(signal))/22
					measured_data_df = pandas.DataFrame(
						{
							'n_waveform': n_waveform,
							'n_event': n_event,
							'device_name': device_name,
							'Time (s)': time_array,
							'Amplitude (V)': signal,
							'Temperature (°C)': -20 + numpy.random.randn()*.1,
							'When': datetime.datetime.now(),
						}
					)
					measured_data_df.set_index(['n_waveform','n_event','device_name'], inplace=True)
					ever_growing_dataframe.append(measured_data_df)
					n_waveform += 1

if __name__ == '__main__':
	script_core(Path.home()/'Desktop/measured_data'/Path(input('Measurement name? ')))
