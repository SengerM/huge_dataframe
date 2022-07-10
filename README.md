# huge_dataframe

Handle huge Pandas dataframes easily.

# Usage

Usage example below

```python
import pandas
from pathlib import Path
import numpy
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper

SIGNALS_N_SAMPLES = 4444
N_EVENTS = 999

prototype_dataframe = pandas.DataFrame(
	{
		'n_event': 0,
		'device_name': 'hola',
		'Time (s)': numpy.linspace(1,2),
		'Amplitude (V)': numpy.linspace(1,2),
		'Temperature (°C)': 4,
	}
)
prototype_dataframe.set_index(['n_event','device_name'],inplace=True)

with SQLiteDataFrameDumper(prototype_dataframe, Path('waveforms.sqlite'), dump_after_n_appends=1e3) as huge_dataframe:
	time_array = numpy.linspace(0,10e-9,SIGNALS_N_SAMPLES)
	for n_event in range(N_EVENTS):
		print(f'n_event={n_event}/{N_EVENTS-1}')
		for device_name in {'detector 1','detector 2','detector 3'}:
			measured_data = pandas.DataFrame(
				{
					'n_event': n_event,
					'device_name': device_name,
					'Time (s)': time_array,
					'Amplitude (V)': numpy.random.randn(len(time_array)),
					'Temperature (°C)': -20 + numpy.random.randn()*.1,
				}
			)
			huge_dataframe.append(measured_data)
```
