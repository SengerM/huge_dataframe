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
N_EVENTS = 3333 # Make it as big as your hard drive can store

with SQLiteDataFrameDumper(Path('waveforms.sqlite'), dump_after_n_appends=1e3) as ever_growing_dataframe:
	time_array = numpy.linspace(0,10e-9,SIGNALS_N_SAMPLES)
	for n_event in range(N_EVENTS):
		if n_event%111 == 0:
			print(f'n_event={n_event}/{N_EVENTS-1}')
		for device_name in {'detector 1','detector 2','detector 3'}:
			measured_data_df = pandas.DataFrame(
				{
					'n_event': n_event,
					'device_name': device_name,
					'Time (s)': time_array,
					'Amplitude (V)': numpy.random.randn(len(time_array)),
					'Temperature (°C)': -20 + numpy.random.randn()*.1,
				}
			)
			measured_data_df.set_index(['n_event','device_name'],inplace=True)
			ever_growing_dataframe.append(measured_data_df)
```

More examples [here](examples).
