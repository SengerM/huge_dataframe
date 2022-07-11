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
N_EVENTS = 9999 # Make it as big as your hard drive can store and watch as the `waveforms.sqlite` file grows in size.

with SQLiteDataFrameDumper(
		Path('waveforms.sqlite'), 
		dump_after_n_appends = 1e3, # Use this to limit the amount of RAM memory consumed.
		dump_after_seconds = 10, # Use this to ensure the data is stored after some time.
	) as ever_growing_dataframe: # The `with` statement ensures all the data that was ever appended will be stored in disk.
	for n_event in range(N_EVENTS):
		if n_event%111 == 0:
			print(f'n_event={n_event}/{N_EVENTS-1}')
		for device_name in {'detector 1','detector 2','detector 3'}:
			measured_data_df = pandas.DataFrame(
				{
					'n_event': n_event,
					'device_name': device_name,
					'Time (s)': numpy.linspace(0,10e-9,SIGNALS_N_SAMPLES), # Fake data
					'Amplitude (V)': numpy.random.randn(SIGNALS_N_SAMPLES), # Fake data
					'Temperature (°C)': -20 + numpy.random.randn()*.1,
				}
			)
			measured_data_df.set_index(['n_event','device_name'],inplace=True) # Indexes make data accession (and also writing) way much faster.
			ever_growing_dataframe.append(measured_data_df)
```

More examples [here](examples).
