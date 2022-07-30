# huge_dataframe

Dump huge and ever-growing Pandas dataframes into disk easily.

This tiny package comes to solve a specific problem I find daily in the 
lab which is that I measure data from individual events repetitively for
a lot of events. This happens within a loop and when the number of events
is big enough the computer normally runs out of memory and everything fails,
unless you dump the data regularly into the hard drive, which is annoying.
So this package allows to trivially and without effort dump data regularly
into an [SQLite](https://sqlite.org/index.html) file, the most used database
engine in the world.

There are some packages that are supposed to do this, like [dask](https://www.dask.org/)
but I was not able to make it work, due to some installation issue. *huge_dataframe*
and all its dependencies should be trivial to install.

# Installation

```
pip install git+https://github.com/SengerM/huge_dataframe
```
To install the dependencies just run `pip install whatever_python_cannot_find`.		

# Usage

This was designed to be easy to use and automatic. Below is a pseudocode showing how it is used:

```python
from huge_dataframe.SQLiteDataFrame import SQLiteDataFrameDumper

with SQLiteDataFrameDumper('waveforms.sqlite', dump_after_n_appends=1e3, dump_after_seconds=10) as ever_growing_dataframe:
	# The `with` statement ensures data will be stored no matter what happens.
	While True:
		bunch_data_df = produce_data() # Here you measure, process, or whatever.
		ever_growing_dataframe.append(bunch_data_df) # Just append your bunch of data, that's it.
```

Usage working example:

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
			# Create some fake data...
			measured_data_df = pandas.DataFrame(
				{
					'n_event': n_event,
					'device_name': device_name,
					'Time (s)': numpy.linspace(0,10e-9,SIGNALS_N_SAMPLES),
					'Amplitude (V)': numpy.random.randn(SIGNALS_N_SAMPLES),
					'Temperature (°C)': -20 + numpy.random.randn()*.1,
				}
			)
			measured_data_df.set_index(['n_event','device_name'],inplace=True) # Indexes make data accession (and also writing) way much faster.
			# Append the new data into the ever growing dataframe...
			ever_growing_dataframe.append(measured_data_df)
# At this point you will find the data automatically dumped into the `waveforms.sqlite` file.
```

You can later on read the dumped dataframe all at once
```python
from huge_dataframe.SQLiteDataFrame import load_whole_dataframe

df = load_whole_dataframe('waveforms.sqlite')
print(df)
```
but this will be slow and may need more memory than you have, or you can
iterate event by event without loading it all at once, which will be very
fast if you iterate over the indices:
```python
import sqlite3
import pandas

connection = sqlite3.connect('waveforms.sqlite')
cursor = connection.cursor()
cursor.execute('SELECT max(n_event) from dataframe_table')
number_of_events_to_process = cursor.fetchone()[0]+1

for n_event in range(number_of_events_to_process):
	event_df = pandas.read_sql(f'SELECT * from dataframe_table where n_event=={n_event}', connection) # Because we created an index with `n_event`, this is amazingly fast reading directly from disk and uses almost no memory.
	print(event_df)
```

More examples [here](examples).

For more documentation take a look at [the source code](huge_dataframe/SQLiteDataFrame.py).
