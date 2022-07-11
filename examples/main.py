"""Run this for a complete example demonstration."""

from measure import script_core as measure
from parse import script_core as parse
from plot_parsed import script_core as plot_parsed
from pathlib import Path

try:
	path_to_measurement_base_directory = measure(Path.home()/Path('measured_data_DELETEME')/Path(input('Measurement name? ').replace(' ','_')))
except:
	pass
parse(path_to_measurement_base_directory)
plot_parsed(path_to_measurement_base_directory)
print(f'You can find the results in {path_to_measurement_base_directory}')
