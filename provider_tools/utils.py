import logging
from glob import iglob
from datetime import datetime
from dateutil.parser import parse, ParserError

__all__ = ['iter_files', 'parse_date_time_string']

def iter_files(file_path_globs, min_modification_time = None):
	'''Accept a list of glob file paths and return the individuals file path'''
	
	if min_modification_time is not None:
		min_modification_time = min_modification_time.timestamp()
	
	for file_path_glob in file_path_globs:
		for file_path in iglob(file_path_glob, recursive = True):
			if min_modification_time and os.path.getmtime(file_path) < min_modification_time:
				logging.info('Skipping FITS file "%s": file modification time earlier than specified min', file_path)
			else:
				yield file_path

def parse_date_time_string(date_time_string, default = datetime(2000, 1, 1)):
	'''Parse a date time like string and return a timestamp'''
	try:
		date_time = parse(date_time_string, default = default)
	except ParserError as why:
		raise ValueError('Date time string "%s" is not a valid date: %s' % (date_time_string, why)) from why
	else:
		return date_time
