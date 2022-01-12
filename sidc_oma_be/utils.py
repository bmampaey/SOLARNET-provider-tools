import logging
from glob import iglob
from datetime import datetime
from dateutil.parser import parse, ParserError

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

def get_svo_auth(auth_file):
	'''Read the username and api key for SVO authentication from a file'''
	try:
		with open(auth_file, 'r') as file:
			auth = file.read().strip()
	except Exception as why:
		raise RuntimeError('Could not read SVO username and api key from file "%s": %s' % (auth_file, why)) from why
	
	try:
		username, api_key = auth.split(':', 1)
	except ValueError as why:
		raise RuntimeError('Authentication file "%s" does not have the correct format, i.e. username:api_key' % auth_file) from why
	
	return username, api_key

def parse_date_time(date_time_string, default = datetime(2000, 1, 1)):
	'''Parse a date time like string and return a datetime object'''
	if date_time_string:
		try:
			date_time = parse(date_time_string, default = default)
		except ParserError as why:
			raise ValueError('Date time string "%s" is not a valid date: %s' % (date_time_string, why)) from why
		else:
			date_time = date_time.timestamp()
	else:
		date_time = None
	
	return date_time
