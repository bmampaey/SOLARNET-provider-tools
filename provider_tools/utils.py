import os
import logging
from glob import iglob
from datetime import datetime
from urllib.parse import urljoin, unquote, urlparse
from dateutil.parser import parse, ParserError
from pyvo.dal import tap
import htmllistparse


__all__ = ['parse_date_time_string', 'iter_files', 'iter_urls', 'iter_tap_records']

def parse_date_time_string(date_time_string, default = datetime(2000, 1, 1)):
	'''Parse a date time like string and return a timestamp'''
	try:
		date_time = parse(date_time_string, default = default)
	except ParserError as why:
		raise ValueError('Date time string "%s" is not a valid date: %s' % (date_time_string, why)) from why
	else:
		return date_time


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


def iter_urls(base_urls, extension = '.fits', min_modification_time = None, timeout = 30):
	'''Accept a list of base URLs and return the individuals file URLs'''
	
	for base_url in base_urls:
		if urlparse(unquote(base_url)).path.endswith(extension):
			yield base_url
		else:
			trash, listing = htmllistparse.fetch_listing(base_url, timeout=timeout)
			
			for file_entry in listing:
				url = urljoin(base_url, file_entry.name)
				
				if file_entry.name.endswith(extension):
					if min_modification_time is None or datetime(*file_entry.modified[:6]) >= min_modification_time:
						yield url
					else:
						logging.info('Skipping URL "%s": file modification time earlier than specified min', url)
				
				elif file_entry.name.endswith('/'):
					for url in iter_urls([url], extension, min_modification_time, timeout):
						yield url
				
				else:
					logging.debug('Skipping URL "%s": not a directory of a file with extension "%s"', url, extension)


def iter_tap_records(service_url, table_name, max_count = 1000, min_modification_time = None, exclude_granule_uid = []):
	'''Accept a service URL and a table name and return the records in the table'''
	
	# If the min_modification_time, add a WHERE clause to exclude older records
	where_clause = ''
	if min_modification_time is not None:
		where_clause += ' WHERE modification_date >= \'%s\'' % min_modification_time.isoformat()
	
	# Get the total number of records to process
	query = 'SELECT count(*) AS record_count FROM %s %s' % (table_name, where_clause)
	
	record_count = None
	while record_count is None:
		logging.debug('Executing TAP query %s', query)
		try:
			result = tap.search(service_url, query)
		except Exception as why:
			logging.warning('TAP query failed (%s), retrying!', why)
			continue
		else:
			record_count = result.getvalue('record_count', 0)
	
	logging.info('Found %s records for table %s', record_count, table_name)
	
	# Get the records by batch of max_count until there are no more records to process
	query = 'SELECT TOP %s * FROM %s %s ORDER BY granule_uid ASC OFFSET %%s' % (max_count, table_name, where_clause)
	offset = 0
	
	while record_count > 0:
		logging.debug('Executing TAP query %s', query % offset)
		try:
			result = tap.search(service_url, query % offset)
		except Exception as why:
			logging.warning('TAP query failed (%s), retrying!', why)
			continue
		
		if len(result) != min(record_count, max_count):
			logging.warning('Expected %s TAP records but received %s', min(record_count, max_count), len(result))
		
		record_count -= len(result)
		offset += len(result)
		
		for record in result:
			if record['granule_uid'] in exclude_granule_uid:
				logging.info('Record with granule_uid %s is in the exclude list, skipping!', record['granule_uid'])
			else:
				yield record
