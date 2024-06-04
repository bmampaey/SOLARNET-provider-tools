import os
import logging
import io
import zlib
import requests
from glob import iglob
from datetime import datetime
from urllib.parse import urljoin, unquote, urlparse
from dateutil.parser import parse, ParserError
from pyvo.dal import tap
from astropy.io import fits
import htmllistparse


__all__ = [
	'parse_date_time_string',
	'iter_files',
	'iter_urls',
	'iter_tap_records',
	'get_fits_header_from_local_file',
	'get_fits_header_from_url',
]


def parse_date_time_string(date_time_string, default=datetime(2000, 1, 1)):
	"""Parse a date time like string and return a timestamp"""
	try:
		date_time = parse(date_time_string, default=default)
	except ParserError as error:
		raise ValueError('Date time string "%s" is not a valid date: %s' % (date_time_string, error)) from error
	else:
		return date_time


def iter_files(file_path_globs, min_modification_time=None):
	"""Accept a list of glob file paths and return the individuals file path"""

	if min_modification_time is not None:
		min_modification_time = min_modification_time.timestamp()

	for file_path_glob in file_path_globs:
		for file_path in iglob(file_path_glob, recursive=True):
			if min_modification_time and os.path.getmtime(file_path) < min_modification_time:
				logging.info('Skipping FITS file "%s": file modification time earlier than specified min', file_path)
			else:
				yield file_path


def iter_urls(base_urls, extension='.fits', min_modification_time=None, timeout=30):
	"""Accept a list of base URLs and return the individuals file URLs"""

	for base_url in base_urls:
		url_path = urlparse(unquote(base_url)).path

		if url_path.endswith(extension):
			yield base_url

		elif url_path.endswith('/'):
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


def iter_tap_records(service_url, table_name, max_count=1000, min_modification_time=None, exclude_granule_uid=[]):
	"""Accept a service URL and a table name and return the records in the table"""

	# If the min_modification_time, add a WHERE clause to exclude older records
	where_clause = ''
	if min_modification_time is not None:
		where_clause += " WHERE modification_date >= '%s'" % min_modification_time.isoformat()

	# Get the total number of records to process
	query = 'SELECT count(*) AS record_count FROM %s %s' % (table_name, where_clause)

	record_count = None
	while record_count is None:
		logging.debug('Executing TAP query %s', query)
		try:
			result = tap.search(service_url, query)
		except Exception as error:
			logging.warning('TAP query failed (%s), retrying!', error)
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
		except Exception as error:
			logging.warning('TAP query failed (%s), retrying!', error)
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


def get_fits_header_from_local_file(file_path, hdu_name_or_index=0):
	"""Return the header of a local FITS file"""

	with fits.open(file_path) as hdus:
		return hdus[hdu_name_or_index].header


def get_fits_header_from_url(file_url, header_size=2880, header_offset=0, zipped=False, webserver_auth=None):
	"""Return the header of a FITS file URL"""

	# If FITS file is zipped, the response content must be decompressed before writing it to the pseudo file
	if zipped:
		decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)

	# We download the file by chunk, by specifying the desired range, until we have the complete FITS header
	range_start = header_offset
	range_end = header_offset + header_size

	# We store the response in a pseudo file for the fits library
	fits_file = io.BytesIO()

	while True:
		logging.debug('Reading file %s from %s to %s', file_url, range_start, range_end - 1)
		# We set the desired range in the HTTP header, note that both bounds are inclusive
		response = requests.get(
			file_url, headers={'Range': 'Bytes=%s-%s' % (range_start, range_end - 1)}, auth=webserver_auth
		)

		if zipped:
			fits_file.write(decompressor.decompress(response.content))
		else:
			fits_file.write(response.content)

		# It is necessary to rewind the file to pass it to the fits library
		fits_file.seek(0)

		# Try to read a full header from the pseudo file, if header is partial, an IOError will be raised
		try:
			fits_header = fits.Header.fromfile(fits_file)
		except IOError:
			# Header is partial, we need to read more from the file
			# Per fits standard, fits file header size is always a multiple of 2880
			range_start = range_end
			range_end = range_start + 2880
		else:
			if range_end > (header_size + header_offset):
				logging.warning(
					'Header size of FITS file %s is %s (was set to %s), consider increasing the value of header_size',
					file_url,
					range_end - header_offset,
					header_size,
				)

			return fits_header
