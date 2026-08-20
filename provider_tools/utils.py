import datetime
import glob
import io
import logging
import os
import urllib.parse
import zlib

import astropy.io.fits
import dateutil.parser
import htmllistparse
import pyvo
import requests
import simplejson
import slumber

__all__ = [
	'parse_date_time_string',
	'iter_files',
	'iter_urls',
	'iter_tap_records',
	'get_fits_header_from_local_file',
	'get_fits_header_from_url',
	'JsonSerializer',
]


def parse_date_time_string(date_time_string, default=datetime.datetime(2000, 1, 1)):
	"""Parse a date and time string into a datetime object.

	Args:
		date_time_string (str): Date-time string to parse.
		default (datetime.datetime): Default datetime values to use for components
			not specified in `date_time_string`.

	Returns:
		(datetime.datetime): The parsed datetime object.

	Raises:
		ValueError: If `date_time_string` is not a valid date-time string.
	"""
	try:
		date_time = dateutil.parser.parse(date_time_string, default=default)
	except dateutil.parser.ParserError as error:
		raise ValueError('Date time string "%s" is not a valid date: %s' % (date_time_string, error)) from error
	else:
		return date_time


def iter_files(file_path_globs, min_modification_time=None):
	"""Iterate over files matching the given glob patterns.

	Args:
		file_path_globs (Iterable[str]): Glob patterns used to find files. Patterns may
			include recursive wildcards.
		min_modification_time (datetime.datetime): If specified, only files modified at or after
			this datetime are returned.

	Yields:
		(str): Paths to files matching the given glob patterns and modification
			time constraint.
	"""

	if min_modification_time is not None:
		min_modification_time = min_modification_time.timestamp()

	for file_path_glob in file_path_globs:
		for file_path in glob.iglob(file_path_glob, recursive=True):
			if min_modification_time is not None and os.path.getmtime(file_path) < min_modification_time:
				logging.info('Skipping FITS file "%s": file modification time earlier than specified min', file_path)
			else:
				yield file_path


def iter_urls(base_urls, extension='.fits', min_modification_time=None, timeout=30):
	"""Iterate over files with the given extension found at the specified URLs.

	Args:
		base_urls (Iterable[str]): URLs of files or directory listings to search.
		extension (str): File extension to include.
		min_modification_time (datetime.datetime): If specified, only files modified at
			or after this datetime are returned.
		timeout (int): Timeout in seconds for fetching directory listings.

	Yields:
		(str): URLs of files matching the given extension and modification time
			constraint.
	"""
	for base_url in base_urls:
		url_path = urllib.parse.urlparse(urllib.parse.unquote(base_url)).path

		if url_path.endswith(extension):
			yield base_url

		elif url_path.endswith('/'):
			trash, listing = htmllistparse.fetch_listing(base_url, timeout=timeout)

			for file_entry in listing:
				url = urllib.parse.urljoin(base_url, file_entry.name)

				if file_entry.name.endswith(extension):
					if min_modification_time is None or datetime.datetime(*file_entry.modified[:6]) >= min_modification_time:
						yield url
					else:
						logging.info('Skipping URL "%s": file modification time earlier than specified min', url)

				elif file_entry.name.endswith('/'):
					for url in iter_urls([url], extension, min_modification_time, timeout):
						yield url

				else:
					logging.debug('Skipping URL "%s": not a directory of a file with extension "%s"', url, extension)


def iter_tap_records(service_url, table_name, max_count=1000, min_modification_time=None, exclude_granule_uid=None):
	"""Iterate over records from a TAP service.

	Args:
		service_url (str): URL of the TAP service.
		table_name (str): Name of the table to query.
		max_count (int): Maximum number of records to retrieve per query.
		min_modification_time (datetime.datetime): If specified, only records
			 modified at or after this datetime are returned.
		exclude_granule_uid (list, optional): List of granule UIDs to exclude from the results.

	Yields:
		(Mapping): Records returned by the TAP service, excluding records whose granule
			UID is in `exclude_granule_uid`.
	"""

	if exclude_granule_uid is None:
		exclude_granule_uid = []

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
			result = pyvo.dal.tap.search(service_url, query)
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
			result = pyvo.dal.tap.search(service_url, query % offset)
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
	"""Return the header of a local FITS file.

	Args:
		file_path (str): Path to the FITS file.
		hdu_name_or_index (str or int): Name or index of the HDU from which
			 to retrieve the header.

	Returns:
		(astropy.io.fits.header.Header): The FITS header from the specified HDU.
	"""

	return astropy.io.fits.getheader(file_path, hdu_name_or_index)


def get_fits_header_from_url(
	file_url, http_session, header_size=2880, header_offset=0, zipped=False, max_retry_count=3
):
	"""Return the header of a remote FITS file.

	Args:
		file_url (str): URL of the FITS file.
		http_session (requests.Session): HTTP session used to download the file.
		header_size (int): Number of bytes to download initially for the FITS header.
		header_offset (int): Byte offset at which to start downloading the header.
		zipped (bool): Whether the FITS file is gzip-compressed.
		max_retry_count (int): Maximum number of attempts for each HTTP request.

	Returns:
		(astropy.io.fits.header.Header): The FITS header read from the remote file.

	Raises:
		RuntimeError: If the FITS file cannot be downloaded after the maximum
			number of retries.
	"""

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
		for retry_count in range(1, max_retry_count + 1):
			try:
				response = http_session.get(
					file_url, headers={'Range': 'Bytes=%s-%s' % (range_start, range_end - 1)}, timeout=(10, 30)
				)
				response.raise_for_status()
				break
			except requests.HTTPError as error:
				logging.warning('Request error for %s : %s (Attempt %s/%s)', file_url, error, retry_count, max_retry_count)
			except requests.exceptions.Timeout:
				logging.warning('Timeout error for %s (Attempt %s/%s)', file_url, retry_count, max_retry_count)
			except requests.exceptions.SSLError as error:
				logging.warning('SSL Handshake failed for %s : %s (Attempt %s/%s)', file_url, error, retry_count, max_retry_count)
			except requests.exceptions.RequestException as error:
				logging.warning('Network error for  %s : %s (Attempt %s/%s)', file_url, error, retry_count, max_retry_count)
		else:
			raise RuntimeError('Could not download file %s' % file_url)

		if zipped:
			fits_file.write(decompressor.decompress(response.content))
		else:
			fits_file.write(response.content)

		# It is necessary to rewind the file to pass it to the fits library
		fits_file.seek(0)

		# Try to read a full header from the pseudo file, if header is partial, an IOError will be raised
		try:
			fits_header = astropy.io.fits.Header.fromfile(fits_file)
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


class JsonSerializer(slumber.serialize.JsonSerializer):
	"""JSON serializer that encodes date and time objects as ISO 8601 strings and
	serializes NaN and infinity as null.
	"""

	def dumps(self, data):
		return simplejson.dumps(data, ignore_nan=True, cls=DateTimeEncoder)


class DateTimeEncoder(simplejson.JSONEncoder):
	"""Encode a date and time object into an ISO 8601 string"""

	def default(self, o):
		if isinstance(o, (datetime.datetime, datetime.date, datetime.time)):
			return o.isoformat()

		return super().default(o)
