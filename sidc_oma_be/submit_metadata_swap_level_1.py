#!/usr/bin/env python3
'''Script to extract metadata from the SWAP archive and submit it to the SOLARNET Virtual Observatory'''

import argparse
import logging
import os
from glob import iglob
from pathlib import Path
from datetime import datetime, timedelta
from pprint import pformat
from dateutil.parser import parse, ParserError

from api import MetadataRecord, DataLocationRecord, RESTfulApi

DATASET = 'SWAP level 1'

class DataLocationRecord(DataLocationRecord):
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = '/data/proba2/swap/bsd/'
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'http://proba2.oma.be/swap/data/bsd/'


class MetadataRecord(MetadataRecord):
	
	def get_field_date_beg(self):
		return self.get_field_value('date_obs')
	
	def get_field_date_end(self):
		return self.get_field_value('date_obs') + timedelta(seconds=self.get_field_value('exptime'))
	
	# TODO is there a better value for this
	def get_field_wavemin(self):
		return self.get_field_value('wavelnth') / 10.0
	
	def get_field_wavemax(self):
		return self.get_field_value('wavelnth') / 10.0


if __name__ == "__main__":

	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS file to the SVO')
	parser.add_argument('--debug', '-d', action='store_true', help='Set the logging level to debug')
	parser.add_argument('fits_files', metavar = 'FITS FILE', help='A FITS file to submit to the SVO (also accept glob pattern)')
	parser.add_argument('--auth-file', '-a', default='./.svo_auth', help='A file containing the username (email) and API key separated by a colon of the owner of the metadata')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')
	parser.add_argument('--min-modif-time', '-m', help='Only submit file if the modification time is after that date')
	
	args = parser.parse_args()
	
	# Setup the logging
	if args.debug:
		logging.basicConfig(level = logging.DEBUG, format = '%(levelname)-8s: %(funcName)s %(message)s')
	else:
		logging.basicConfig(level = logging.INFO, format = '%(levelname)-8s: %(message)s')
	
	if args.min_modif_time:
		try:
			min_modif_time = parse(args.min_modif_time, default = datetime(2000, 1, 1))
		except ParserError as why:
			logging.critical('Parameter min-modif-time is not a valid date: %s', why)
			raise
		else:
			min_modif_time = min_modif_time.timestamp()
	else:
		min_modif_time = None
	
	if args.dry_run:
		username, api_key = None, None
	else:
		try:
			with open(args.auth_file, 'r') as file:
				username, api_key = file.read().strip().split(':', 1)
		except Exception as why:
				logging.critical('Could not read auth from file "%s": %s', args.auth_file, why)
				raise
	
	try:
		api = RESTfulApi(DATASET, username, api_key)
	except Exception as why:
		logging.critical('Could not create api for dataset "%s": %s', DATASET, why)
		raise
	
	try:
		keywords = api.get_keywords()
	except Exception as why:
		logging.critical('Could not retrieve keywords for dataset "%s": %s', DATASET, why)
		raise
	
	for fits_file in iglob(args.fits_files, recursive = True):
		if min_modif_time and os.path.getmtime(fits_file) < min_modif_time:
			logging.info('Skipping FITS file "%s": file modif time earlier than specified min', fits_file)
			continue
		
		try:
			record = DataLocationRecord(api.dataset['resource_uri'], fits_file = fits_file)
			data_location = record.get_data_location()
		except Exception as why:
			logging.critical('Could not extract data location for FITS file "%s": %s', fits_file, why)
			continue
		
		try:
			record = MetadataRecord(fits_file = fits_file, keywords = keywords)
			metadata = record.get_metadata()
		except Exception as why:
			logging.critical('Could not extract metadata for FITS file "%s": %s', fits_file, why)
			continue
		
		# The thumbnail URL depends on the metadata
		data_location['thumbnail_url'] = 'http://proba2.oma.be/swap/data/qlviewer/%s/%s' % (metadata['date_obs'].strftime('%Y/%m/%d'), Path(metadata['file_tmr']).with_suffix('.png'))
		metadata['data_location'] = data_location
		
		if args.dry_run:
			logging.info('Metadata record for FITS file "%s" :\n%s', fits_file, pformat(metadata, indent = 2, width = 200))
		else:
			try:
				result = api.create_metadata(metadata)
			except Exception as why:
				logging.error('Could not create metadata record for FITS file "%s": %s', fits_file, why)
			else:
				logging.info('Created metadata record "%s" for FITS file "%s"', result['resource_uri'], fits_file)
