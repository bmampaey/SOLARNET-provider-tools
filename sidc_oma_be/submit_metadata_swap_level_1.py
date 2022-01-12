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
from utils import parse_date_time, get_svo_auth, iter_files

DATASET = 'SWAP level 1'

class DataLocationRecord(DataLocationRecord):
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = '/data/proba2/swap/bsd/'
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'http://proba2.oma.be/swap/data/bsd/'
	
	BASE_THUMBNAIL_URL = 'http://proba2.oma.be/swap/data/qlviewer/'
	
	# The thumbnail URL depends on the metadata
	def __init__(self, metadata_date_obs, metadata_file_tmr, *args, **kwargs):
		super().__init__(*args, **kwargs)
		if self.thumbnail_url is None:
				self.thumbnail_url = self.BASE_THUMBNAIL_URL + str(Path(metadata_date_obs.strftime('%Y/%m/%d'), metadata_file_tmr).with_suffix('.png'))

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
	parser.add_argument('--verbose', '-v', choices = ['DEBUG', 'INFO', 'ERROR'], default = 'INFO', help='Set the logging level (default is INFO)')
	parser.add_argument('fits_files', metavar = 'FITS FILE', nargs='+', help='A FITS file to submit to the SVO (also accept glob pattern)')
	parser.add_argument('--auth-file', '-a', default='./.svo_auth', help='A file containing the username (email) and API key separated by a colon of the owner of the metadata')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')
	parser.add_argument('--min-modif-time', '-m', type=parse_date_time, help='Only submit file if the modification time is after that date')
	
	args = parser.parse_args()
	
	# Setup the logging
	logging.basicConfig(level = getattr(logging, args.verbose), format = '%(asctime)s %(levelname)-8s: %(message)s')
	
	try:
		username, api_key = get_svo_auth(args.auth_file)
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
	
	for fits_file in iter_files(args.fits_files, args.min_modif_time):
		
		try:
			record = MetadataRecord(fits_file = fits_file, keywords = keywords)
			metadata = record.get_resource_data()
		except Exception as why:
			logging.critical('Could not extract metadata for FITS file "%s": %s', fits_file, why)
			continue
		
		try:
			record = DataLocationRecord(metadata['date_obs'], metadata['file_tmr'], fits_file)
			data_location = record.get_resource_data()
		except Exception as why:
			logging.critical('Could not extract data location for FITS file "%s": %s', fits_file, why)
			continue
		
		data_location['dataset'] = api.dataset['resource_uri']
		metadata['data_location'] = data_location
		
		logging.info('Creating metadata resource for FITS file "%s"', fits_file)
		logging.debug(pformat(metadata, indent = 2, width = 200))
		
		if args.dry_run:
			logging.info('Called with dry-run option, not creating anything')
		else:
			try:
				result = api.create_metadata(metadata)
			except Exception as why:
				logging.error('Could not create metadata record for FITS file "%s": %s', fits_file, why)
			else:
				logging.info('Created metadata resource "%s" for FITS file "%s"', result['resource_uri'], fits_file)
