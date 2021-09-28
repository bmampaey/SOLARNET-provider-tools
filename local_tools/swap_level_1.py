#!/usr/bin/env python3
'''Script to extract metadata from the SWAP archive and submit it to the SOLARNET Virtual Observatory'''

import argparse
import logging
from glob import iglob
from itertools import chain
from pathlib import Path
from datetime import timedelta
from pprint import pprint

from records import MetadataRecord, DataLocationRecord
from restful_api import RESTfulApi

DATASET = 'SWAP level 1'
USER_API_EMAIL = None
USER_API_KEY = None

class DataLocationRecord(DataLocationRecord):
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = '/data/proba2/swap/bsd/'
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'http://proba2.oma.be/swap/data/bsd/'
	
	def get_thumbnail_url(self):
		'''Return the proper URL for the thumbnail'''
		return 'http://proba2.oma.be/swap/data/qlviewer/%s/%s' % (self.metadata['date_obs'].strftime('%Y/%m/%d'), Path(self.metadata['file_tmr']).with_suffix('.png'))


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
	parser.add_argument('fits_files', metavar = 'FITS FILE', nargs = '+', help='A FITS file to submit to the SVO')
	parser.add_argument('--username', '-u', default=USER_API_EMAIL, help='The username (email) of the user owning the data')
	parser.add_argument('--api-key', '-k', default=USER_API_KEY, help='The API key of the user owning the data')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')

	args = parser.parse_args()
	
	# Setup the logging
	if args.debug:
		logging.basicConfig(level = logging.DEBUG, format = '%(levelname)-8s: %(funcName)s %(message)s')
	else:
		logging.basicConfig(level = logging.INFO, format = '%(levelname)-8s: %(message)s')
	
	try:
		api = RESTfulApi(DATASET, args.username, args.api_key)
	except Exception as why:
		logging.critical('Could not create api for dataset "%s": %s', DATASET, why)
		raise
	
	try:
		keywords = api.get_keywords()
	except Exception as why:
		logging.critical('Could not retrieve keywords for dataset "%s": %s', DATASET, why)
		raise
	
	fits_files = chain.from_iterable(iglob(fits_files) for fits_files in args.fits_files)
	
	for fits_file in fits_files:
		
		try:
			record = MetadataRecord(fits_file = fits_file, keywords = keywords)
			metadata = record.get_metadata()
		except Exception as why:
			logging.critical('Could not extract metadata for FITS file "%s": %s', fits_file, why)
			continue
		
		try:
			record = DataLocationRecord(api.dataset['resource_uri'], fits_file = fits_file, metadata = metadata)
			data_location = record.get_data_location()
		except Exception as why:
			logging.critical('Could not extract data location for FITS file "%s": %s', fits_file, why)
			continue
		
		metadata['data_location'] = data_location
		
		if args.dry_run:
			pprint(metadata, indent = 2)
		else:
			try:
				result = api.create_metadata(metadata)
			except Exception as why:
				logging.error('Could not create metadata record for FITS file "%s": %s', fits_file, why)
			else:
				logging.info('Created metadata record "%s" for FITS file "%s"', result['resource_uri'], fits_file)
