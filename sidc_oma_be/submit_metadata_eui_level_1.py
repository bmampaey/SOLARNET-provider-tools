#!/usr/bin/env python3
'''Script to extract metadata from the EUI archive and submit it to the SOLARNET Virtual Observatory'''

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

DATASET = 'EUI level 1'

class DataLocationRecord(DataLocationRecord):
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = '/data/EUI/managed/releases/202107_release_3.0/L1/'
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://wwwbis.sidc.be/EUI/data/releases/202107_release_3.0/L1/'
	
	# The base directory to to check for the thumbnail file
	BASE_THUMBNAIL_DIRECTORY = '/data/EUI/managed/releases/202107_release_3.0/L3/'
	
	# The base thumbnail URL to build the default tumbnail_url, uses the image2thumbnail service of the SVO to convert JP2 to png
	BASE_THUMBNAIL_URL = 'https://solarnet2.oma.be/service/image2thumbnail/?url=https://wwwbis.sidc.be/EUI/data/releases/202107_release_3.0/L3/'
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		# The thumbnail URL is constructed from the file_path but with a jp2 extension
		file_path = Path(self.get_file_path())
		file_path = file_path.with_name('solo_L3_' + file_path.name[8:].rsplit('_', 1)[0] + '_V01.jp2')
		if Path(self.BASE_THUMBNAIL_DIRECTORY, file_path).is_file():
			return self.BASE_THUMBNAIL_URL + str(file_path)
		else:
			return None

class MetadataRecord(MetadataRecord):
	
	def get_field_date_end(self):
		return self.get_field_value('date_beg') + timedelta(seconds=self.get_field_value('xposure'))
	
	def get_field_wavemin(self):
		return float(self.fits_header['WAVEMIN']) / 10.0
	
	def get_field_wavemax(self):
		return float(self.fits_header['WAVEMAX']) / 10.0
	
	def get_field_oid(self):
		'''Return the observation id (oid) for the record. Override to adapt to the desired behavior'''
		if self.oid:
			return self.oid
		else:
			return self.get_field_value('filename').rsplit('_', 2)[1]


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
			record = DataLocationRecord(fits_file)
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
