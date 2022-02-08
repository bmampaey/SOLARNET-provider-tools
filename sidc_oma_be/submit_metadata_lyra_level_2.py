#!/usr/bin/env python3
'''Script to extract metadata from the LYRA archive and submit it to the SOLARNET Virtual Observatory'''

import sys
import argparse
import logging
from pathlib import Path
from datetime import timedelta

# HACK to make sure the provider_tools package is findable
sys.path.append("..")
from provider_tools import MetadataFromFitsFile, DataLocationFromLocalFile, RESTfulApi, ProviderFromLocalFitsFile, utils


DATASET = 'LYRA level 2'

class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = '/data/proba2/lyra/bsd/'
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'http://proba2.oma.be/lyra/data/bsd/'


class Metadata(MetadataFromFitsFile):
	
	def get_field_date_beg(self):
		return self.get_field_value('date_obs')
	
	# Taken from the instrument description
	def get_field_wavemin(self):
		return 6
	
	def get_field_wavemax(self):
		return 222


class Provider(ProviderFromLocalFitsFile):
	
	METADATA_CLASS = Metadata
	
	DATA_LOCATION_CLASS = DataLocation
	
	BASE_THUMBNAIL_URL = 'https://proba2.sidc.be/lyra/data/Level4calibrated/'
	
	def get_resource_data(self, fits_file):
		'''Extract the data for the metadata and data_location resource from a FITS file'''
		# The thumbnail URL depends on the metadata
		resource_data = super().get_resource_data(fits_file)
		resource_data['data_location']['thumbnail_url'] = self.BASE_THUMBNAIL_URL + 'LyraL4C%s.png' % resource_data['date_obs'].strftime('%Y%m%d')
		return resource_data

if __name__ == "__main__":

	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS file to the SVO')
	parser.add_argument('--verbose', '-v', choices = ['DEBUG', 'INFO', 'ERROR'], default = 'INFO', help='Set the logging level (default is INFO)')
	parser.add_argument('fits_files', metavar = 'FITS FILE', nargs='+', help='A FITS file to submit to the SVO (also accept glob pattern)')
	parser.add_argument('--auth-file', '-a', default='./.svo_auth', help='A file containing the username (email) and API key separated by a colon of the owner of the metadata')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')
	parser.add_argument('--min-modif-time', '-m', type=utils.parse_date_time_string, help='Only submit file if the modification time is after that date')
	
	args = parser.parse_args()
	
	# Setup the logging
	logging.basicConfig(level = getattr(logging, args.verbose), format = '%(asctime)s %(levelname)-8s: %(message)s')
	
	try:
		provider = Provider(RESTfulApi(auth_file = args.auth_file), DATASET)
	except Exception as why:
		logging.critical('Could not create provider: %s', why)
		raise
	
	provider.submit_new_metadata(utils.iter_files(args.fits_files, args.min_modif_time), args.dry_run)
