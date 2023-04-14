#!/usr/bin/env python3
'''Script to extract metadata from the XRT online archive and submit it to the SOLARNET Virtual Observatory'''

import sys
import argparse
import logging
from pathlib import Path
from pprint import pformat

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import MetadataFromFitsHeader, DataLocationFromUrl, RESTfulApi, ProviderFromFitsUrl, utils


DATASET = 'XRT level 1'
BASE_FILE_URL = 'https://sao.virtualsolar.org/VSO/DataProvider/SAO/hinode/xrt/level1/'

class DataLocation(DataLocationFromUrl):
	
	BASE_FILE_URL = BASE_FILE_URL
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		return super().get_file_url()[:-5] + '.jpeg'


class Metadata(MetadataFromFitsHeader):
	
	def get_field_date_beg(self):
		return self.get_field_value('date_obs')
	
	def get_field_wavemin(self):
		return 0.88
	
	def get_field_wavemax(self):
		return 33.5


class Provider(ProviderFromFitsUrl):
	
	HEADER_SIZE = 6 * 2880

	METADATA_CLASS = Metadata
	
	DATA_LOCATION_CLASS = DataLocation


if __name__ == "__main__":

	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS URL the SVO')
	parser.add_argument('--verbose', '-v', choices = ['DEBUG', 'INFO', 'ERROR'], default = 'INFO', help='Set the logging level (default is INFO)')
	parser.add_argument('urls', metavar = 'URL', nargs='*', default = [BASE_FILE_URL], help='A URL to a FITS file to submit to the SVO (also accept apache style directory indexing, don\'t forget to end diretories URL with a slash)')
	parser.add_argument('--auth-file', '-a', default='./.svo_auth', help='A file containing the username (email) and API key separated by a colon of the owner of the metadata')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')
	parser.add_argument('--min-modif-time', '-m', type=utils.parse_date_time_string, help='Only submit record if the modification_date is after that date')

	args = parser.parse_args()
	
	# Setup the logging
	logging.basicConfig(level = getattr(logging, args.verbose), format = '%(asctime)s %(levelname)-8s: %(message)s')
	
	try:
		provider = Provider(RESTfulApi(auth_file = args.auth_file, debug = args.verbose == 'DEBUG'), DATASET)
	except Exception as why:
		logging.critical('Could not create provider: %s', why)
		raise
	
	provider.submit_new_metadata(utils.iter_urls(args.urls, min_modification_time = args.min_modif_time), args.dry_run)
