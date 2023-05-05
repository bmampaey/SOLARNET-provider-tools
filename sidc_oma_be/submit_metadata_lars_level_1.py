#!/usr/bin/env python3
'''Script to extract metadata from the Leibniz-KIS TAP service and submit it to the SOLARNET Virtual Observatory'''

import sys
import argparse
import logging
import requests
from pathlib import Path
from pprint import pformat

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import MetadataFromTapRecord, DataLocationFromTapRecord, RESTfulApi, ProviderFromTapRecord, utils


DATASET = 'LARS level 1'
TAP_SERVICE_URL = 'http://dachs.sdc.leibniz-kis.de/tap'
TABLE_NAME = 'lars.epn_core'


class DataLocation(DataLocationFromTapRecord):
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		try:
			file_size = super().get_file_size()
		except ValueError as why:
			file_size = 0
			logging.warning('File size is unknown, setting to 0')
		
		return int(file_size)
	
	def get_file_path(self):
		
		try:
			file_path = super().get_file_path()
		except ValueError as why:
			file_path = ''
		
		if not file_path:
			file_path = self.record['granule_uid'] + '.tar'
			logging.warning('Setting file path to arbitrary value %s', file_path)
		
		return file_path

class Provider(ProviderFromTapRecord):
	
	METADATA_CLASS = MetadataFromTapRecord
	
	DATA_LOCATION_CLASS = DataLocation


if __name__ == "__main__":

	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a TAP service to the SVO')
	parser.add_argument('--verbose', '-v', choices = ['DEBUG', 'INFO', 'ERROR'], default = 'INFO', help='Set the logging level (default is INFO)')
	parser.add_argument('--auth-file', '-a', default='./.svo_auth', help='A file containing the username (email) and API key separated by a colon of the owner of the metadata')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')
	parser.add_argument('--min-modif-time', '-m', type=utils.parse_date_time_string, help='Only submit record if the modification_date is after that date')
	parser.add_argument('--batch-size', '-c', type=int, default = 1000, help='The number of records to fetch from the TAP service in one call')

	args = parser.parse_args()
	
	# Setup the logging
	logging.basicConfig(level = getattr(logging, args.verbose), format = '%(asctime)s %(levelname)-8s: %(message)s')
	
	try:
		provider = Provider(RESTfulApi(auth_file = args.auth_file, debug = args.verbose == 'DEBUG'), DATASET)
	except Exception as why:
		logging.critical('Could not create provider: %s', why)
		raise
	
	provider.submit_new_metadata(utils.iter_tap_records(TAP_SERVICE_URL, TABLE_NAME, max_count = args.batch_size, min_modification_time = args.min_modif_time), args.dry_run)
