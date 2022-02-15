#!/usr/bin/env python3
'''Script to extract metadata from the MEDOC TAP serviceand submit it to the SOLARNET Virtual Observatory'''

import sys
import argparse
import logging
import requests
from pathlib import Path

from pprint import pformat


# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import MetadataFromTapRecord, DataLocationFromTapRecord, RESTfulApi, Provider, utils


DATASET = 'EIT synoptic'
TAP_SERVICE_URL = 'http://idoc-dachs.ias.u-psud.fr/tap/'
TABLE_NAME = 'eit_syn.epn_core'


class DataLocation(DataLocationFromTapRecord):
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		# The access_estsize is not the correct file size, so get the actual file size by making a HEAD request on the file
		if self.file_size is not None:
			return self.file_size
		else:
			response = requests.head(self.get_file_url())
			return response.headers['Content-Length']


class Metadata(MetadataFromTapRecord):
	
	def get_field_oid(self):
		'''Return the observation id (oid) for the resource. Override to adapt to the desired behavior'''
		# The granule_uid contains a _fts suffix, so use the obs_id that is the same thing without the ugly suffix
		if self.oid:
			return self.oid
		else:
			return self.get_field_value('obs_id')

class Provider(Provider):
	
	METADATA_CLASS = Metadata
	
	DATA_LOCATION_CLASS = DataLocation
	
	def get_resource_data(self, record):
		'''Extract the data for the metadata and data_location resource from a TAPRecord'''
		metadata = self.METADATA_CLASS(record = record, keywords = self.keywords)
		data_location = self.DATA_LOCATION_CLASS(record)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data
	
	def submit_new_metadata(self, records, dry_run = False):
		'''Create a new metadata and data_location resources a TAP service'''
		
		for record in records:
			
			logging.info('Creating metadata and data_location resource for record "%s"', record)
			
			try:
				resource_data = self.get_resource_data(record)
			except Exception as why:
				logging.critical('Could not extract resource data for record "%s": %s', record, why)
				raise
			else:
				logging.debug(pformat(resource_data, indent = 2, width = 200))
			
			if dry_run:
				logging.info('Called with dry-run option, not submitting anything')
			else:
				try:
					result = self.create(resource_data)
				except Exception as why:
					logging.error('Could not create new metadata or data_location resource for record "%s": %s', record, why)
				else:
					logging.info('Created new metadata resource "%s" for record "%s"', result['resource_uri'], record)

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
