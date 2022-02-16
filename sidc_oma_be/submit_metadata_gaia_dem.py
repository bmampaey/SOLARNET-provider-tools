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
from provider_tools import MetadataFromTapRecord, DataLocationFromTapRecord, RESTfulApi, ProviderFromTapRecord, utils


DATASET = 'GAIA DEM'
TAP_SERVICE_URL = 'http://idoc-dachs.ias.u-psud.fr/tap/'
TABLE_NAME = 'gaia_dem.epn_core'


class DataLocation(DataLocationFromTapRecord):
	
	def get_file_size(self):
		'''Override to return the correct size of file in bytes'''
		# The access_estsize is not the correct file size, so get the actual file size by making a HEAD request on the file
		if self.file_size is not None:
			return self.file_size
		else:
			response = requests.head(self.get_file_url())
			return response.headers['Content-Length']
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		return super().get_thumbnail_url()[:-6] + '1024.png'


class Metadata(MetadataFromTapRecord):
	
	def get_field_oid(self):
		'''Return the observation id (oid) for the resource. Override to adapt to the desired behavior'''
		if self.oid:
			return self.oid
		else:
			return '%s_%s' % (self.get_field_value('granule_gid'), self.get_field_value('date_beg').strftime('%Y%m%d%H%M%S'))


class Provider(ProviderFromTapRecord):
	
	METADATA_CLASS = Metadata
	
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