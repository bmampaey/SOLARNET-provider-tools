#!/usr/bin/env python3
"""Script to extract metadata from the MEDOC TAP serviceand submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path

import requests

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import DataLocationFromTapRecord, MetadataFromTapRecord, ProviderFromTapRecord, RESTfulApi, utils

DATASET = 'EIT synoptic'
TAP_SERVICE_URL = 'https://idoc-dachs.ias.u-psud.fr/tap/'
TABLE_NAME = 'eit_syn.epn_core'


class DataLocation(DataLocationFromTapRecord):
	def get_file_size(self):
		# The access_estsize is not the correct file size, so get the actual file size by making a HEAD request on the file
		# If the request fail, return 0 so that it is easy to find the failed ones and retry later
		if self.file_size is not None:
			return self.file_size
		else:
			try:
				response = requests.head(self.get_file_url())
				return response.headers['Content-Length']
			except Exception as error:
				logging.error('Could not retrieve size of file %s: %s', self.get_file_url(), error)
				return 0


class Metadata(MetadataFromTapRecord):
	def get_oid(self):
		# The granule_uid contains a _fts suffix, so use the obs_id that is the same thing without the ugly suffix
		return self.extract_field_value('obs_id')


class Provider(ProviderFromTapRecord):
	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Extract metadata from a TAP service and submit them to the SVO for dataset "%s"' % DATASET
	)
	parser.add_argument(
		'--verbose',
		'-v',
		choices=['DEBUG', 'INFO', 'ERROR'],
		default='INFO',
		help='Set the logging level (default is INFO)',
	)
	parser.add_argument(
		'--auth-file',
		'-a',
		default='./.svo_auth',
		help='File containing authentication credentials for the SVO (in the format email:API key)',
	)
	parser.add_argument(
		'--min-modif-time',
		'-m',
		type=utils.parse_date_time_string,
		help='Only extract the metadata if the modification time is later than the minimum',
	)
	parser.add_argument(
		'--submit',
		default=True,
		action=argparse.BooleanOptionalAction,
		help='If set (the default), submit the metadata to the server; if negated with --no-submit, only print the metadata',
	)
	parser.add_argument(
		'--output-file',
		'-o',
		help='Path to a JSONL file to which the metadata will be written, instead of printed',
	)
	parser.add_argument(
		'--batch-size', '-c', type=int, default=1000, help='The number of records to fetch from the TAP service in one call'
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(level=getattr(logging, args.verbose), format='%(asctime)s %(levelname)-8s: %(message)s')

	try:
		provider = Provider(RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'), DATASET)
	except Exception as error:
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = utils.iter_tap_records(
		TAP_SERVICE_URL, TABLE_NAME, max_count=args.batch_size, min_modification_time=args.min_modif_time
	)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
