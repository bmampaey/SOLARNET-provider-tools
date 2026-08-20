#!/usr/bin/env python3
"""Script to extract metadata from the Leibniz-KIS TAP service and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import DataLocationFromTapRecord, MetadataFromTapRecord, ProviderFromTapRecord, RESTfulApi, utils

DATASET = 'LARS level 1'
TAP_SERVICE_URL = 'http://dachs.sdc.leibniz-kis.de/tap'
TABLE_NAME = 'lars.epn_core'


class DataLocation(DataLocationFromTapRecord):
	def get_file_size(self):
		# The access_estsize is not always set
		try:
			file_size = super().get_file_size()
		except ValueError:
			file_size = 0
			logging.warning('File size is unknown, setting to 0')

		return int(file_size)

	def get_file_path(self):
		try:
			file_path = super().get_file_path()
		except ValueError:
			file_path = ''

		if not file_path:
			file_path = self.tap_record['granule_uid'] + '.tar'
			logging.warning('Setting file path to arbitrary value %s', file_path)

		return file_path


class Provider(ProviderFromTapRecord):
	METADATA_CLASS = MetadataFromTapRecord

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
		'--no-submit',
		dest='submit',
		action='store_false',
		help='Do not submit the metadata to the server; only print it',
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
