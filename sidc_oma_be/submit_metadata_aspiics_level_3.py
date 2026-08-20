#!/usr/bin/env python3
"""Script to extract metadata from the ASPIICS level 2 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path, PurePosixPath

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	DataLocationFromLocalFile,
	MetadataFromFitsHeader,
	ProviderFromLocalFitsFile,
	RESTfulApi,
	utils,
)

DATASET = 'ASPIICS level 3'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '/data/p3sc_science_data/L3/v03/'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://p3sc.oma.be/datarepfiles/L3/v03/'


class Metadata(MetadataFromFitsHeader):
	def get_oid(self):
		# Use the filename as the oid, it contains the version number and the date
		return PurePosixPath(self.extract_field_value('filename')).stem

	def get_wavemin(self):
		return self.extract_field_value('WAVEMIN') / 10.0

	def get_wavemax(self):
		return self.extract_field_value('WAVEMAX') / 10.0


class Provider(ProviderFromLocalFitsFile):
	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation

	HDU_NAME_OR_INDEX = 0

	BASE_THUMBNAIL_URL = 'https://p3sc.oma.be/datarepfiles/L3_png/v03'

	def get_resource_data(self, file_path):
		# The thumbnail URL requires the orbit_id in the metadata
		resource_data = super().get_resource_data(file_path)
		resource_data['data_location']['thumbnail_url'] = (
			f'{self.BASE_THUMBNAIL_URL}/orbit_{int(resource_data["orbit_id"]):04d}/{PurePosixPath(resource_data["filename"]).stem}.png'
		)
		return resource_data


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Extract metadata from FITS files and submit them to the SVO for dataset "%s"' % DATASET
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
		'fits_files',
		metavar='FITS FILE',
		nargs='+',
		help='Path to a FITS file to process (also accept glob pattern)',
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(
		level=getattr(logging, args.verbose),
		format='%(asctime)s %(levelname)-8s: %(message)s',
	)

	try:
		provider = Provider(RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'), DATASET)
	except Exception as error:
		logging.critical('Could not create exractor: %s', error)
		raise

	items = utils.iter_files(args.fits_files, args.min_modif_time)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
