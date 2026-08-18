#!/usr/bin/env python3
"""Script to extract metadata from the SPoCA Coronal Hole archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	DataLocationFromLocalFile,
	MetadataFromFitsHeader,
	ProviderFromLocalFitsFile,
	RESTfulApi,
	utils,
)

DATASET = 'SPoCA Coronal Hole'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '/data/spoca/spoca4tap/rob_spoca_ch/ch_map'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://spoca.oma.be/spoca4tap/rob_spoca_ch/ch_map/'

	# The base thumbnail URL to build the default tumbnail_url, uses the fits2thumbnail service of the SVO to convert FITS to png
	BASE_THUMBNAIL_URL = 'https://spoca.oma.be/spoca4tap/rob_spoca_ch/ch_map_overlay/'

	def get_thumbnail_url(self):
		# Construct the thumbnail URL from the filename
		return self.BASE_THUMBNAIL_URL + Path(self.local_file).with_suffix('.png').name


class Metadata(MetadataFromFitsHeader):
	def get_date_beg(self):
		return self.extract_field_value('date_obs')

	def get_date_end(self):
		return self.extract_field_value('date_obs')

	# TODO is there a better value for this
	def get_wavemin(self):
		return None

	def get_wavemax(self):
		return None

	def get_thrawar(self):
		return False

	def get_tracked(self):
		return True


class Provider(ProviderFromLocalFitsFile):
	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation

	HDU_NAME_OR_INDEX = 1


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
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = utils.iter_files(args.fits_files, args.min_modif_time)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
