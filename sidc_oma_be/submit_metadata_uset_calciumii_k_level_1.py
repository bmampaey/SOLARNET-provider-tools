#!/usr/bin/env python3
"""Script to extract metadata from the USET archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path, PurePosixPath

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	DataLocationFromUrl,
	MetadataFromFitsHeader,
	ProviderFromFitsUrl,
	RESTfulApi,
	utils,
)

DATASET = 'USET CalciumII-K level 1'
BASE_FILE_URL = 'https://www.sidc.be/uset/data/cameras/output_img/FTS/L1c/USET_CalciumII-K/'


class DataLocation(DataLocationFromUrl):
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = BASE_FILE_URL

	# The base thumbnail URL to build the default tumbnail_url, uses the fits2thumbnail service of the SVO to convert FITS to png
	BASE_THUMBNAIL_URL = 'https://www.sidc.be/uset/data/cameras/output_img/png/L1c_png_750_text/USET_CalciumII-K/'

	def get_thumbnail_url(self):
		# Construct the thumbnail URL from the filename
		return self.BASE_THUMBNAIL_URL + str(PurePosixPath(self.get_file_path()).with_suffix('.png'))


class Metadata(MetadataFromFitsHeader):
	def get_date_end(self):
		return self.extract_field_value('date_beg') + timedelta(seconds=self.extract_field_value('xposure'))

	# These are fixed values that coresspond to wavelnth -/+ 1/2 waveband
	def get_wavemin(self):
		return 393.235

	def get_wavemax(self):
		return 393.505


class Provider(ProviderFromFitsUrl):
	HEADER_SIZE = 2 * 2880

	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation


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
		'fits_urls',
		metavar='URL',
		nargs='*',
		default=[BASE_FILE_URL],
		help="Path to a FITS file to process (also accept apache style directory indexing, don't forget to end diretories URL with a slash)",
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(level=getattr(logging, args.verbose), format='%(asctime)s %(levelname)-8s: %(message)s')

	try:
		provider = Provider(RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'), DATASET)
	except Exception as error:
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = utils.iter_urls(args.fits_urls, extension='.FTS', min_modification_time=args.min_modif_time)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
