#!/usr/bin/env python3
"""Script to extract metadata from the EUI archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from datetime import timedelta
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

DATASET = 'EUI level 1'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '/data/EUI/managed/L1/'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://www.sidc.be/EUI/data/L1/'

	# The base directory to to check for the thumbnail file
	BASE_THUMBNAIL_DIRECTORY = '/data/EUI/managed/L3/'

	# The base thumbnail URL to build the default tumbnail_url, uses the image2thumbnail service of the SVO to convert JP2 to png
	BASE_THUMBNAIL_URL = 'https://solarnet.oma.be/service/image2thumbnail/?url=https://www.sidc.be/EUI/data/L3/'

	def get_thumbnail_url(self):
		# The thumbnail URL is constructed from the file_path but with a jp2 extension
		# Then use the SVO thumbnail service to convert the jp2 to png
		file_path = Path(self.get_file_path())
		thumbnails = sorted(
			Path(self.BASE_THUMBNAIL_DIRECTORY, file_path.parent).glob('*' + '_'.join(file_path.name.split('_')[2:4]) + '*.jp2'),
			reverse=True,
		)
		if thumbnails:
			return self.BASE_THUMBNAIL_URL + str(thumbnails[0].relative_to(self.BASE_THUMBNAIL_DIRECTORY))
		else:
			return None


class Metadata(MetadataFromFitsHeader):
	def get_date_end(self):
		return self.extract_field_value('date_beg') + timedelta(seconds=self.extract_field_value('xposure'))

	def get_wavemin(self):
		if 'WAVEMIN' in self.fits_header:
			return self.extract_field_value('WAVEMIN') / 10.0
		else:
			return None

	def get_wavemax(self):
		if 'WAVEMAX' in self.fits_header:
			return self.extract_field_value('WAVEMAX') / 10.0
		else:
			return None

	def get_oid(self):
		# Use the filename as the oid, it contains the version number and the date
		return PurePosixPath(self.extract_field_value('filename')).stem


class Provider(ProviderFromLocalFitsFile):
	# Files are tiled compressed, so the important header is in the second HDU
	HDU_NAME_OR_INDEX = 1

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
		'fits_files',
		metavar='FITS FILE',
		nargs='+',
		help='Path to a FITS file to process (also accept glob pattern)',
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(level=getattr(logging, args.verbose), format='%(asctime)s %(levelname)-8s: %(message)s')

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
