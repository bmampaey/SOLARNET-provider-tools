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
	ExtractorFromLocalFitsFile,
	MetadataFromFitsHeader,
	RESTfulApi,
	utils,
)

DATASET = 'EUI level 2 release 7.0'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '/data/EUI/managed/L2/'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://www.sidc.be/EUI/data/L2/'

	# The base directory to to check for the thumbnail file
	BASE_THUMBNAIL_DIRECTORY = '/data/EUI/managed/L3/'

	# The base thumbnail URL to build the default tumbnail_url, uses the image2thumbnail service of the SVO to convert JP2 to png
	BASE_THUMBNAIL_URL = 'https://solarnet2.oma.be/service/image2thumbnail/?url=https://www.sidc.be/EUI/data/L3/'

	def get_thumbnail_url(self):
		"""Override to return the proper URL for the thumbnail"""

		# The thumbnail URL is constructed from the file_path but with a jp2 extension
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
	def get_field_date_end(self):
		return self.get_field_value('date_beg') + timedelta(seconds=self.get_field_value('xposure'))

	def get_field_wavemin(self):
		if 'WAVEMIN' in self.fits_header:
			return float(self.fits_header['WAVEMIN']) / 10.0
		else:
			return None

	def get_field_wavemax(self):
		if 'WAVEMAX' in self.fits_header:
			return float(self.fits_header['WAVEMAX']) / 10.0
		else:
			return None

	def get_field_oid(self):
		return PurePosixPath(self.get_field_value('filename')).stem


class Extractor(ExtractorFromLocalFitsFile):
	# Files are tiled compressed, so the important header is in the second HDU
	HDU_NAME_OR_INDEX = 1

	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS file to the SVO')
	parser.add_argument(
		'--verbose', '-v', choices=['DEBUG', 'INFO', 'ERROR'], default='INFO', help='Set the logging level (default is INFO)'
	)
	parser.add_argument(
		'fits_files', metavar='FITS FILE', nargs='+', help='A FITS file to submit to the SVO (also accept glob pattern)'
	)
	parser.add_argument(
		'--auth-file',
		'-a',
		default='./.svo_auth',
		help='A file containing the username (email) and API key separated by a colon of the owner of the metadata',
	)
	parser.add_argument(
		'--min-modif-time',
		'-m',
		type=utils.parse_date_time_string,
		help='Only submit file if the modification time is after that date',
	)
	parser.add_argument(
		'--output-file',
		'-o',
		help='JSONL file for the output, if not provided will output to stdout',
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(level=getattr(logging, args.verbose), format='%(asctime)s %(levelname)-8s: %(message)s')

	try:
		exractor = Extractor(RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'), DATASET)
	except Exception as error:
		logging.critical('Could not create exractor: %s', error)
		raise

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			exractor.write_metadata(utils.iter_files(args.fits_files, args.min_modif_time), output_file)
	else:
		exractor.write_metadata(utils.iter_files(args.fits_files, args.min_modif_time), sys.stdout)
