#!/usr/bin/env python3
"""Script to extract metadata from the ASPIICS level 2 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path, PurePosixPath

from provider_tools.utils import get_fits_header_from_local_file

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

	# The base thumbnail URL to build the default tumbnail_url, uses the fits2thumbnail service of the SVO to convert FITS to png
	BASE_THUMBNAIL_URL = 'https://p3sc.oma.be/datarepfiles/L3_png/v03'

	def __init__(self, local_file, metadata, **kwargs):
		self.local_file = local_file
		self.metadata = metadata
		super().__init__(local_file, **kwargs)

	def get_thumbnail_url(self):
		"""Override to return the proper URL for the thumbnail"""
		return f'{self.BASE_THUMBNAIL_URL}/orbit_{int(self.metadata["orbit_id"]):04d}/{PurePosixPath(self.metadata["filename"]).stem}.png'


class Metadata(MetadataFromFitsHeader):
	def get_field_oid(self):
		return PurePosixPath(self.get_field_value('filename')).stem

	def get_field_wavemin(self):
		return float(self.fits_header['WAVEMIN']) / 10.0

	def get_field_wavemax(self):
		return float(self.fits_header['WAVEMAX']) / 10.0


class Provider(ProviderFromLocalFitsFile):
	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation

	HDU_NAME_OR_INDEX = 0

	def get_resource_data(self, file_path):
		"""Extract the data for the metadata and data_location resource from a local FITS file"""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_local_file(file_path, self.HDU_NAME_OR_INDEX), keywords=self.keywords
		)
		resource_data = metadata.get_resource_data()
		# Pass the metadata for building the thumbnail_url
		data_location = self.DATA_LOCATION_CLASS(file_path, resource_data)
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a "%s" FITS file to the SVO' % DATASET)
	parser.add_argument(
		'fits_files',
		metavar='FITS FILE',
		nargs='+',
		help='A FITS file to submit to the SVO (also accept glob pattern)',
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
		help='A file containing the username (email) and API key separated by a colon of the owner of the metadata',
	)
	parser.add_argument(
		'--min-modif-time',
		'-m',
		type=utils.parse_date_time_string,
		help='Only submit file if the modification time is after that date',
	)
	parser.add_argument(
		'--submit',
		default=True,
		action=argparse.BooleanOptionalAction,
		help='If set (the default), submit data to the server; if negated with --no-submit, only print the data',
	)
	parser.add_argument(
		'--output-file',
		'-o',
		help='Path to a JSONL file to write the data to, instead of printing it',
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
