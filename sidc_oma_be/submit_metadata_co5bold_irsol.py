#!/usr/bin/env python3
"""Script to extract metadata from the AIA level 1.5 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import os
import re
import sys
from datetime import timedelta
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

DATASET = 'CO5BOLD@IRSOL_d3gt57g44ssdgreyh12'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '%s/rhd.end.h5'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://rgw.cscs.ch/cscs:cscs-lts-741ce008-0014-4966-beed-fa1775a436e9/modelb/v3diff/magnetic/'

	def get_file_path(self):
		# The HD5F file path is very different than the FITS filename
		filename = os.path.basename(self.local_file)
		number = int(re.search(r'snapshot_(\d+)\.fits$', filename).group(1))
		return self.BASE_FILE_PATH % (number * 5)


class Metadata(MetadataFromFitsHeader):
	def get_oid(self):
		# There can be more than 1 image per second, so add the wavelength to discriminate
		return '%s_%s' % (self.extract_field_value('point_id'), self.extract_field_value('snapshot'))

	# TODO is there a better value for this
	def get_wavemin(self):
		return None

	def get_wavemax(self):
		return None


class Provider(ProviderFromLocalFitsFile):
	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation

	HDU_NAME_OR_INDEX = 0


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
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = utils.iter_files(args.fits_files, args.min_modif_time)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
