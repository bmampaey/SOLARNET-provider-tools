#!/usr/bin/env python3
"""Script to extract metadata from the IBIS level 2 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path

# HACK to make sure the provider_tools module is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))

from provider_tools import (
	MetadataFromFitsHeader,
	ProviderFromLocalFitsFile,
	RESTfulApi,
	utils,
)

# Dont change this
DATASET = 'IBIS level 2'


class DataLocation:
	def __init__(self, file_path):
		self.file_path = file_path

	# Define here how to infer the file_url, file_path, thumbnail_url, etc from the fits_file_path
	def get_resource_data(self):
		# TODO Change this to point to the actual download URL
		file_url = 'http://ibis.oa-roma.inaf.it/data/' + self.file_path.name

		# TODO Change this to point to the actual thumbnail URL
		thumbnail_url = 'http://ibis.oa-roma.inaf.it/static/preview/' + self.file_path.with_suffix('.jpg').name

		return {
			'file_url': file_url,
			'file_size': self.file_path.stat().st_size,
			'file_path': self.file_path.name,
			'thumbnail_url': thumbnail_url,
			'offline': False,
		}


class Metadata(MetadataFromFitsHeader):
	# DATE-END is not defined in the FITS file, so we infer it using DATE-BEG and SCAN_DUR
	def get_date_end(self):
		return self.extract_field_value('date_beg') + timedelta(seconds=self.extract_field_value('scan_dur'))

	# WAVEMIN and WAVEMAX in the FITS header are defined in Angstrom, but SVO requires them in nm
	def get_wavemin(self):
		return self.extract_field_value('wavemin') / 10

	def get_wavemax(self):
		return self.extract_field_value('wavemin') / 10


class Provider(ProviderFromLocalFitsFile):
	HDU_NAME_OR_INDEX = 0
	METADATA_CLASS = Metadata
	DATA_LOCATION_CLASS = DataLocation

	def get_resource_data(self, fits_file):
		file_path = Path(fits_file)
		resource_data = super().get_resource_data(file_path)
		resource_data['oid'] = file_path.stem
		return resource_data


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Extract metadata from FITS files and submit them to the SVO for dataset "%s"' % DATASET
	)
	parser.add_argument(
		'--verbose',
		'-v',
		choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
		default='WARNING',
		help='Set the logging level (default is WARNING)',
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
