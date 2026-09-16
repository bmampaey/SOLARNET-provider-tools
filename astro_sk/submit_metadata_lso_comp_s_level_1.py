#!/usr/bin/env python3
"""Script to extract metadata from the AIA level 1.5 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path

from astropy.io import fits

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	MetadataFromFitsHeader,
	ProviderFromLocalFitsFile,
	RESTfulApi,
	utils,
)

DATASET = 'LSO CoMP-S level 1'


class Metadata(MetadataFromFitsHeader):
	def get_wavemin(self):
		return self.extract_field_value('fw_wvlng')

	def get_wavemax(self):
		return self.extract_field_value('fw_wvlng')


class Provider(ProviderFromLocalFitsFile):
	METADATA_CLASS = Metadata

	def __init__(self, *args, data_location=None, **kwargs):
		super().__init__(*args, **kwargs)
		self.data_location = data_location or {}

	def get_resource_data(self, item):
		fits_header, oid = item
		metadata = self.METADATA_CLASS(fits_header=fits_header, keywords=self.keywords)
		resource_data = metadata.get_resource_data()
		resource_data['oid'] = oid
		resource_data['data_location'] = self.data_location
		return resource_data


def list_items(fits_files, min_modif_time=None):
	for fits_file in utils.iter_files(fits_files, min_modif_time):
		with fits.open(fits_file) as hdus:
			for i, hdu in enumerate(hdus):
				if i == 0:
					continue
				else:
					yield hdu.header, f'{Path(fits_file).name.removeprefix("lso_comp-s_lev1.0_").removesuffix(".fits.fz")}_{i}'


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

	data_location = {
		'file_url': 'https://lsodb.astro.sk/api/20250912/data/download',
		'file_size': 11339130880,
		'file_path': 'lso_comp-s_lev1.0_20250912.tar',
		'thumbnail_url': 'https://lsodb.astro.sk/api/20250912/CoMP-S_obse_previews/lso_comp-s_lev1.0_20250912_061110_wave0656_pa293_cam2_obse_wavepoint001_all_frames.png',
		'offline': False,
	}

	try:
		provider = Provider(
			RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'), DATASET, data_location=data_location
		)
	except Exception as error:
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = list_items(args.fits_files, args.min_modif_time)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
