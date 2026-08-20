#!/usr/bin/env python3
"""Script to extract metadata from the XRT online archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import DataLocationFromUrl, MetadataFromFitsHeader, ProviderFromFitsUrl, RESTfulApi, utils

DATASET = 'XRT level 1'
BASE_FILE_URL = 'https://xrt.cfa.harvard.edu/level1/'


class DataLocation(DataLocationFromUrl):
	BASE_FILE_URL = BASE_FILE_URL

	BASE_THUMBNAIL_URL = 'https://solarnet.oma.be/service/fits2thumbnail/?max_percentile=98&hdu=0&url='

	def get_thumbnail_url(self):
		# Use the SVO thumbnail service to convert the FITS file to png
		return self.BASE_THUMBNAIL_URL + self.get_file_url()


class Metadata(MetadataFromFitsHeader):
	def get_date_beg(self):
		return self.extract_field_value('date_obs')

	def get_wavemin(self):
		return 0.88

	def get_wavemax(self):
		return 33.5

	def get_history(self):
		return '\n'.join(self.fits_header['HISTORY']).replace("\n(cont'd)", '')

	def get_oid(self):
		# Include the milliseconds, as there can be more than 1 observation per second
		return self.get_date_beg().strftime('%Y%m%d%H%M%S%f')[:-3]


class Provider(ProviderFromFitsUrl):
	HEADER_SIZE = 7 * 2880

	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Extract metadata from FITS URLs and submit them to the SVO for dataset "%s"' % DATASET
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

	items = utils.iter_urls(args.fits_urls, min_modification_time=args.min_modif_time)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
