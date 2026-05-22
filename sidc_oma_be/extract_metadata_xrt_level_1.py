#!/usr/bin/env python3
"""Script to extract metadata from the XRT online archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import DataLocationFromUrl, ExtractorFromFitsUrl, MetadataFromFitsHeader, RESTfulApi, utils

DATASET = 'XRT level 1'
BASE_FILE_URL = 'https://xrt.cfa.harvard.edu/level1/'


class DataLocation(DataLocationFromUrl):
	BASE_FILE_URL = BASE_FILE_URL

	BASE_THUMBNAIL_URL = 'https://solarnet.oma.be/service/fits2thumbnail/?max_percentile=98&hdu=0&url='

	def get_thumbnail_url(self):
		"""Override to return the proper URL for the thumbnail"""
		return self.BASE_THUMBNAIL_URL + self.get_file_url()


class Metadata(MetadataFromFitsHeader):
	def get_field_date_beg(self):
		return self.get_field_value('date_obs')

	def get_field_wavemin(self):
		return 0.88

	def get_field_wavemax(self):
		return 33.5

	def get_field_history(self):
		return '\n'.join(self.fits_header['HISTORY']).replace("\n(cont'd)", '')

	def get_field_oid(self):
		if self.oid:
			return self.oid
		else:
			return self.get_field_value('date_beg').strftime('%Y%m%d%H%M%S%f')[:-3]


class Extractor(ExtractorFromFitsUrl):
	HEADER_SIZE = 7 * 2880

	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS URL the SVO')
	parser.add_argument(
		'--verbose', '-v', choices=['DEBUG', 'INFO', 'ERROR'], default='INFO', help='Set the logging level (default is INFO)'
	)
	parser.add_argument(
		'urls',
		metavar='URL',
		nargs='*',
		default=[BASE_FILE_URL],
		help="A URL to a FITS file to submit to the SVO (also accept apache style directory indexing, don't forget to end diretories URL with a slash)",
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
		help='Only submit record if the modification_date is after that date',
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
			exractor.write_metadata(utils.iter_urls(args.urls, min_modification_time=args.min_modif_time), output_file)
	else:
		exractor.write_metadata(utils.iter_urls(args.urls, min_modification_time=args.min_modif_time), sys.stdout)
