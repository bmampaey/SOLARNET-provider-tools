#!/usr/bin/env python3
"""Script to extract metadata from the EIT archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
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

DATASET = 'EIT level 0'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '/data/soho-archive/eit/lz/'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://www.sidc.be/eitlz/'

	# The base thumbnail URL to build the default tumbnail_url, uses the image2thumbnail service of the SVO to convert JP2 to png
	BASE_THUMBNAIL_URL = 'https://solarnet.oma.be/service/fits2thumbnail/?max_percentile=99.5&url='

	def get_thumbnail_url(self):
		# Use the SVO thumbnail service to convert the FITS file to png
		return self.BASE_THUMBNAIL_URL + self.get_file_url()


class Metadata(MetadataFromFitsHeader):
	def __init__(self, fits_header, oid=None, keywords=[]):
		super().__init__(fits_header, oid, keywords)

		# EIT has additional keywords in comments in the form "COMMENT BLOCKS_HORZ = 1"
		# so add these to the FITS header to simplify parsing
		# but save the original FITS header value for fits_header metadata field

		self.fits_header_string = self.fits_header.tostring().strip()

		for comment in self.fits_header['COMMENT']:
			try:
				key, value = comment.split('=', 1)
			except ValueError:
				pass
			else:
				self.fits_header.setdefault(key.strip().replace(' ', '_'), value.strip().strip("'"))

	def get_fits_header(self):
		"""Return the value of the fits_header metadata field"""
		return self.fits_header_string

	def get_duration_from_fits_header(self, fits_keyword):
		"""Parse a FITS keyword wich value is in the form "0.111 s" and return a float"""
		try:
			value = self.fits_header[fits_keyword]
		except KeyError:
			logging.info('Keyword %s missing from FITS header', fits_keyword)
			return None

		try:
			return float(value.split()[0])
		except (IndexError, ValueError) as error:
			logging.info('Value of keyword %s is not a float', fits_keyword)
			return None

	def get_shutter_close_time(self):
		return self.get_duration_from_fits_header('SHUTTER_CLOSE_TIME')

	def get_commanded_exposure_time(self):
		return self.get_duration_from_fits_header('COMMANDED_EXPOSURE_TIME')

	def get_exptime(self):
		commanded_exposure_time = self.extract_field_value('commanded_exposure_time')
		shutter_close_time = self.extract_field_value('shutter_close_time')
		if commanded_exposure_time is not None and shutter_close_time is not None:
			return commanded_exposure_time + shutter_close_time
		else:
			return None

	def get_date_beg(self):
		return self.extract_field_value('corrected_date_obs')

	def get_date_end(self):
		return self.get_date_beg() + timedelta(seconds=self.extract_field_value('exptime'))

	def get_wavemin(self):
		return self.extract_field_value('wavelnth') / 10.0

	def get_wavemax(self):
		return self.extract_field_value('wavelnth') / 10.0


class Provider(ProviderFromLocalFitsFile):
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
