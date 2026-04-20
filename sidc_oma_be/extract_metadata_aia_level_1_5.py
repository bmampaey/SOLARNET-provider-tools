#!/usr/bin/env python3
"""Script to extract metadata from the AIA level 1.5 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path
from urllib.parse import urljoin

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	DataLocationFromLocalFile,
	ExtractorFromLocalFitsFile,
	MetadataFromFitsHeader,
	RESTfulApi,
	utils,
)

DATASET = 'AIA level 1.5'


class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_PATH = '/data/sdo/aia_synoptic/'

	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'http://jsoc.stanford.edu/data/aia/synoptic/'

	# The base thumbnail URL to build the default tumbnail_url, uses the fits2thumbnail service of the SVO to convert FITS to png
	BASE_THUMBNAIL_URL = 'https://solarnet.oma.be/service/fits2thumbnail/?max_percentile=95&hdu=1&url='

	def get_file_url(self):
		"""Override to return the proper URL for the file"""
		if self.file_url is not None:
			return self.file_url
		elif self.BASE_FILE_URL:
			file_path = self.get_file_path()
			relative_url = (
				file_path[5:16] + 'H%02d00/' % int(file_path[43:45]) + 'AIA%s_%s.fits' % (file_path[34:47], file_path[29:33])
			)
			return urljoin(self.BASE_FILE_URL, relative_url)
		else:
			raise ValueError('Either file_url or BASE_FILE_URL must be set')

	def get_thumbnail_url(self):
		"""Override to return the proper URL for the thumbnail"""
		return self.BASE_THUMBNAIL_URL + self.get_file_url()


class Metadata(MetadataFromFitsHeader):
	def get_field_oid(self):
		return '%04d%s' % (self.get_field_value('wavelnth'), self.get_field_value('date_beg').strftime('%Y%m%d%H%M%S'))

	def get_field_date_beg(self):
		return self.get_field_value('date_obs')

	def get_field_date_end(self):
		return self.get_field_value('date_obs') + timedelta(seconds=self.get_field_value('exptime'))

	# TODO is there a better value for this
	def get_field_wavemin(self):
		return self.get_field_value('wavelnth') / 10.0

	def get_field_wavemax(self):
		return self.get_field_value('wavelnth') / 10.0


class Extractor(ExtractorFromLocalFitsFile):
	METADATA_CLASS = Metadata

	DATA_LOCATION_CLASS = DataLocation

	HDU_NAME_OR_INDEX = 1


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS file to the SVO')
	parser.add_argument(
		'--verbose',
		'-v',
		choices=['DEBUG', 'INFO', 'ERROR'],
		default='INFO',
		help='Set the logging level (default is INFO)',
	)
	parser.add_argument(
		'fits_files',
		metavar='FITS FILE',
		nargs='+',
		help='A FITS file to submit to the SVO (also accept glob pattern)',
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
	logging.basicConfig(
		level=getattr(logging, args.verbose),
		format='%(asctime)s %(levelname)-8s: %(message)s',
	)

	try:
		exractor = Extractor(RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'), DATASET)
	except Exception as error:
		logging.critical('Could not create provider: %s', error)
		raise

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			exractor.write_metadata(utils.iter_files(args.fits_files, args.min_modif_time), output_file)
	else:
		exractor.write_metadata(utils.iter_files(args.fits_files, args.min_modif_time), sys.stdout)
