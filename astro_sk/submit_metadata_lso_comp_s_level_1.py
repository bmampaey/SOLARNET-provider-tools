#!/usr/bin/env python3
"""Script to extract metadata from the AIA level 1.5 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
import tarfile
from pathlib import Path

from astropy.io import fits

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	MetadataFromFitsHeader,
	ProviderFromLocalFitsFile,
	RESTfulApi,
)

# Inside the TAR archive, the script expects the FITS files to have these prefix and suffix, they will be removed from the FITS filename to create the OID
FITS_FILE_PREFIX = 'lso_comp-s_lev1.0_'
FITS_FILE_SUFFIX = '.fits.fz'

# Dont change this
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


# Scan through a tar archive (compressed or not) to extract FITS metadata
# and yield for each HDU the header and the OID
def list_items(tar_file_path):
	with tarfile.open(tar_file_path, 'r:*') as tar:
		for member in tar.getmembers():
			fits_file_path = Path(member.name)
			if not fits_file_path.name.endswith(FITS_FILE_SUFFIX):
				logging.warning('Skipping TAR file %s because it is not ending with suffix %s', fits_file_path, FITS_FILE_SUFFIX)
				continue

			if not fits_file_path.name.startswith(FITS_FILE_PREFIX):
				logging.warning('Skipping TAR file %s because it is not starting with prefix %s', fits_file_path, FITS_FILE_PREFIX)
				continue

			fits_file = tar.extractfile(member)

			if fits_file is None:
				logging.info('Skipping TAR entry %s, not a regular file', member.name)
				continue

			logging.info('Extracting metadata from FITS file %s', fits_file_path)

			with fits.open(fits_file) as hdus:
				for i, hdu in enumerate(hdus):
					if 'DATE-BEG' not in hdu.header:
						logging.warning(
							'Skipping HDU %s of file %s, it does not contain the required metadata DATE-BEG', i, fits_file_path
						)
						continue
					yield hdu.header, f'{fits_file_path.name[len(FITS_FILE_PREFIX) : -len(FITS_FILE_SUFFIX)]}_{i}'


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Extract metadata from FITS files within a TAR archive and submit them to the SVO for dataset "%s"'
		% DATASET
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
		'tar_file_path',
		metavar='TAR-ARCHIVE',
		type=Path,
		help='Path to the TAR archive on disk contaning FITS files to process',
	)
	parser.add_argument(
		'--file-url',
		metavar='URL',
		required=True,
		help='URL to download the TAR archive',
	)
	parser.add_argument(
		'--thumbnail-url',
		metavar='URL',
		required=True,
		help='URL to an image (NOT an HTML page)',
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(
		level=getattr(logging, args.verbose),
		format='%(asctime)s %(levelname)-8s: %(message)s',
	)

	data_location = {
		'file_url': args.file_url,
		'file_size': args.tar_file_path.stat().st_size,
		'file_path': args.tar_file_path.name,
		'thumbnail_url': args.thumbnail_url,
		'offline': False,
	}

	logging.info('Setting data location to\n\t%s', '\n\t'.join('%s: %s' % item for item in data_location.items()))

	try:
		provider = Provider(
			RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'),
			DATASET,
			data_location=data_location,
		)
	except Exception as error:
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = list_items(args.tar_file_path)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
