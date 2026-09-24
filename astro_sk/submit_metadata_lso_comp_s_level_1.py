#!/usr/bin/env python3

"""Script to extract metadata from the CoMP-S level 1 archive and submit it to the SOLARNET Virtual Observatory"""

import argparse
import logging
import sys
from pathlib import Path

from astropy.io import fits

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import (
	MetadataFromFitsHeader,
	ProviderFromLocalFitsFile,
	RESTfulApi,
)

# These prefix and suffix will be removed from the FITS filename to create the OID
FITS_FILE_PREFIX = 'lso_comp-s_lev1.0_'
FITS_FILE_SUFFIX = '.fits.fz'

# Dont change this
DATASET = 'CoMP-S level 1'


# Define here how to infer the file_url, file_path, thumbnail_url, etc from the fits_file_path
def get_data_location_from_file_path(fits_file_path):
	# The subdirectory in the URLs (e.g 20260913) needs to be infered from the fits_file_path
	# e.g. /home/observer/SVO/data/20260913/lso_comp-s_lev1.0_....fits.fz
	subdirectory = fits_file_path.resolve().parts[5]  # parts[0] is /, parts[1] is home, parts[5] is 20260913

	# Construct the file_url from the filename and the subdirectory
	file_url = 'https://lsodb.astro.sk/api/' + subdirectory + '/data/download/' + fits_file_path.name
	# Get the file size from disk
	file_size = fits_file_path.stat().st_size
	# Construct the file_path from the filename and the subdirectory
	file_path = subdirectory + '/' + fits_file_path.name
	# Construct the thumbnail_url from the filename and the subdirectory, and change the suffix
	thumbnail_url = (
		'https://lsodb.astro.sk/static/CoMP-S_Logs/'
		+ subdirectory
		+ '/CoMP-S_obse_previews/'
		+ fits_file_path.name[: -len(FITS_FILE_SUFFIX)]
		+ 'wavepoint001_all_frames.png'  # Is this suffix always the same ?
	)

	return {
		'file_url': file_url,
		'file_size': file_size,
		'file_path': file_path,
		'thumbnail_url': thumbnail_url,
		'offline': False,
	}


class Metadata(MetadataFromFitsHeader):
	def get_wavemin(self):
		return self.extract_field_value('fw_wvlng')

	def get_wavemax(self):
		return self.extract_field_value('fw_wvlng')


class Provider(ProviderFromLocalFitsFile):
	METADATA_CLASS = Metadata

	def get_resource_data(self, item):
		fits_header, oid, date_location = item
		metadata = self.METADATA_CLASS(fits_header=fits_header, keywords=self.keywords)
		resource_data = metadata.get_resource_data()
		resource_data['oid'] = oid
		resource_data['data_location'] = date_location
		return resource_data


# Scan through a all HDUs of a FITS file
# and yield for each HDU the header, the OID and the data location
def list_items(fits_file_path):
	fits_file_path = Path(fits_file_path)

	if not fits_file_path.name.endswith(FITS_FILE_SUFFIX):
		logging.warning(
			'Skipping FITS file %s because it does not end with suffix %s',
			fits_file_path,
			FITS_FILE_SUFFIX,
		)
		return

	if not fits_file_path.name.startswith(FITS_FILE_PREFIX):
		logging.warning(
			'Skipping FITS file %s because it does not start with prefix %s',
			fits_file_path,
			FITS_FILE_PREFIX,
		)
		return

	data_location = get_data_location_from_file_path(fits_file_path)

	logging.info(
		'Data location for FITS file %s to\n\t%s',
		fits_file_path,
		'\n\t'.join('%s: %s' % item for item in data_location.items()),
	)

	logging.info('Extracting metadata from FITS file %s', fits_file_path)

	with fits.open(fits_file_path) as hdus:
		for i, hdu in enumerate(hdus):
			if 'DATE-BEG' not in hdu.header:
				logging.warning(
					'Skipping HDU %s of file %s, it does not contain the required metadata DATE-BEG',
					i,
					fits_file_path,
				)
				continue

			oid = f'{fits_file_path.name[len(FITS_FILE_PREFIX) : -len(FITS_FILE_SUFFIX)]}_{i}'

			yield hdu.header, oid, data_location


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
		'fits_file_path',
		metavar='FITS-FILE',
		type=Path,
		help='Path to the FITS file to process',
	)
	args = parser.parse_args()

	# Setup the logging
	logging.basicConfig(
		level=getattr(logging, args.verbose),
		format='%(asctime)s %(levelname)-8s: %(message)s',
	)

	try:
		provider = Provider(
			RESTfulApi(auth_file=args.auth_file, debug=args.verbose == 'DEBUG'),
			DATASET,
		)
	except Exception as error:
		logging.critical('Could not initialise provider: %s', error)
		raise

	items = list_items(args.fits_file_path)

	if args.output_file:
		with open(args.output_file, 'wt') as output_file:
			provider.process_items(items, args.submit, output_file)
	else:
		provider.process_items(items, args.submit)
