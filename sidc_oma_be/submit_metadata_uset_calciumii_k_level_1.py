#!/usr/bin/env python3
'''Script to extract metadata from the USET archive and submit it to the SOLARNET Virtual Observatory'''

import sys
import argparse
import logging
from pathlib import Path
from datetime import timedelta

# HACK to make sure the provider_tools package is findable
sys.path.append(str(Path(__file__).resolve().parent.parent))
from provider_tools import MetadataFromFitsFile, DataLocationFromLocalFile, RESTfulApi, ProviderFromLocalFitsFile, utils


DATASET = 'USET CalciumII-K level 1'

class DataLocation(DataLocationFromLocalFile):
	# The base directory to build the default file_path
	BASE_FILE_DIRECTORY = '/data/usetml/external/USET_imager/L1centered/USET_CalciumII-K/'
	
	# The base file URL to build the default file_url (must end with a /)
	BASE_FILE_URL = 'https://wwwbis.sidc.be/data/usetml/external/USET_imager/L1centered/USET_CalciumII-K/'
	
	# The base thumbnail URL to build the default tumbnail_url, uses the fits2thumbnail service of the SVO to convert FITS to png
	BASE_THUMBNAIL_URL = 'https://solarnet.oma.be/service/fits2thumbnail/?url=https://wwwbis.sidc.be/data/usetml/external/USET_imager/L1centered/USET_CalciumII-K/'
	
	def get_thumbnail_url(self):
		'''Override to return the proper URL for the thumbnail'''
		return self.BASE_THUMBNAIL_URL + self.get_file_path()


class Metadata(MetadataFromFitsFile):
	
	def get_field_date_end(self):
		return self.get_field_value('date_beg') + timedelta(seconds=self.get_field_value('xposure'))
	
	# TODO is there a better value for this
	def get_field_wavemin(self):
		return self.get_field_value('wavelnth')
	
	def get_field_wavemax(self):
		return self.get_field_value('wavelnth')


class Provider(ProviderFromLocalFitsFile):
	
	METADATA_CLASS = Metadata
	
	DATA_LOCATION_CLASS = DataLocation


if __name__ == "__main__":

	# Get the arguments
	parser = argparse.ArgumentParser(description='Submit metadata from a FITS file to the SVO')
	parser.add_argument('--verbose', '-v', choices = ['DEBUG', 'INFO', 'ERROR'], default = 'INFO', help='Set the logging level (default is INFO)')
	parser.add_argument('fits_files', metavar = 'FITS FILE', nargs='+', help='A FITS file to submit to the SVO (also accept glob pattern)')
	parser.add_argument('--auth-file', '-a', default='./.svo_auth', help='A file containing the username (email) and API key separated by a colon of the owner of the metadata')
	parser.add_argument('--dry-run', '-f', action='store_true', help='Do not submit data but print what data would be submitted instead')
	parser.add_argument('--min-modif-time', '-m', type=utils.parse_date_time_string, help='Only submit file if the modification time is after that date')
	
	args = parser.parse_args()
	
	# Setup the logging
	logging.basicConfig(level = getattr(logging, args.verbose), format = '%(asctime)s %(levelname)-8s: %(message)s')
	
	try:
		provider = Provider(RESTfulApi(auth_file = args.auth_file, debug = args.verbose == 'DEBUG'), DATASET)
	except Exception as why:
		logging.critical('Could not create provider: %s', why)
		raise
	
	provider.submit_new_metadata(utils.iter_files(args.fits_files, args.min_modif_time), args.dry_run)
