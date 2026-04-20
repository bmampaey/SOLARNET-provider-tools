import json
import logging
from pprint import pformat

from .data_location import DataLocationFromLocalFile, DataLocationFromUrl
from .metadata import MetadataFromFitsHeader
from .utils import get_fits_header_from_local_file, get_fits_header_from_url

__all__ = ['Extractor', 'ExtractorFromLocalFitsFile', 'ExtractorFromFitsUrl']


class Extractor:
	"""Class for data providers to interract with a dataset's resource via the RESTful API"""

	def __init__(self, restful_api, dataset_name):
		self.api = restful_api
		self.dataset = self.get_dataset(dataset_name)
		self.keywords = self.get_keywords(dataset_name)

	def get_dataset(self, dataset_name):
		"""Get the dataset info from the API"""
		try:
			return self.api.dataset(dataset_name).get()
		except Exception as error:
			raise RuntimeError('Could not retrieve info for dataset "%s": %s', (dataset_name, error)) from error

	def get_keywords(self, dataset_name):
		"""Return the list of keywords for the dataset from the API"""
		try:
			return self.api.keyword.get(dataset__name=dataset_name, limit=0)['objects']
		except Exception as error:
			raise RuntimeError('Could not retrieve keywords for dataset "%s": %s' % (dataset_name, error)) from error


class ExtractorFromLocalFitsFile(Extractor):
	"""Class to extract metadadata from a FITS file and save it to JSON"""

	HDU_NAME_OR_INDEX = 0

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, file_path):
		"""Extract the data for the metadata and data_location resource from a local FITS file"""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_local_file(file_path, self.HDU_NAME_OR_INDEX), keywords=self.keywords
		)
		data_location = self.DATA_LOCATION_CLASS(file_path)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data

	def write_metadata(self, file_paths, output_file):
		"""Write metadata and data_location resources from local FITS files"""

		for file_path in file_paths:
			logging.info('Extracting metadata and data_location resource for file "%s"', file_path)

			try:
				resource_data = self.get_resource_data(file_path)
			except Exception as error:
				logging.critical('Could not extract resource data for file "%s": %s', file_path, error)
				continue
			else:
				logging.debug(pformat(resource_data, indent=2, width=200))

			json.dump(resource_data, output_file, default=str)
			output_file.write('\n')


class ExtractorFromFitsUrl(Extractor):
	"""Class to extract metadadata from a URL and submit it to the SVO via the RESTful API"""

	# Must be a multiple of 2880
	HEADER_SIZE = 2880

	HEADER_OFFSET = 0

	ZIPPED = False

	WEBSERVER_AUTH = None

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromUrl

	def get_resource_data(self, file_url):
		"""Extract the data for the metadata and data_location resource from a FITS file URL"""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_url(
				file_url, self.HEADER_SIZE, self.HEADER_OFFSET, self.ZIPPED, self.WEBSERVER_AUTH
			),
			keywords=self.keywords,
		)
		data_location = self.DATA_LOCATION_CLASS(file_url)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data

	def write_metadata(self, file_urls, output_file):
		"""Write metadata and data_location resources from local FITS files"""

		for file_url in file_urls:
			logging.info('Extracting metadata and data_location resource for file "%s"', file_url)

			try:
				resource_data = self.get_resource_data(file_url)
			except Exception as error:
				logging.critical('Could not extract resource data for file "%s": %s', file_url, error)
				continue
			else:
				logging.debug(pformat(resource_data, indent=2, width=200))

			json.dump(resource_data, output_file, default=str)
			output_file.write('\n')
