import json
import logging
from pprint import pformat

from ..data_locations import DataLocationFromLocalFile
from ..metadatas import MetadataFromFitsHeader
from ..utils import get_fits_header_from_local_file
from .extractor import Extractor

__all__ = ['ExtractorFromLocalFitsFile']


class ExtractorFromLocalFitsFile(Extractor):
	"""Extract metadata for local FITS files and write it to JSON."""

	HDU_NAME_OR_INDEX = 0

	METADATA_CLASS = MetadataFromFitsHeader

	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, file_path):
		"""Build a resource payload from a local FITS file."""
		metadata = self.METADATA_CLASS(
			fits_header=get_fits_header_from_local_file(file_path, self.HDU_NAME_OR_INDEX), keywords=self.keywords
		)
		data_location = self.DATA_LOCATION_CLASS(file_path)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		return resource_data

	def write_metadata(self, file_paths, output_file):
		"""Write metadata and data_location payloads for local FITS files."""

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
