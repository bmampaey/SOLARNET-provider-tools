import logging
from pprint import pformat

from .restful_api import RESTfulApi
from .metadata import MetadataFromFitsFile
from .data_location import DataLocationFromLocalFile

__all__ = ['Provider', 'ProviderFromLocalFitsFile']

class Provider:
	'''Class for data providers to interract with a dataset's metadata resource via the RESTful API'''
	
	def __init__(self, restful_api, dataset_name):
		self.api = restful_api
		self.dataset = self.get_dataset(dataset_name)
		self.keywords = self.get_keywords(dataset_name)
		# Set up the metadata resource from the URI provided in the dataset info
		self.metadata_resource = self.api(self.dataset['metadata']['resource_uri'])

	def get_dataset(self, dataset_name):
		'''Get the dataset info from the API'''
		try:
			return self.api.dataset(dataset_name).get()
		except Exception as why:
			raise RuntimeError('Could not retrieve info for dataset "%s": %s', (dataset_name, why)) from why

	def get_keywords(self, dataset_name):
		'''Return the list of keywords for the dataset from the API'''
		try:
			return self.api.keyword.get(dataset__name=dataset_name, limit=0)['objects']
		except Exception as why:
			raise RuntimeError('Could not retrieve keywords for dataset "%s": %s' % (dataset_name, why)) from why
	
	def get_metadata(self, oid):
		'''Return the metadata corresponding to the oid or None if no such exists'''
		try:
			result = self.metadata_resource.get(oid = oid)
		except Exception as why:
			raise RuntimeError('Could not retrieve metadata for dataset "%s":\n%s' % (self.dataset['name'], self.api.exception_to_text(why))) from why
		
		return result['objects'][0] if result.get('objects', None) else None
	
	def get_data_location(self, file_url):
		'''Return the data location corresponding to the file URL or None if no such exists'''
		try:
			result = self.api.data_location.get(dataset__name = self.dataset['name'], file_url = file_url, limit = 1)
		except Exception as why:
			raise RuntimeError('Could not retrieve data location for dataset "%s":\n%s' % (self.dataset['name'], self.api.exception_to_text(why))) from why
		
		return result['objects'][0] if result.get('objects', None) else None
	
	def create(self, resource_data):
		'''Create a new data record for the dataset'''
		try:
			result = self.metadata_resource.post(resource_data)
		except Exception as why:
			raise RuntimeError('Could not create metadata for dataset "%s":\n%s' % (self.dataset['name'], self.api.exception_to_text(why))) from why
		return result
	
	def update(self, resource_data, oid = None):
		'''Update an existing data record for the dataset'''
		# Remove the oid from the metadata but copy it first as to not modify the input
		resource_data = resource_data.copy()
		oid = resource_data.pop('oid', oid)
		if not oid:
			raise ValueError('"oid" is undefined: it must be present in the metadata dict or passed explicitly') from why
		
		try:
			result = self.metadata_resource(oid).patch(resource_data)
		except Exception as why:
			raise RuntimeError('Could not update metadata %s for dataset "%s":\n%s' % (self.dataset['name'], oid, self.api.exception_to_text(why))) from why
		return result

class ProviderFromLocalFitsFile(Provider):
	'''Class to extract metadadata from FITS file and submit it to the SVO via the RESTful API'''
	
	METADATA_CLASS = MetadataFromFitsFile
	
	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, fits_file):
		'''Extract the data for the metadata and data_location resource from a local FITS file'''
		metadata = self.METADATA_CLASS(fits_file = fits_file, keywords = self.keywords)
		data_location = self.DATA_LOCATION_CLASS(fits_file)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data
	
	def submit_new_metadata(self, fits_files, dry_run = False):
		'''Create a new metadata and data_location resources from a FITS file'''
		
		for fits_file in fits_files:
			
			logging.info('Creating metadata and data_location resource for FITS file "%s"', fits_file)
			
			try:
				resource_data = self.get_resource_data(fits_file)
			except Exception as why:
				logging.critical('Could not extract resource data for FITS file "%s": %s', fits_file, why)
				raise
			else:
				logging.debug(pformat(resource_data, indent = 2, width = 200))
			
			if dry_run:
				logging.info('Called with dry-run option, not submitting anything')
			else:
				try:
					result = self.create(resource_data)
				except Exception as why:
					logging.error('Could not create new metadata or data_location resource for FITS file "%s": %s', fits_file, why)
				else:
					logging.info('Created new metadata resource "%s" for FITS file "%s"', result['resource_uri'], fits_file)
