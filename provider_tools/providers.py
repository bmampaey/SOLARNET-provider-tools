import logging
from pprint import pformat

from .metadata import MetadataFromFitsHeader, MetadataFromTapRecord
from .data_location import DataLocationFromLocalFile, DataLocationFromTapRecord, DataLocationFromUrl
from .utils import get_fits_header_from_local_file, get_fits_header_from_url

__all__ = ['Provider', 'ProviderFromLocalFitsFile', 'ProviderFromFitsUrl', 'ProviderFromTapRecord']

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
			raise RuntimeError('Could not retrieve metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(why))) from why
		
		return result['objects'][0] if result.get('objects', None) else None
	
	def get_data_location(self, file_url):
		'''Return the data location corresponding to the file URL or None if no such exists'''
		try:
			result = self.api.data_location.get(dataset__name = self.dataset['name'], file_url = file_url, limit = 1)
		except Exception as why:
			raise RuntimeError('Could not retrieve data location for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(why))) from why
		
		return result['objects'][0] if result.get('objects', None) else None
	
	def create(self, resource_data):
		'''Create a new data record for the dataset'''
		try:
			result = self.metadata_resource.post(resource_data)
		except Exception as why:
			raise RuntimeError('Could not create metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(why))) from why
		return result
	
	def update(self, resource_data, oid = None):
		'''Update an existing data record for the dataset'''
		# Remove the oid from the metadata but copy it first as to not modify the input
		resource_data = resource_data.copy()
		oid = resource_data.pop('oid', oid)
		if not oid:
			raise ValueError('"oid" is undefined: it must be present in the resource_data dict or passed explicitly')
		try:
			result = self.metadata_resource(oid).patch(resource_data)
		except Exception as why:
			raise RuntimeError('Could not update metadata %s for dataset "%s": %s' % (self.dataset['name'], oid, self.api.exception_to_text(why))) from why
		return result


class ProviderFromLocalFitsFile(Provider):
	'''Class to extract metadadata from a FITS file and submit it to the SVO via the RESTful API'''
	
	HDU_NAME_OR_INDEX = 0

	METADATA_CLASS = MetadataFromFitsHeader
	
	DATA_LOCATION_CLASS = DataLocationFromLocalFile

	def get_resource_data(self, file_path):
		'''Extract the data for the metadata and data_location resource from a local FITS file'''
		metadata = self.METADATA_CLASS(fits_header = get_fits_header_from_local_file(file_path, self.HDU_NAME_OR_INDEX), keywords = self.keywords)
		data_location = self.DATA_LOCATION_CLASS(file_path)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data
	
	def submit_new_metadata(self, file_paths, dry_run = False):
		'''Create new metadata and data_location resources from local FITS files'''
		
		for file_path in file_paths:
			
			logging.info('Creating metadata and data_location resource for file "%s"', file_path)
			
			try:
				resource_data = self.get_resource_data(file_path)
			except Exception as why:
				logging.critical('Could not extract resource data for file "%s": %s', file_path, why)
			else:
				logging.debug(pformat(resource_data, indent = 2, width = 200))
				
				data_location = self.get_data_location(resource_data['data_location']['file_url'])
				if data_location is not None:
					logging.info('Data location for file %s already exists, reusing!', file_path)
					resource_data['data_location'] = data_location['resource_uri']
				
				if dry_run:
					logging.info('Called with dry-run option, not submitting anything')
				else:
					try:
						result = self.create(resource_data)
					except Exception as why:
						logging.error('Could not create new metadata or data_location resource for file "%s": %s', file_path, why)
					else:
						logging.info('Created new metadata resource "%s" for file "%s"', result['resource_uri'], file_path)


class ProviderFromFitsUrl(Provider):
	'''Class to extract metadadata from a URL and submit it to the SVO via the RESTful API'''
	
	# Must be a multiple of 2880
	HEADER_SIZE = 2880
	
	HEADER_OFFSET = 0
	
	ZIPPED = False
	
	WEBSERVER_AUTH = None

	METADATA_CLASS = MetadataFromFitsHeader
	
	DATA_LOCATION_CLASS = DataLocationFromUrl

	def get_resource_data(self, file_url):
		'''Extract the data for the metadata and data_location resource from a FITS file URL'''
		metadata = self.METADATA_CLASS(fits_header = get_fits_header_from_url(file_url, self.HEADER_SIZE, self.HEADER_OFFSET, self.ZIPPED, self.WEBSERVER_AUTH), keywords = self.keywords)
		data_location = self.DATA_LOCATION_CLASS(file_url)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data
	
	def submit_new_metadata(self, file_urls, dry_run = False):
		'''Create a new metadata and data_location resources from FITS files'''
		
		for file_url in file_urls:
			
			logging.info('Creating metadata and data_location resource for URL "%s"', file_url)
			
			try:
				resource_data = self.get_resource_data(file_url)
			except Exception as why:
				logging.critical('Could not extract resource data for URL "%s": %s', file_url, why)
			else:
				logging.debug(pformat(resource_data, indent = 2, width = 200))
				
				data_location = self.get_data_location(resource_data['data_location']['file_url'])
				if data_location is not None:
					logging.info('Data location for URL %s already exists, reusing!', file_url)
					resource_data['data_location'] = data_location['resource_uri']
				
				if dry_run:
					logging.info('Called with dry-run option, not submitting anything')
				else:
					try:
						result = self.create(resource_data)
					except Exception as why:
						logging.error('Could not create new metadata or data_location resource for URL "%s": %s', file_url, why)
					else:
						logging.info('Created new metadata resource "%s" for URL "%s"', result['resource_uri'], file_url)


class ProviderFromTapRecord(Provider):
	'''Class to extract metadadata from a TAP record and submit it to the SVO via the RESTful API'''

	METADATA_CLASS = MetadataFromTapRecord
	
	DATA_LOCATION_CLASS = DataLocationFromTapRecord
	
	def get_resource_data(self, record):
		'''Extract the data for the metadata and data_location resource from a TAPRecord'''
		metadata = self.METADATA_CLASS(record = record, keywords = self.keywords)
		data_location = self.DATA_LOCATION_CLASS(record)
		resource_data = metadata.get_resource_data()
		resource_data['data_location'] = data_location.get_resource_data()
		resource_data['data_location']['dataset'] = self.dataset['resource_uri']
		return resource_data
	
	def submit_new_metadata(self, records, dry_run = False):
		'''Create new metadata and data_location resources from TAP records'''
		
		for record in records:
			
			logging.info('Creating metadata and data_location resource for record "%s"', record)
			
			try:
				resource_data = self.get_resource_data(record)
			except Exception as why:
				logging.critical('Could not extract resource data for record "%s": %s', record, why)
			else:
				logging.debug(pformat(resource_data, indent = 2, width = 200))
				
				data_location = self.get_data_location(resource_data['data_location']['file_url'])
				if data_location is not None:
					logging.info('Data location for record %s already exists, reusing!', record)
					resource_data['data_location'] = data_location['resource_uri']
				
				if dry_run:
					logging.info('Called with dry-run option, not submitting anything')
				else:
					try:
						result = self.create(resource_data)
					except Exception as why:
						logging.error('Could not create new metadata or data_location resource for record "%s": %s', record, why)
					else:
						logging.info('Created new metadata resource "%s" for record "%s"', result['resource_uri'], record)
