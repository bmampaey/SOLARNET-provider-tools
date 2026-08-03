__all__ = ['Provider']


class Provider:
	"""Base class for submitting dataset metadata through the API."""

	def __init__(self, restful_api, dataset_name):
		"""Initialize the provider with API access and dataset metadata."""
		self.api = restful_api
		self.dataset = self.get_dataset(dataset_name)
		self.keywords = self.get_keywords(dataset_name)
		# Set up the metadata resource from the URI provided in the dataset info
		self.metadata_resource = self.api(self.dataset['metadata']['resource_uri'])

	def get_dataset(self, dataset_name):
		"""Fetch dataset information from the API."""
		try:
			return self.api.dataset(dataset_name).get()
		except Exception as error:
			raise RuntimeError('Could not retrieve info for dataset "%s": %s', (dataset_name, error)) from error

	def get_keywords(self, dataset_name):
		"""Return the keywords available for the dataset."""
		try:
			return self.api.keyword.get(dataset__name=dataset_name, limit=0)['objects']
		except Exception as error:
			raise RuntimeError('Could not retrieve keywords for dataset "%s": %s' % (dataset_name, error)) from error

	def get_metadata(self, oid):
		"""Return the metadata record matching the given identifier, if present."""
		try:
			result = self.metadata_resource.get(oid=oid)
		except Exception as error:
			raise RuntimeError(
				'Could not retrieve metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error

		return result['objects'][0] if result.get('objects', None) else None

	def get_data_location(self, file_url, retries=3):
		"""Return the data_location record matching the given file URL, if present."""

		try:
			result = self.api.data_location.get(dataset__name=self.dataset['name'], file_url=file_url, limit=1)
		except Exception as error:
			if retries:
				return self.get_data_location(file_url, retries - 1)
			raise RuntimeError(
				'Could not retrieve data location for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error

		return result['objects'][0] if result.get('objects', None) else None

	def create(self, resource_data):
		"""Create a new metadata record for the dataset."""
		try:
			result = self.metadata_resource.post(resource_data)
		except Exception as error:
			raise RuntimeError(
				'Could not create metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error
		return result

	def update(self, resource_data, oid=None):
		"""Update an existing metadata record for the dataset."""
		# Remove the oid from the metadata but copy it first as to not modify the input
		resource_data = resource_data.copy()
		oid = resource_data.pop('oid', oid)
		if not oid:
			raise ValueError('"oid" is undefined: it must be present in the resource_data dict or passed explicitly')
		try:
			result = self.metadata_resource(oid).patch(resource_data)
		except Exception as error:
			raise RuntimeError(
				'Could not update metadata %s for dataset "%s": %s' % (self.dataset['name'], oid, self.api.exception_to_text(error))
			) from error
		return result
