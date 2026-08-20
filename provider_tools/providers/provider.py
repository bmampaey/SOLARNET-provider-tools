import logging

from ..utils import JsonSerializer

__all__ = ['Provider']


class Provider:
	"""Base class for extracting the metadata and data_location resource payloads
	for a dataset and submit them to the SVO RESTful API.

	Subclasses must implement [`get_resource_data`][provider_tools.providers.Provider.get_resource_data]
	to define how the metadata and data_location resources paylods
	are extracted from the source data.
	"""

	def __init__(self, restful_api, dataset_name, logger=None):
		"""Initialize the provider and fetch dataset and keywords information from SVO.

		Args:
			restful_api (RESTfulApi): A initialized RESTfulApi with proper authentication
				to make requests to the SVO (e.g. dataset, keyword, metadata, data_location endpoints).
			dataset_name (str): Name of the dataset to operate on. Used to
				look up the dataset resource, its keywords, and to build the
				metadata resource endpoint.

		Raises:
			RuntimeError: If the dataset or its keywords cannot be retrieved.
		"""
		self.api = restful_api
		self.logger = logger or logging
		self.dataset = self.get_dataset(dataset_name)
		self.keywords = self.get_keywords(dataset_name)
		# Set up the metadata resource from the URI provided in the dataset info
		self.metadata_resource = self.api(self.dataset['metadata']['resource_uri'])

	def get_dataset(self, dataset_name):
		"""Fetch dataset information from the API.

		Args:
			dataset_name (str): Name of the dataset to look up.

		Returns:
			(dict): The dataset resource returned by the API.

		Raises:
			RuntimeError: If the API request fails for any reason.
		"""
		try:
			return self.api.dataset(dataset_name).get()
		except Exception as error:
			raise RuntimeError('Could not retrieve resource for dataset "%s": %s', (dataset_name, error)) from error

	def get_keywords(self, dataset_name):
		"""Fetch all keywords registered for the dataset.

		Args:
			dataset_name (str): Name of the dataset whose keywords should
				be retrieved.

		Returns:
			(list): The list of all keyword objects associated with the dataset.

		Raises:
			RuntimeError: If the API request fails for any reason.
		"""
		try:
			return self.api.keyword.get(dataset__name=dataset_name, limit=0)['objects']
		except Exception as error:
			raise RuntimeError('Could not retrieve keywords for dataset "%s": %s' % (dataset_name, error)) from error

	def get_metadata(self, oid):
		"""Look up an existing metadata resource by its identifier.

		Args:
			oid (str): The metadata identifier to search for.

		Returns:
			(dict | None): The matching metadata resource, or `None` if no
				resource with the given `oid` exists for this dataset.

		Raises:
			RuntimeError: If the API request fails for any reason.
		"""
		try:
			result = self.metadata_resource.get(oid=oid)
		except Exception as error:
			raise RuntimeError(
				'Could not retrieve metadata "%s" for dataset "%s": %s'
				% (oid, self.dataset['name'], self.api.exception_to_text(error))
			) from error

		return result['objects'][0] if result.get('objects', None) else None

	def get_data_location(self, file_url):
		"""Look up an existing data_location resource by file URL.

		Args:
			file_url (str): The file URL to search for, matched against
				data_location resources belonging to this dataset.

		Returns:
			(dict | None): The matching data_location resource, or `None`
				if no resource with the given `file_url` exists for this
				dataset.

		Raises:
			RuntimeError: If the API request fails for any reason.

		"""
		try:
			result = self.api.data_location.get(dataset__name=self.dataset['name'], file_url=file_url, limit=1)
		except Exception as error:
			raise RuntimeError(
				'Could not retrieve data location for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error

		return result['objects'][0] if result.get('objects', None) else None

	def create(self, resource_data):
		"""Create new metadata and data_location resources for the dataset.

		Args:
			resource_data (dict): The payload to POST to the dataset's
				metadata resource endpoint.

		Returns:
			(dict): The newly created metadata resource, as returned by the API.

		Raises:
			RuntimeError: If the API request fails for any reason.
		"""

		data_location = self.get_data_location(resource_data['data_location']['file_url'])

		if data_location is not None:
			self.logger.info(
				'Data location resource for URL %s already exists, reusing it!', resource_data['data_location']['file_url']
			)
			resource_data['data_location'] = data_location['resource_uri']
		else:
			resource_data['data_location']['dataset'] = self.dataset['resource_uri']

		try:
			result = self.metadata_resource.post(resource_data)
		except Exception as error:
			raise RuntimeError(
				'Could not create metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error

		return result

	def update(self, resource_data, oid=None):
		"""Update an existing metadata resource for the dataset.

		Args:
			resource_data (dict): Fields to update on the metadata resource.
				If it contains an `oid` key, that value is used as the
				resource identifier and removed from the payload before
				sending. A copy is made to prevent mutating the input dictionary.
			oid (str): Identifier of the resource to update. Only used if
				`resource_data` does not already contain an `oid` key.

		Returns:
			(dict): The updated metadata resource, as returned by the API.

		Raises:
			ValueError: If no `oid` is available from either
				`resource_data` or the `oid` argument.
			RuntimeError: If the API request fails for any reason.
		"""
		# Remove the oid from the metadata but copy it first as to not modify the input
		resource_data = resource_data.copy()
		oid = resource_data.pop('oid', oid)
		if not oid:
			raise ValueError('"oid" is undefined: it must be present in the resource_data dict or passed explicitly')
		try:
			result = self.metadata_resource(oid).patch(resource_data)
		except Exception as error:
			raise RuntimeError(
				'Could not update metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error
		return result

	def get_resource_data(self, item):
		"""Build a metadata + data_location resource payload for a single item.

		Must be implemented by subclasses.

		Args:
			item (Any): The item to process (a URL, file path, TAP record, etc.,
				depending on the subclass).

		Returns:
			(dict): A resource payload containing
				both the metadata fields and a nested `data_location` payload.
		"""
		raise NotImplementedError

	def process_items(self, items, submit=True, output=None):
		"""Extract, print, and optionally submit resource data for a list of items.

		For each item, extract the metadata and data_location payloads and
		print them as a single line of JSON to `output` (or standard
		output if `output` is not provided). If `submit` is True, also
		create the corresponding metadata and data_location resource on the
		SVO API.
		Errors for individual items are logged and do not stop processing
		of the remaining items.

		Args:
			items (Iterable): Items to process (URLs, file paths, TAP
				records, etc., depending on the subclass).
			submit (bool): If True (the default), also submit each resource
				payload to the SVO API.
			output(TextIO): A writable file-like object that JSON-serialized
				resource payloads are printed to, one per line. Defaults to
				standard output.
		Returns:
			(None)
		"""

		serializer = JsonSerializer()

		for item in items:
			self.logger.info('Processing %s', item)

			try:
				resource_data = self.get_resource_data(item)
			except Exception as error:
				self.logger.error('Could not extract resource data for %s: %s', item, error)
				continue

			print(serializer.dumps(resource_data), file=output)

			if submit:
				try:
					result = self.create(resource_data)
				except Exception as error:
					self.logger.error('Could not create new metadata or data_location resource for %s: %s', item, error)
				else:
					self.logger.info('Created new metadata resource "%s" for %s', result['resource_uri'], item)
