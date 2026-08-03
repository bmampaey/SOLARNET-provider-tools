import logging

__all__ = ['Provider']


class Provider:
	"""Base class for submitting dataset metadata and data-location resources through the RESTful API.

	Handles fetching dataset info and keywords on initialization, and
	provides generic create/update/lookup operations against the
	metadata and data_location API resources. Subclasses are expected
	to implement dataset-type-specific logic (e.g. building resource
	payloads from source files).
	"""

	def __init__(self, restful_api, dataset_name, logger=None):
		"""Initialize the provider and load dataset metadata and keywords.

		Args:
			restful_api: A initialized RESTfulApi with proper authentication
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
			dict: The dataset resource returned by the API.

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
			list: The list of keyword objects associated with the dataset
			(unbounded, since the request is made with ``limit=0``).

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
			oid: The metadata identifier to search for.

		Returns:
			dict or None: The matching metadata resource, or ``None`` if no
			resource with the given ``oid`` exists for this dataset.

		Raises:
			RuntimeError: If the API request fails for any reason.
		"""
		try:
			result = self.metadata_resource.get(oid=oid)
		except Exception as error:
			raise RuntimeError(
				'Could not retrieve metadata for dataset "%s": %s' % (self.dataset['name'], self.api.exception_to_text(error))
			) from error

		return result['objects'][0] if result.get('objects', None) else None

	def get_data_location(self, file_url):
		"""Look up an existing data_location resource by file URL.

		Args:
			file_url (str): The file URL to search for, matched against
				data_location resources belonging to this dataset.

		Returns:
			dict or None: The matching data_location resource, or ``None``
			if no resource with the given ``file_url`` exists for this
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
		"""Create a new metadata and data_location resources for the dataset.

		Args:
			resource_data (dict): The payload to POST to the dataset's
				metadata resource endpoint.

		Returns:
			dict: The newly created metadata resource, as returned by the API.

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
				If it contains an ``oid`` key, that value is used as the
				resource identifier and removed from the payload before
				sending. A copy is made so the caller's dict is not mutated.
			oid: Identifier of the resource to update. Only used if
				``resource_data`` does not already contain an ``oid`` key.

		Returns:
			dict: The updated metadata resource, as returned by the API.

		Raises:
			ValueError: If no ``oid`` is available from either
				``resource_data`` or the ``oid`` argument.
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
				'Could not update metadata %s for dataset "%s": %s' % (self.dataset['name'], oid, self.api.exception_to_text(error))
			) from error
		return result
