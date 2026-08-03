__all__ = ['Extractor']


class Extractor:
	"""Base class for extracting dataset resources through the API."""

	def __init__(self, restful_api, dataset_name):
		"""Initialize the extractor with API access and dataset metadata."""
		self.api = restful_api
		self.dataset = self.get_dataset(dataset_name)
		self.keywords = self.get_keywords(dataset_name)

	def get_dataset(self, dataset_name):
		"""Fetch dataset information from the API."""
		try:
			return self.api.dataset(dataset_name).get()
		except Exception as error:
			raise RuntimeError('Could not retrieve info for dataset "%s": %s', (dataset_name, error)) from error

	def get_keywords(self, dataset_name):
		"""Return the list of keywords associated with the dataset."""
		try:
			return self.api.keyword.get(dataset__name=dataset_name, limit=0)['objects']
		except Exception as error:
			raise RuntimeError('Could not retrieve keywords for dataset "%s": %s' % (dataset_name, error)) from error
