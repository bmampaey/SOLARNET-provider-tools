import logging
from math import isfinite

from dateutil.parser import parse

__all__ = ['Metadata']


class Metadata:
	"""Base class for building metadata resource payloads."""

	# Methods to convert the FITS keywords values to the proper SVO type
	KEYWORD_VALUE_CONVERSION = {'text': str, 'boolean': bool, 'integer': int, 'real': float, 'time (ISO 8601)': parse}

	def __init__(self, keywords):
		"""Store the keyword definitions used to populate metadata field values."""
		self.keywords = keywords

	def get_resource_data(self):
		"""Return a dictionary containing the metadata payload fields."""

		# Create the resource data dict with the required keywords
		# and use the metadata keyword definitions to extract the value for the rest of the fields
		resource_data = {
			field: self.get_field_value(field) for field in ('oid', 'date_beg', 'date_end', 'wavemin', 'wavemax')
		}

		for keyword in self.keywords:
			try:
				resource_data[keyword['name']] = self.get_field_value(keyword)
			except ValueError as error:
				logging.warning('Could not get value for field %s: %s', keyword['name'], error)
			else:
				logging.debug('Field %s has value "%s"', keyword['name'], resource_data[keyword['name']])

		return resource_data

	def get_field_value(self, keyword):
		"""Return the metadata value defined by a keyword specification."""
		# If there is a specific method to get the value for the field, use it
		field_value_getter = getattr(self, 'get_field_' + keyword['name'], None)
		if field_value_getter is not None:
			return field_value_getter()

		# Else use the general extraction method and convert it following the keyword type
		field_value = self.extract_field_value(keyword)
		field_value = self.convert_field_value(field_value, keyword['type'])

	def extract_field_value(self, keyword):
		"""Extract a field value from the source data."""
		raise NotImplementedError()

	def convert_field_value(self, field_value, keyword_type):
		"""Convert a field value to the expected metadata type."""
		keyword_value_conversion = self.KEYWORD_VALUE_CONVERSION.get(keyword_type, None)

		if keyword_value_conversion is not None:
			try:
				field_value = keyword_value_conversion(field_value)
			except Exception as error:
				raise ValueError('Could not convert value "%s" to %s' % (field_value, keyword_type)) from error

		if keyword_type == 'real' and not isfinite(field_value):
			field_value = None

		return field_value
