import datetime
import logging
import math

__all__ = ['Metadata']


class Metadata:
	"""Base class for building metadata resource payloads."""

	# Correspondence mapping between the python type of the field value and the keyword type
	KEYWORD_TYPE_CHECK = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': datetime.datetime,
	}

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
		# Else extract it from the source data
		field_value_getter = getattr(self, 'get_field_' + keyword['name'], None)
		if field_value_getter is not None:
			field_value = field_value_getter()
		else:
			field_value = self.extract_field_value(keyword)

		self.check_field_value_type(field_value, keyword)

		return field_value

	def extract_field_value(self, keyword):
		"""Extract a field value from the source data."""
		raise NotImplementedError()

	def check_field_value_type(self, field_value, keyword):
		"""Check that a field value is of the expected keyword type."""
		try:
			field_value_type = self.KEYWORD_TYPE_CHECK[keyword['type']]
		except KeyError as error:
			raise NotImplementedError('Unknown type %s of keyword %s' % (keyword['type'], keyword['name'])) from error

		if not isinstance(field_value, field_value_type):
			raise TypeError(
				'Value for field %s expected to be %s but is %s (%s)'
				% (keyword['name'], field_value_type, type(field_value), field_value)
			)

		if isinstance(field_value, float) and not math.isfinite(field_value):
			raise ValueError('Value for field %s must be finite, not %s' % (keyword['name'], field_value))
