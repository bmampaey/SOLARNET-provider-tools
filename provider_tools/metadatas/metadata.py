import datetime
import logging
import math

__all__ = ['Metadata']


class Metadata:
	"""Base class for building metadata resource payloads.

	Subclasses must implement :meth:`extract_field_value` to define how a
	field's value is pulled from their particular source data (e.g. a TAP
	record or a FITS header). Individual fields can also be overridden via
	``get_field_<name>`` methods when custom extraction logic is needed for
	a specific field.
	"""

	# Correspondence mapping between the python type of the field value and the keyword type
	KEYWORD_TYPE_CHECK = {
		'text': str,
		'boolean': bool,
		'integer': int,
		'real': float,
		'time (ISO 8601)': datetime.datetime,
	}

	REQUIRED_FIELDS = [
		{'name': 'oid', 'type': 'text'},
		{'name': 'date_beg', 'verbose_name': 'DATE-BEG', 'type': 'time (ISO 8601)'},
		{'name': 'date_end', 'verbose_name': 'DATE-END', 'type': 'time (ISO 8601)'},
		{'name': 'wavemin', 'verbose_name': 'WAVEMIN', 'type': 'real'},
		{'name': 'wavemax', 'verbose_name': 'WAVEMAX', 'type': 'real'},
	]

	def __init__(self, keywords):
		"""Store the keyword definitions used to populate metadata field values.

		Args:
			keywords (list): Keyword definitions (each a dict with at least
				``name`` and ``type`` keys) describing the additional
				metadata fields to extract, beyond the required fields
				(``oid``, ``date_beg``, ``date_end``, ``wavemin``, ``wavemax``).
		"""
		self.keywords = keywords

	def get_resource_data(self):
		"""Build the metadata payload from the required fields and keywords.

		The required fields (``oid``, ``date_beg``, ``date_end``, ``wavemin``,
		``wavemax``) are always extracted. In addition, each keyword defined
		in ``self.keywords`` is extracted and added to the payload under its
		own name; keywords whose value cannot be resolved are skipped with a
		warning rather than causing the whole payload to fail.

		Returns:
			dict: The metadata resource payload, keyed by field/keyword name.
		"""
		# Create the resource data dict with the required keywords
		# and use the metadata keyword definitions to extract the value for the rest of the fields
		resource_data = {field: self.get_field_value(field) for field in self.REQUIRED_FIELDS}

		for keyword in self.keywords:
			try:
				resource_data[keyword['name']] = self.get_field_value(keyword)
			except ValueError as error:
				logging.warning('Could not get value for field %s: %s', keyword['name'], error)
			else:
				logging.debug('Field %s has value "%s"', keyword['name'], resource_data[keyword['name']])

		return resource_data

	def get_field_value(self, keyword):
		"""Resolve the value for a single field, validating its type.

		If a ``get_field_<name>`` method exists on the instance, it is used
		to compute the value; otherwise the value is pulled from the source
		data via :meth:`extract_field_value`. The resulting value is then
		checked against the keyword's declared type before being returned.

		Args:
			keyword: A keyword definition (dict with ``name`` and
				``type`` keys).

		Returns:
			The extracted and type-checked field value.

		Raises:
			ValueError: If the value could not be extracted or is invalid
				(e.g. missing from the source data, or a non-finite float).
			TypeError: If the extracted value does not match the type
				expected for the keyword.
			NotImplementedError: If the keyword's declared type is unknown.
		"""
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
		"""Extract a field's raw value from the source data.

		Must be implemented by subclasses to define how values are pulled
		from their specific source (e.g. a TAP record or FITS header).

		Args:
			keyword (dict): Keyword definition describing the field to
				extract, including its ``verbose_name`` (usually the source field
				name) and ``type``.

		Returns:
			The extracted field value, converted to the appropriate type.

		Raises:
			NotImplementedError: Always, unless overridden by a subclass.
		"""
		raise NotImplementedError()

	def check_field_value_type(self, field_value, keyword):
		"""Validate that a field value matches its keyword's expected type.

		Args:
			field_value: The value to validate.
			keyword (dict): Keyword definition whose ``type`` entry
				determines the expected Python type, via
				:data:`KEYWORD_TYPE_CHECK`.

		Raises:
			NotImplementedError: If the keyword's ``type`` is not a known
				entry in :data:`KEYWORD_TYPE_CHECK`.
			TypeError: If ``field_value`` is not an instance of the type
				expected for this keyword.
			ValueError: If ``field_value`` is a float and is not finite
				(e.g. ``NaN`` or infinite).
		"""
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
