import datetime
import numbers

__all__ = ['Metadata']


class Metadata:
	"""Base class for building metadata resource payloads.

	Subclasses must implement :meth:`extract_field_value` to define how a
	field's value is pulled from their particular source data (e.g. a TAP
	record or a FITS header). Subclasses can optionally define
	``get_<field_name>`` methods when custom extraction logic
	is needed for specific fields.
	"""

	# Correspondence mapping between the python type of the field value and the keyword type
	KEYWORD_TYPE_CHECK = {
		'text': str,
		'boolean': bool,
		'integer': numbers.Integral,
		'real': numbers.Real,
		'time (ISO 8601)': datetime.datetime,
	}

	REQUIRED_KEYWORDS = [
		{'name': 'oid', 'verbose_name': 'Observation ID', 'type': 'text'},
		{'name': 'date_beg', 'verbose_name': 'DATE-BEG', 'type': 'time (ISO 8601)'},
		{'name': 'date_end', 'verbose_name': 'DATE-END', 'type': 'time (ISO 8601)'},
		{'name': 'wavemin', 'verbose_name': 'WAVEMIN', 'type': 'real'},
		{'name': 'wavemax', 'verbose_name': 'WAVEMAX', 'type': 'real'},
	]

	def __init__(self, keywords):
		"""Store the keyword definitions used to populate metadata field values.

		Args:
			keywords (list): Keyword definitions (each a dict with at least
				``name`` and ``type`` keys) describing the metadata
				fields to extract.
		"""
		self.keywords = {keyword['name']: keyword for keyword in keywords}

		# Add and check the required keywords if not provided
		for keyword in self.REQUIRED_KEYWORDS:
			if keyword['name'] not in self.keywords:
				self.keywords[keyword['name']] = keyword
			else:
				if self.keywords[keyword['name']]['type'] != keyword['type']:
					raise ValueError(
						'Invalid type "%s" for required keyword "%s", must be "%s"'
						% (self.keywords[keyword['name']]['type'], keyword['name'], keyword['type'])
					)

	def get_resource_data(self):
		"""Build the metadata resource payload and validate field value types.

		Returns:
			dict: The metadata resource payload, keyed by field name.
		"""
		resource_data = {}

		for field_name in self.keywords:
			# Subclasses may define direct getter for field values
			# e.g. def get_oid(self) that returns the oid value
			field_value_getter = getattr(self, 'get_' + field_name, None)
			if field_value_getter is not None:
				field_value = field_value_getter()
			else:
				field_value = self.extract_field_value(field_name)

			self.check_field_value_type(field_name, field_value)

			resource_data[field_name] = field_value

		return resource_data

	def extract_field_value(self, field_name):
		"""Extract a field value from the source data.

		Must be implemented by subclasses to define how values are pulled
		from their specific source (e.g. a TAP record or FITS header).

		Args:
			field_name (str): The name of the resource field.

		Returns:
			The extracted field value. The value is validated separately by
			:meth:`check_field_value_type`.

		Raises:
			NotImplementedError: Always, unless overridden by a subclass.
		"""
		raise NotImplementedError()

	def check_field_value_type(self, field_name, field_value):
		"""Validate that a field value matches its keyword's expected type.

		Args:
			field_name (str): The name of the resource field.
			field_value (object): The value to validate.

		Raises:
			KeyError: If no keyword correspond to the field name, or if the
				type is missing in the keyword definition.
			NotImplementedError: If the keyword's ``type`` is not a known
				entry in :data:`KEYWORD_TYPE_CHECK`.
			TypeError: If ``field_value`` is not an instance of the type
				expected for this keyword. ``None`` is accepted for all fields.
		"""
		if field_value is None:
			return

		try:
			keyword = self.keywords[field_name]
		except KeyError as error:
			raise KeyError('No keyword corresponds to field name %s' % field_name) from error

		try:
			keyword_type = keyword['type']
		except KeyError as error:
			raise KeyError('Keyword for field name %s does not define a type' % field_name) from error

		try:
			field_value_type = self.KEYWORD_TYPE_CHECK[keyword_type]
		except KeyError as error:
			raise NotImplementedError('Unknown type %s of keyword %s' % (keyword_type, field_name)) from error

		# Since isinstance(True, int) is True, we need to explicitly discriminate against it
		if not isinstance(field_value, field_value_type) or (
			keyword_type in ('integer', 'real') and isinstance(field_value, bool)
		):
			raise TypeError(
				'Value for field %s expected to be %s but is %s (%s)'
				% (field_name, field_value_type, type(field_value), field_value)
			)
