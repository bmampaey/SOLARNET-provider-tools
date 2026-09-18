#!/usr/bin/env python3
import argparse
import collections
import json
import logging
import re

import numpy.ma as ma
import pyvo

QUERY_TIMEOUT = 600
QUERY_MAX_RETRIES = 3
MAX_SAMPLE_VALUES = 100
MAX_DISTINCT_VALUES = 3


class KeywordInspector:
	"""Inspect the columns of of a TAP service table, and build the information needed for the SVO"""

	# Conversion from VOtable datatype to SVO keyword type
	# See https://www.ivoa.net/documents/VOTable/20130920/REC-VOTable-1.3-20130920.html#ToC11
	KEYWORD_TYPE = {
		'boolean': 'boolean',
		'bit': 'integer',
		'unsignedByte': 'integer',
		'short': 'integer',
		'int': 'integer',
		'long': 'integer',
		'char': 'text',
		'unicodeChar': 'text',
		'float': 'real',
		'double': 'real',
		'floatComplex': None,
		'doubleComplex': None,
	}

	def __init__(
		self,
		url,
		table_name=None,
		exclude_columns=[],
		max_sample_values=MAX_SAMPLE_VALUES,
		max_distinct_values=MAX_DISTINCT_VALUES,
		query_timeout=QUERY_TIMEOUT,
		query_max_retries=QUERY_MAX_RETRIES,
	):
		self.exclude_columns = exclude_columns
		self.max_sample_values = max_sample_values
		self.max_distinct_values = max_distinct_values
		self.query_timeout = query_timeout
		self.query_max_retries = query_max_retries
		self.service = pyvo.dal.TAPService(url)

		if table_name is None:
			table_name = self.resolve_ambiguity('Select the table to inspect', [table.name for table in self.service.tables])
		try:
			self.table = self.service.tables[table_name]
		except KeyError as error:
			raise ValueError('Unknown table "%s" in service "%s"' % (table_name, url)) from error

		logging.info('Table description:\n%s\n', self.table.description)

	def get_keyword_infos(self):
		"""Return a list of all keyword info {name, verbose_name, type, unit, description} resolving ambiguities by aking the user"""

		keyword_infos = list()

		# Get sample values to help user resolve ambiguities for keyword type and constant_value
		sample_values = self.get_sample_values()

		for column in self.table.columns:
			if column.name in self.exclude_columns:
				logging.info('Skipping excluded column "%s"', column.name)
				continue

			logging.info('Processing column %s', column.name)
			column_sample_values = sample_values.get(column.name, collections.Counter())

			# Check if column has a unique value
			constant_value = None
			if self.max_distinct_values and column_sample_values and len(column_sample_values) < self.max_distinct_values:
				CHOICES = {
					'D': 'define a regular keyword',
					'C': 'define a keyword with a constant value',
					'S': 'skip the column',
				}

				choice = self.resolve_ambiguity(
					'Column %s sample has only %s distinct values\n- %s\nDo you want to'
					% (
						column.name,
						len(column_sample_values),
						'\n- '.join('{!r} ({})'.format(*sample) for sample in column_sample_values.items()),
					),
					list(CHOICES.values()),
				)

				if choice == CHOICES['S']:
					logging.info('Skipping column "%s"', column.name)
					continue
				elif choice == CHOICES['C']:
					constant_value = self.get_constant_value(column)

			keyword_infos.append({
				'name': self.get_keyword_name(column),
				'verbose_name': self.get_keyword_verbose_name(column),
				'type': self.get_keyword_type(column, column_sample_values),
				'unit': self.get_keyword_unit(column),
				'description': self.get_keyword_description(column),
				'constant_value': constant_value,
			})

		return keyword_infos

	def execute_tap_query(self, query):
		logging.debug('Executing TAP query "%s"', query)
		try:
			records = self.service.run_async(query, timeout=self.query_timeout, max_retries=self.query_max_retries)
		except Exception as error:
			logging.error('TAP query "%s" failed : %s', query, error)
			raise
		return records

	def get_sample_values(self):
		"""Query a sample of table rows and return, for each column,
		a Counter mapping each distinct value to its number of
		occurrences in the sample.
		"""
		query = 'SELECT TOP {max_sample_values:d} * FROM {table_name}'.format(
			max_sample_values=self.max_sample_values, table_name=self.table.name
		)

		records = self.execute_tap_query(query)

		table = records.to_table()

		value_counts = {}
		for col_name in table.colnames:
			values = (None if v is ma.masked else v for v in table[col_name])
			value_counts[col_name] = collections.Counter(values)

		return value_counts

	def get_keyword_name(self, column):
		"""Extract the keyword name from the column definition"""
		# Convert the column name into a SVO compliant keyword name

		# Convert the keyword to lower case
		name = column.name.strip().lower()
		# replace any unusual name character by an underscore
		name = re.sub(r'[^a-zA-Z0-9_]', r'_', name)
		# remove consecutive underscores
		while name.find(r'__') >= 0:
			name = name.replace(r'__', r'_')
		# remove underscores at extremities
		name = name.strip('_')

		return name

	def get_keyword_verbose_name(self, column):
		"""Extract the keyword verbose name from the column definition"""
		return column.name

	def get_keyword_type(self, column, sample_values):
		"""Extract the keyword type from the column definition"""

		# Inspect the column datatype to infer the SVO keyword type equivalant
		# VOtable do not have the equivalant datetime datatype, so inspect also the ucd if it contains the time word

		keyword_type = self.KEYWORD_TYPE.get(column.datatype.content, None)

		samples = ', '.join('{!r} ({})'.format(*sample) for sample in sample_values.most_common(3))

		if keyword_type is None:
			keyword_type = self.resolve_ambiguity(
				'Column "{column.name}" datatype "{column.datatype.content}" is not supported, select appropriate SVO keyword type\nUnit: {unit}\nSample values: {samples}'.format(
					column=column, unit=self.get_keyword_unit(column), samples=samples
				),
				['text', 'boolean', 'integer', 'real', 'time (ISO 8601)'],
			)

		if keyword_type != 'time (ISO 8601)' and column.ucd and 'time' in column.ucd:
			keyword_type = self.resolve_ambiguity(
				'Column "{column.name}" has ucd "{column.ucd}", select appropriate SVO keyword type\nUnit: {unit}\nSample values: {samples}'.format(
					column=column, unit=self.get_keyword_unit(column), samples=samples
				),
				['time (ISO 8601)', keyword_type],
			)

		if 'x' in column.datatype.arraysize or (
			column.datatype.arraysize != '1' and keyword_type not in ['text', 'time (ISO 8601)']
		):
			keyword_type = self.resolve_ambiguity(
				'Column "{column.name}" datatype "{column.datatype.content}" with arraysize {column.datatype.arraysize} is not supported, select appropriate SVO keyword type\nUnit: {unit}\nSample values: {samples}'.format(
					column=column, unit=self.get_keyword_unit(column), samples=samples
				),
				['text', 'boolean', 'integer', 'real', 'time (ISO 8601)'],
			)

		return keyword_type

	def get_keyword_unit(self, column):
		"""Extract the keyword type from the column definition"""
		return column.unit

	def get_keyword_description(self, column):
		"""Extract the keyword description from the column definition"""
		return column.description

	def get_constant_value(self, column):
		"""Check if the column has a constant value"""

		# Fetch the most frequently occurring values for a column
		query = 'SELECT TOP {max_distinct_values:d} "{column_name}" as val, COUNT(*) FROM {table_name} GROUP BY "{column_name}" ORDER BY COUNT(*) DESC'.format(
			max_distinct_values=self.max_distinct_values, column_name=column.name, table_name=self.table.name
		)

		try:
			records = self.execute_tap_query(query)
		except Exception as error:
			return None

		constant_value = self.resolve_ambiguity(
			'Column "{column}" has {distinct_count} distinct values, select an option below'.format(
				column=column.name, distinct_count=len(records)
			),
			['{!r} ({})'.format(*record.values()) for record in records] + [None],
		)

		return constant_value

	def resolve_ambiguity(self, title, choices):
		"""Resolve an ambiguity between different values by asking the user"""
		print(title)

		for i, choice in enumerate(choices):
			print('[%d] %s' % (i, choice))

		UNSET = object()

		value = UNSET
		while value is UNSET:
			selection = input('Please enter one of the options between [] or enter for first one: ') or '0'
			if selection.isdecimal() and int(selection) < len(choices):
				value = choices[int(selection)]
			else:
				print('Invalid selection', selection)

		return value


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Query a TAP service to extract the keywords definitions for the SOLARNET Virtual Observatory.'
	)
	parser.add_argument('service_url', metavar='URL', help='The URL of the TAP service')
	parser.add_argument(
		'--table-name',
		'-t',
		help='Name of the TAP table to use; if omitted, you will be asked to choose from the available tables',
	)
	parser.add_argument(
		'--output',
		'-o',
		default='keywords_definitions.json',
		help='Path to the output JSON file with the keywords definitions',
	)
	parser.add_argument(
		'--exclude',
		'-E',
		metavar='COLUMN',
		default=[],
		action='append',
		help='Columns to exclude, can be specified multiple times',
	)
	parser.add_argument(
		'--max-sample-values',
		'-c',
		type=int,
		default=MAX_SAMPLE_VALUES,
		metavar='MAX',
		help=f'Maximum number of sample values to fetch from the TAP service (default {MAX_SAMPLE_VALUES})',
	)
	parser.add_argument(
		'--max-distinct-values',
		'-m',
		default=MAX_DISTINCT_VALUES,
		type=int,
		metavar='MAX',
		help=f'Maximum number of distinct values for a field to be considered constant (default {MAX_DISTINCT_VALUES})',
	)
	parser.add_argument('--debug', '-d', action='store_true', help='Set the logging level to debug')

	args = parser.parse_args()

	# Setup the logging
	if args.debug:
		logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s: %(funcName)s %(message)s')
	else:
		logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

	# Remove asyncio logging
	logging.getLogger('asyncio').setLevel(logging.WARNING)

	# Process the fits files and write the keyword info to the output file
	keyword_inspector = KeywordInspector(
		args.service_url,
		table_name=args.table_name,
		exclude_columns=args.exclude,
		max_sample_values=args.max_sample_values,
		max_distinct_values=args.max_distinct_values,
	)

	keyword_infos = keyword_inspector.get_keyword_infos()

	try:
		with open(args.output, 'tw', encoding='UTF-8') as output_file:
			json.dump(keyword_infos, output_file, ensure_ascii=False, indent='\t')
	except IOError as error:
		logging.critical('Could not open file %s for writing: %s' % (args.output, error))
		raise
	except Exception as error:
		logging.critical('Fatal error: %s' % error)
		raise
	else:
		logging.info('Wrote keywords definitions to file %s', args.output)
