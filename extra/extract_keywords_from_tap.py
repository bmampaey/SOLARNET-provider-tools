#!/usr/bin/env python3
import logging
import argparse
import json
import re
import pyvo


class KeywordInspector:
	"""Given the URL of a TAP service, inspect the columns and output the information needed for the SVO database"""

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

	def __init__(self, url, exclude_columns=[]):
		self.service = pyvo.dal.TAPService(url)
		table_name = self.resolve_ambiguity('Select the table to inspect', [table.name for table in self.service.tables])
		self.table = self.service.tables[table_name]
		self.exclude_columns = exclude_columns
		logging.info('Table description:\n%s\n', self.table.description)

	def get_sample_record(self):
		"""Get the first record from the table to use as sample"""
		query = 'SELECT TOP 1 * FROM %s' % self.table.name

		logging.debug('Executing TAP query "%s"', query)
		try:
			records = self.service.search(query)
		except Exception as error:
			logging.warning('TAP query "%s" failed : %s', query, error)
			raise

		return records[0]

	def get_keyword_infos(self):
		"""Return a list of all keyword info {name, verbose_name, type, unit, description} resolving ambiguities by aking the user"""

		keyword_infos = list()

		# Get a sample value to help user resolve ambiguities in keyword type
		sample_record = self.get_sample_record()

		for column in self.table.columns:
			if column.name in self.exclude_columns:
				logging.info('Skipping excluded column "%s"', column.name)
			else:
				keyword_infos.append(
					{
						'name': self.get_keyword_name(column),
						'verbose_name': self.get_keyword_verbose_name(column),
						'type': self.get_keyword_type(column, sample_record.get(column.name)),
						'unit': self.get_keyword_unit(column),
						'description': self.get_keyword_description(column),
					}
				)

		return keyword_infos

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

	def get_keyword_type(self, column, sample_value):
		"""Extract the keyword type from the column definition"""

		# Inspect the column datatype to infer the SVO keyword type equivalant
		# VOtable do not have the equivalant datetime datatype, so inspect also the ucd if it contains the time word

		keyword_type = self.KEYWORD_TYPE.get(column.datatype.content, None)

		if keyword_type is None:
			keyword_type = self.resolve_ambiguity(
				'Column "{column.name}" datatype "{column.datatype.content}" is not supported, select appropriate SVO keyword type\nUnit: {unit}\nSample value: {sample_value}'.format(
					column=column, unit=self.get_keyword_unit(column), sample_value=sample_value
				),
				['text', 'boolean', 'integer', 'real', 'time (ISO 8601)'],
			)

		if keyword_type != 'time (ISO 8601)' and 'time' in column.ucd:
			keyword_type = self.resolve_ambiguity(
				'Column "{column.name}" has ucd "{column.ucd}", select appropriate SVO keyword type\nUnit: {unit}\nSample value: {sample_value}'.format(
					column=column, unit=self.get_keyword_unit(column), sample_value=sample_value
				),
				['time (ISO 8601)', keyword_type],
			)

		if 'x' in column.datatype.arraysize or (
			column.datatype.arraysize != '1' and keyword_type not in ['text', 'time (ISO 8601)']
		):
			keyword_type = self.resolve_ambiguity(
				'Column "{column.name}" datatype "{column.datatype.content}" with arraysize {column.datatype.arraysize} is not supported, select appropriate SVO keyword type\nUnit: {unit}\nSample value: {sample_value}'.format(
					column=column, unit=self.get_keyword_unit(column), sample_value=sample_value
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

	def resolve_ambiguity(self, title, choices):
		"""Resolve an ambiguity between different values by asking the user"""
		print(title)

		for i, choice in enumerate(choices):
			print('[%d] %s' % (i, choice))

		value = None

		while value is None:
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
	parser.add_argument('--output', '-o', help='Path to the output JSON file with the keywords definitions')
	parser.add_argument(
		'--exclude',
		'-E',
		metavar='COLUMN',
		default=[],
		action='append',
		help='Columns to exclude, can be specified multiple times',
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
	keyword_inspector = KeywordInspector(args.service_url, exclude_columns=args.exclude)

	if args.output:
		output_filename = args.output
	else:
		output_filename = '%s_keywords_definition.json' % keyword_inspector.table.name

	try:
		with open(output_filename, 'tw', encoding='UTF-8') as output_file:
			json.dump(keyword_inspector.get_keyword_infos(), output_file, ensure_ascii=False, indent='\t')
	except IOError as error:
		logging.critical('Could not open file %s for writing: %s' % (args.output, error))
		raise
	except Exception as error:
		logging.critical('Fatal error: %s' % error)
		raise
	else:
		logging.info('Wrote keywords description file %s', output_filename)
