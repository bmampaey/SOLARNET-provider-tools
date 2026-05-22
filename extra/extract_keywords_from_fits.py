#!/usr/bin/env python3
import argparse
import json
import logging
import os
import pickle
import re
import string
from collections import Counter, defaultdict
from datetime import datetime

from astropy.io import fits
from dateutil.parser import parse as parse_date

DEFAULT_EXCLUDE_KEYWORDS = ['DATASUM', 'CHECKSUM', 'SIMPLE', 'BITPIX']


class KeywordInspector:
	"""Given a list of FITS file paths or URLs, inspect the keywords and output the information needed for the SVO database"""

	# Default keywords to exclude

	# SVO keyword type names
	KEYWORD_TYPE_NAMES = {
		bool: 'boolean',
		int: 'integer',
		float: 'real',
		datetime: 'time (ISO 8601)',
		str: 'text',
	}

	# Regex to extract the unit from the comment in a FITS header
	# Units are usually specified at the beginning of the comment between brackets
	UNIT_PATTERN = re.compile(r'\s*\[\s*(?P<unit>[^\]]+)\s*\](?P<comment>.*)\s*')

	def __init__(self, hdu, exclude_keywords=None, add_fits_header=False, backup_file=None, force_interactive=False):
		self.hdu = hdu
		self.exclude_keywords = (
			set(keyword.upper() for keyword in exclude_keywords) if exclude_keywords is not None else set()
		)
		self.add_fits_header = add_fits_header
		self.backup_file = backup_file
		self.force_interactive = force_interactive

		# Count all the possible values and comments for each keyword
		self.keyword_values = defaultdict(Counter)
		self.keyword_comments = defaultdict(Counter)
		self.keyword_names = {}
		self.processed_fits_files = []

		self.log = logging.getLogger('keyword inspector')

	def process_fits_files(self, fits_files):
		"""Inspect all the FITS files and extract the keywords, their value and comment"""

		self.restore_backup()

		for fits_file in fits_files:
			if fits_file in self.processed_fits_files:
				self.log.info('File %s was already processed. Skipping !', fits_file)
			else:
				self.log.info('Processing file %s', fits_file)
				try:
					with fits.open(fits_file, cache=False, lazy_load_hdus=True) as hdu_list:
						try:
							header = hdu_list[self.hdu].header
						except IndexError:
							self.log.error('File %s does not have HDU %s . Skipping!', fits_file, hdu)
						else:
							self.inspect_header(header)
							self.processed_fits_files.append(fits_file)
							self.save_backup()
				except OSError as error:
					self.log.error('Could not open file %s: %s . Skipping!', fits_file, error)

	def inspect_header(self, header):
		"""Inspect a FITS header and add the keywords, their value and comment"""

		for card in header.cards:
			try:
				keyword = card.keyword
				value = card.value
				comment = card.comment
			except Exception as error:
				self.log.error('Could not parse card %s: %s . Skipping!', card, error)
				continue

			if keyword.upper() in self.exclude_keywords:
				self.log.info('Keyword %s is excluded', keyword)
				continue

			else:
				self.keyword_values[keyword][value] += 1
				self.keyword_comments[keyword][comment] += 1

	def get_keyword_infos(self):
		"""Return a list of all keyword info {name, verbose_name, type, unit, description} resolving ambiguities by aking the user"""

		keyword_infos = list()

		if self.add_fits_header:
			keyword_infos.append({
				'name': 'fits_header',
				'verbose_name': 'FITS header',
				'type': self.KEYWORD_TYPE_NAMES[str],
				'unit': None,
				'description': 'Header of HDU %s in the FITS file' % self.hdu,
			})

		for keyword, values in self.keyword_values.items():
			keyword_type = self.resolve_keyword_type(keyword)
			keyword_unit, keyword_description = self.resolve_keyword_unit_description(keyword)

			keyword_infos.append({
				'name': self.resolve_keyword_name(keyword=keyword, name=keyword.strip().lower()),
				'verbose_name': keyword,
				'type': keyword_type,
				'unit': keyword_unit,
				'description': keyword_description,
			})

		return keyword_infos

	def resolve_keyword_type(self, keyword):
		"""Convert all the possible keyword values to SVO compliant type name, and if there is more than one, resolve the ambiguity by asking the user"""

		keyword_types = defaultdict(Counter)
		for value, count in self.keyword_values[keyword].items():
			keyword_type = self.get_keyword_type(value)
			keyword_types[keyword_type][value] = count

		if len(keyword_types) > 1 or self.force_interactive:
			# Add the missing types to the choices
			for keyword_type in self.KEYWORD_TYPE_NAMES.values():
				if keyword_type not in keyword_types:
					keyword_types[keyword_type][None] = 0
			keyword_type = self.resolve_ambiguity(keyword, 'type', keyword_types)
		else:
			keyword_type = keyword_types.popitem()[0]

		return keyword_type

	def get_keyword_type(self, value):
		"""Convert a keyword value into a SVO compliant type name"""

		# Try to infer the type name from the type of the value
		# if not found, assume it is a string keyword
		for type, name in self.KEYWORD_TYPE_NAMES.items():
			if isinstance(value, type):
				keyword_type_name = name
				break
		else:
			self.log.warning('Could not infer the type for value "%s", assume it is a text keyword', value)
			return self.KEYWORD_TYPE_NAMES[str]

		# Time keywords are represented by string, so try to see if it is a valid time value
		if keyword_type_name is self.KEYWORD_TYPE_NAMES[str]:
			try:
				date = parse_date(value)
			except ValueError:
				pass
			else:
				self.log.debug('Could parse the value "%s" to date "%s", assume it is a time keyword', value, date)
				keyword_type_name = self.KEYWORD_TYPE_NAMES[datetime]

		self.log.debug('Type name for value "%s" is %s', value, keyword_type_name)

		return keyword_type_name

	def resolve_keyword_unit_description(self, keyword):
		"""Convert all the possible keyword comments into unit and description, and if there is more than one, resolve the ambiguity by asking the user"""

		keyword_units = defaultdict(Counter)
		keyword_descriptions = defaultdict(Counter)

		for comment, count in self.keyword_comments[keyword].items():
			keyword_unit, keyword_description = self.get_keyword_unit_description(comment)
			keyword_units[keyword_unit][comment] = count
			keyword_descriptions[keyword_description][comment] = count

		if len(keyword_units) > 1 or self.force_interactive:
			# Make sure that None is an option
			keyword_units[None]
			keyword_unit = self.resolve_ambiguity(keyword, 'unit', keyword_units, manual_input=True)
		else:
			keyword_unit = keyword_units.popitem()[0]

		if len(keyword_descriptions) > 1 or self.force_interactive:
			keyword_description = self.resolve_ambiguity(keyword, 'description', keyword_descriptions, manual_input=True)
		else:
			keyword_description = keyword_descriptions.popitem()[0]

		return keyword_unit, keyword_description

	def get_keyword_unit_description(self, comment):
		"""Convert a keyword comment into a unit and description"""

		match = self.UNIT_PATTERN.match(comment)
		if match is None:
			self.log.debug('No unit found in comment %s', comment)
			return None, comment.strip()
		else:
			self.log.debug('Unit "%s" found in comment %s', match['unit'], comment)
			return match['unit'].strip(), match['comment'].strip()

	def resolve_ambiguity(self, keyword, subject, values, manual_input=False):
		"""Resolve an ambiguity between different values by asking the user"""

		# Sort the values by the most common ones
		options = sorted(
			((sum(counter.values()), option, counter.most_common(3)) for option, counter in values.items()), reverse=True
		)

		# Ask user to select between possible options
		print('Multiple', subject, 'found for keyword', keyword)
		for i, (count, option, examples) in enumerate(options):
			print('[%d] %s (%d occurences) e.g. %s' % (i, option, count, examples))
		if manual_input:
			print('[M] manual input')

		value = None

		while value is None:
			selection = input('Please enter one of the options between [] or enter for first one: ') or '0'
			if selection.isdecimal() and int(selection) < len(options):
				value = options[int(selection)][1]
			elif manual_input and selection == 'M':
				value = input('Please enter the %s:' % subject)
			else:
				print('Invalid selection', selection)

		return value

	def resolve_keyword_name(self, keyword, name=''):
		"""Convert a FITS keyword into a SVO compliant keyword name"""

		# Check if the name is valid
		name_is_valid = True

		if not name:
			print('Field name for keyword "%s" must not be empty' % keyword)
			name_is_valid = False
		elif name in self.keyword_names:
			print(
				'Field name "%s" for keyword "%s" was already used for keyword "%s"' % (name, keyword, self.keyword_names[name])
			)
			name_is_valid = False
		else:
			invalid_errors = []

			# Check for invalid characters, only simple characters are allowed to avoid errors in the RESTful API
			if invalid_chars := set(name) - set(string.digits + string.ascii_lowercase + '_'):
				invalid_errors.append('characters %s are invalid' % ' '.join(invalid_chars))

			# Check consecutive underscores, these will infer with the RESTful API filtering
			if '__' in name:
				invalid_errors.append('double underscore are not allowed')

			# Check for invalid char at start and end, to avoid issues with python variable names
			if name[0] not in string.ascii_lowercase:
				invalid_errors.append('name cannot start with %s' % name[0])

			# Check for invalid char at end, to avoid issues with the RESTful API filtering
			if name[-1] not in (string.digits + string.ascii_lowercase):
				invalid_errors.append('name cannot end with %s' % name[-1])

			if invalid_errors:
				print('Field name "%s" for keyword "%s" is invalid:\n - %s' % (name, keyword, '\n - '.join(invalid_errors)))
				name_is_valid = False

		if name_is_valid:
			self.keyword_names[name] = keyword
			return name

		# Try to suggest a name that will be valid
		suggested_name = re.sub(r'[^a-z0-9_]', '_', keyword.strip().lower())

		while suggested_name and (suggested_name.find(r'__') >= 0):
			suggested_name = suggested_name.replace(r'__', r'_')

		while suggested_name and (suggested_name[0] not in string.ascii_lowercase):
			suggested_name = suggested_name[1:]

		while suggested_name and (suggested_name[-1] not in (string.digits + string.ascii_lowercase)):
			suggested_name = suggested_name[:-1]

		if suggested_name and suggested_name not in self.keyword_names:
			new_name = (
				input('Please enter a new name or press enter for suggested name "%s" : ' % suggested_name) or suggested_name
			)
		else:
			new_name = input('Please enter a new name : ')

		return self.resolve_keyword_name(keyword, new_name)

	def save_backup(self):
		"""Save the state of the keyword inspector from a backup file"""

		# Pickle the processed_fits_files, keyword_values and keyword_comments to the backup file
		if self.backup_file:
			self.log.debug('Saving state to backup file file %s', self.backup_file)
			try:
				with open(self.backup_file, 'wb') as file:
					pickle.dump(self.processed_fits_files, file)
					pickle.dump(self.keyword_values, file)
					pickle.dump(self.keyword_comments, file)
			except IOError as error:
				self.log.error('Could not open backup file %s for writing: %s', self.backup_file, error)
				raise
			except Exception as error:
				self.log.error('Could not save state to backup file: %s', error)
				raise

	def restore_backup(self):
		"""Restore the state of the keyword inspector from a backup file"""

		# Un-pickle the processed_fits_files, keyword_values and keyword_comments from the backup file
		if self.backup_file and os.path.isfile(self.backup_file):
			try:
				with open(self.backup_file, 'rb') as file:
					self.processed_fits_files = pickle.load(file)
					self.keyword_values = pickle.load(file)
					self.keyword_comments = pickle.load(file)
			except IOError as error:
				self.log.error('Could not open backup file %s for reading: %s', self.backup_file, error)
				raise
			except Exception as error:
				self.log.error('Could not restore state from backup file: %s', error)
				raise


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Inspect the header of one or more FITS files and extract the keywords definitions for the SOLARNET Virtual Observatory. If possible try to use files spread over the entire dataset, instead of consecutive files, this will allow to detect changes over time in the header of the files.'
	)
	parser.add_argument('fits_files', metavar='FITSFILE', nargs='*', help='Path or URL to a FITS file to inspect')
	parser.add_argument(
		'--hdu',
		default='0',
		help='The index or name of the HDU to inspect (if not specified the first HDU will be inspected)',
	)
	parser.add_argument(
		'--output',
		'-o',
		default='keywords_definitions.json',
		help='Path to the output JSON file with the keywords definitions',
	)
	parser.add_argument(
		'--exclude-keyword',
		'-e',
		metavar='KEYWORD',
		default=DEFAULT_EXCLUDE_KEYWORDS,
		action='append',
		help='Keywords to exclude, for exemple COMMENT or HISTORY, can be specified multiple times (default to %s)'
		% ', '.join(DEFAULT_EXCLUDE_KEYWORDS),
	)
	parser.add_argument(
		'--add-fits-header',
		action='store_true',
		help='If specified, the special keyword "FITS header" will be added to allow storing the entire FITS header in the SVO',
	)
	parser.add_argument(
		'--backup-file',
		'-b',
		metavar='BACKUPFILE',
		help='Path to a backup file where the progress will be saved so that it is possible to call the script again without rescanning the previously processed FITS files',
	)
	parser.add_argument(
		'--force-interactive',
		'-i',
		action='store_true',
		help='Always ask the user for each keyword even if there is no ambiguity',
	)
	parser.add_argument('--debug', '-d', action='store_true', help='Set the logging level to debug')

	args = parser.parse_args()

	if not args.fits_files:
		if not args.backup_file or not os.path.isfile(args.backup_file) or not os.path.getsize(args.backup_file):
			parser.error('You must provide at least 1 FITS file path or a previously created backup file')

	# Setup the logging
	if args.debug:
		logging.basicConfig(level=logging.DEBUG, format='%(levelname)-8s: %(funcName)s %(message)s')
	else:
		logging.basicConfig(level=logging.INFO, format='%(levelname)-8s: %(message)s')

	# Remove asyncio logging
	logging.getLogger('asyncio').setLevel(logging.WARNING)

	# The HDU can be specified as int or name, therefore we must convert it
	if args.hdu.isdecimal():
		hdu = int(args.hdu)
	else:
		hdu = args.hdu

	# Process the fits files and write the keyword info to the output file
	keyword_inspector = KeywordInspector(
		hdu=hdu,
		exclude_keywords=args.exclude_keyword,
		add_fits_header=args.add_fits_header,
		backup_file=args.backup_file,
		force_interactive=args.force_interactive,
	)

	try:
		keyword_inspector.process_fits_files(args.fits_files)
	except Exception as error:
		logging.critical('Fatal error: %s' % error)
		raise

	try:
		with open(args.output, 'tw', encoding='UTF-8') as output_file:
			json.dump(keyword_inspector.get_keyword_infos(), output_file, ensure_ascii=False, indent='\t')
	except IOError as error:
		logging.critical('Could not open file %s for writing: %s' % (args.output, error))
		raise
	except Exception as error:
		logging.critical('Fatal error: %s' % error)
		raise
