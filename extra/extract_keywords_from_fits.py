#!/usr/bin/env python3
import os
import re
import json
import pickle
import logging
import argparse
from datetime import datetime
from collections import defaultdict, Counter
from astropy.io import fits
from dateutil.parser import parse as parse_date


class KeywordInspector:
	"""Given a list of FITS file paths or URLs, inspect the keywords and output the information needed for the SVO database"""

	# Default keywords to exclude
	DEFAULT_EXCLUDE_KEYWORDS = ['DATASUM', 'CHECKSUM', 'SIMPLE', 'BITPIX', 'COMMENT', 'HISTORY']

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

	def __init__(self, fits_files, hdu, exclude_keywords=[], backup_file_path=None, force_interactive=False):
		self.fits_files = fits_files
		self.hdu = hdu
		self.exclude_keywords = self.DEFAULT_EXCLUDE_KEYWORDS + [keyword.upper() for keyword in exclude_keywords]
		self.backup_file_path = backup_file_path

		# Count all the possible values and comments for each keyword
		self.keyword_values = defaultdict(Counter)
		self.keyword_comments = defaultdict(Counter)
		self.processed_fits_files = []

		self.log = logging.getLogger('keyword inspector')
		self.force_interactive = force_interactive

	def process_fits_files(self):
		"""Inspect all the FITS files and extract the keywords, their value and comment"""

		self.restore_backup()

		for fits_file in self.fits_files:
			if fits_file in self.processed_fits_files:
				self.log.info('File %s was already processed. Skipping !', fits_file)
			else:
				self.log.info('Processing file %s', fits_file)
				try:
					with fits.open(fits_file, cache=False, lazy_load_hdus=True) as hdu_list:
						try:
							header = hdu_list[self.hdu].header
						except IndexError as error:
							self.log.error('File %s does not have HDU %s . Skipping!', fits_file, hdu)
						else:
							self.inspect_header(header)
							self.processed_fits_files.append(fits_file)
							self.save_backup()
				except OSError as error:
					self.log.error('Could not open file %s: %s . Skipping!', fits_file, error)

	def inspect_header(self, header):
		"""Inspect a FITS header and add the keywords, their value and comment"""

		keyword_infos = list()
		comment_count = 0
		history_count = 0

		for card in header.cards:
			try:
				keyword = card.keyword
				value = card.value
				comment = card.comment
			except Exception as error:
				self.log.error('Could not parse card %s: %s . Skipping!', card, error)
				continue

			if keyword.upper() in self.exclude_keywords:
				self.log.debug('Keyword %s in exclude_keywords. Skipping!', keyword)
				continue

			else:
				self.keyword_values[keyword][value] += 1
				self.keyword_comments[keyword][comment] += 1

	def get_keyword_infos(self):
		"""Return a list of all keyword info {name, verbose_name, type, unit, description} resolving ambiguities by aking the user"""

		keyword_infos = list()

		for keyword, values in self.keyword_values.items():
			keyword_type = self.resolve_keyword_type(keyword)
			keyword_unit, keyword_description = self.resolve_keyword_unit_description(keyword)

			keyword_infos.append(
				{
					'name': self.get_keyword_name(keyword),
					'verbose_name': keyword,
					'type': keyword_type,
					'unit': keyword_unit,
					'description': keyword_description,
				}
			)

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

	def get_keyword_name(self, keyword):
		"""Convert a keyword into a SVO compliant keyword name"""

		# Convert the keyword to lower case
		name = keyword.strip().lower()
		# replace any unusual name character by an underscore
		name = re.sub(r'[^a-zA-Z0-9_]', r'_', name)
		# remove consecutive underscores
		while name.find(r'__') >= 0:
			name = new_name.replace(r'__', r'_')
		# remove underscores at extremities
		name = name.strip('_')

		return name

	def save_backup(self):
		"""Save the state of the keyword inspector from a backup file"""

		# Pickle the processed_fits_files, keyword_values and keyword_comments to the backup file
		if self.backup_file_path:
			self.log.debug('Saving state to backup file file %s', self.backup_file_path)
			try:
				with open(self.backup_file_path, 'wb') as file:
					pickle.dump(self.processed_fits_files, file)
					pickle.dump(self.keyword_values, file)
					pickle.dump(self.keyword_comments, file)
			except IOError as error:
				self.log.error('Could not open backup file %s for writing: %s', self.backup_file_path, error)
				raise
			except Exception as error:
				self.log.error('Could not save state to backup file: %s', error)
				raise

	def restore_backup(self):
		"""Restore the state of the keyword inspector from a backup file"""

		# Un-pickle the processed_fits_files, keyword_values and keyword_comments from the backup file
		if self.backup_file_path and os.path.isfile(self.backup_file_path):
			try:
				with open(self.backup_file_path, 'rb') as file:
					self.processed_fits_files = pickle.load(file)
					self.keyword_values = pickle.load(file)
					self.keyword_comments = pickle.load(file)
			except IOError as error:
				self.log.error('Could not open backup file %s for reading: %s', self.backup_file_path, error)
				raise
			except Exception as error:
				self.log.error('Could not restore state from backup file: %s', error)
				raise


if __name__ == '__main__':
	# Get the arguments
	parser = argparse.ArgumentParser(
		description='Inspect the header of one or more FITS files and extract the keywords definitions for the SOLARNET Virtual Observatory. If possible try to use files spread over the entire dataset, instead of consecutive files, this will allow to detect changes over time in the header of the files.'
	)
	parser.add_argument('fits_files', metavar='FITSFILE', nargs='+', help='Path or URL to a FITS file to inspect')
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
		'--exclude',
		'-E',
		metavar='KEYWORD',
		default=[],
		action='append',
		help='Keywords to exclude, can be specified multiple times',
	)
	parser.add_argument(
		'--backup',
		'-b',
		metavar='BACKUPFILE',
		help='Path to a backup file where the progress will be saved so that in case of an error, it is possible to start where it had failed',
	)
	parser.add_argument(
		'--force-interactive',
		'-i',
		action='store_true',
		help='Always ask the user for each keyword even if there is no ambiguity',
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

	# The HDU can be specified as int or name, therefore we must convert it
	if args.hdu.isdecimal():
		hdu = int(args.hdu)
	else:
		hdu = args.hdu

	# Process the fits files and write the keyword info to the output file
	keyword_inspector = KeywordInspector(
		args.fits_files,
		hdu=hdu,
		exclude_keywords=args.exclude,
		backup_file_path=args.backup,
		force_interactive=args.force_interactive,
	)

	try:
		keyword_inspector.process_fits_files()
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
