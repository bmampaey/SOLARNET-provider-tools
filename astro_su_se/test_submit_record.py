#!/usr/bin/env python3
from unittest import TestCase, mock, main
from pathlib import Path
from submit_record import Record, DATE_KEYWORD

class TestSubmiRecord(TestCase):
	'''Test the submit_record script'''
	
	def setUp(self):
		super().setUp()
		self.test_file = str(Path(__file__).parent / 'test_file.fits')
	
	def test_get_file_path(self):
		'''Test the get_file_path method of Record'''
		
		msg = 'When file_path is specified explicitly, get_file_path must return it'
		file_path = 'test/file.fits'
		record = Record(self.test_file, file_path = file_path)
		self.assertEqual(record.get_file_path(), file_path, msg = msg)
		
		msg = 'When file_path is not specified explicitly, get_file_path must return the relative fits file path'
		record = Record(self.test_file)
		self.assertEqual(record.get_file_path(), self.test_file.lstrip('/'), msg = msg)
	
	def test_get_file_url(self):
		'''Test the get_file_url method of Record'''
		
		msg = 'When file_url is specified explicitly, get_file_url must return it'
		file_url = 'https://test.com/file.fits'
		record = Record(self.test_file, file_url = file_url)
		self.assertEqual(record.get_file_url(), file_url, msg = msg)
		
		msg = 'When file_url is not specified explicitly, and BASE_FILE_URL is not set, get_file_url must raise an exception'
		record = Record(self.test_file)
		with self.assertRaises(ValueError, msg=msg):
			record.get_file_url()
		
		msg = 'When file_url is not specified explicitly, get_file_url must combine the file_path and the BASE_FILE_URL'
		record = Record(self.test_file, file_path = 'test_file.fits')
		with mock.patch('submit_record.BASE_FILE_URL', 'https://test.com/'):
			self.assertEqual(record.get_file_url(), 'https://test.com/test_file.fits', msg = msg)
	
	def test_get_oid(self):
		'''Test the get_oid method of Record'''
		
		msg = 'When oid is specified explicitly, get_oid must return it'
		oid = 'test'
		record = Record(self.test_file, oid = oid)
		self.assertEqual(record.get_oid(), oid, msg = msg)
		
		msg = 'When oid is not specified explicitly, and no fits header is passed, get_oid must raise an exception'
		record = Record(self.test_file)
		with self.assertRaises(ValueError, msg=msg):
			record.get_oid()
		
		msg = 'When oid is not specified explicitly, and metadata without a the date keyword is passed, get_oid must raise an exception'
		record = Record(self.test_file)
		with self.assertRaises(ValueError, msg=msg):
			record.get_oid({'not_' + DATE_KEYWORD: '2000-01-01T00:00:00Z'})
		
		msg = 'When oid is not specified explicitly, and metadata with an invalid date keyword is passed, get_oid must raise an exception'
		record = Record(self.test_file)
		with self.assertRaises(ValueError, msg=msg):
			record.get_oid({DATE_KEYWORD: '2000-01-01T24:00:00Z'})
		
		msg = 'When oid is not specified explicitly, and metadata with a valid date keyword is passed, get_oid must return the date simplified'
		record = Record(self.test_file)
		self.assertEqual(record.get_oid({DATE_KEYWORD: '2000-01-01T00:00:00Z'}), '20000101000000', msg = msg)
	
	def test_get_data_location(self):
		'''Test the get_data_location method of Record'''
		
		msg = 'When a data location with the file url already exists in the SVO API, get_data_location must return the resource_uri'
		file_url = 'https://test.com/file.fits'
		record = Record(self.test_file, file_url = file_url)
		with mock.patch.object(record.api, 'data_location') as resource:
			resource_uri = '/api/svo/data_location/1'
			resource.get.return_value = {'objects': [{'resource_uri': resource_uri}]}
			self.assertEqual(record.get_data_location(), resource_uri, msg = msg)
		
		msg = 'When a data location with the file url does not exist in the SVO API, get_data_location must return a dict with the appropriate info'
		file_url = 'https://test.com/file.fits'
		file_path = 'test/file.fits'
		dataset = 'Test'
		record = Record(self.test_file, file_url = file_url, file_path = file_path, dataset = dataset)
		with mock.patch.multiple(record.api, data_location = mock.DEFAULT, dataset = mock.DEFAULT) as resources:
			resources['data_location'].get.return_value = {'objects': []}
			dataset_resource_uri = '/api/svo/dataset/%s' % dataset
			resources['dataset']().get.return_value = {'resource_uri': dataset_resource_uri}
			self.assertEqual(record.get_data_location(), {
				'dataset': dataset_resource_uri,
				'file_url': file_url,
				'file_size': Path(self.test_file).stat().st_size,
				'file_path': file_path,
				'thumbnail_url': None,
				'offline': False,
			}, msg = msg)
	
	def test_get_metadata(self):
		'''Test the get_metadata method of Record'''
		
		msg = 'get_metadata must return a dict with the appropriate info'
		oid = 'test'
		record = Record(self.test_file, oid = oid)
		with mock.patch.object(record.api, 'keyword') as resource:
			resource.get.return_value = {'objects': [
				{'name': 'date_obs', 'verbose_name': 'DATE-OBS'},
				{'name': 'missing', 'verbose_name': 'MISSING'}
			]}
			metadata = record.get_metadata()
			self.assertEqual(metadata['oid'], oid, msg = msg)
			self.assertEqual(metadata['date_obs'], '2000-01-01T00:00:00Z', msg = msg)
			self.assertIn('fits_header', metadata, msg = msg)
			self.assertNotIn('missing', metadata, msg = msg)

if __name__ == '__main__':
	main()
