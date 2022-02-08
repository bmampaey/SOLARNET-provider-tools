#!/usr/bin/env python3
from unittest import TestCase, mock, main
from tempfile import NamedTemporaryFile
from requests import Request

from provider_tools.restful_api import RESTfulApi, ApiKeyAuth, JsonSerializer, DateTimeEncoder

class TestRESTfulApi(TestCase):
	'''Test the RESTfulApi class'''
	def setUp(self):
		super().setUp()
		self.username = 'username@test.com'
		self.api_key = 'my_api_key'

	def test_init(self):
		'''Test the __init__ method'''
		
		msg = 'When the username and api_key are specified, they are used for the auth'
		api = RESTfulApi(username = self.username, api_key = self.api_key)
		self.assertEqual(api._store['session'].auth.username, self.username, msg=msg)
		self.assertEqual(api.api._store['session'].auth.api_key, self.api_key, msg=msg)
		
		msg = 'When an auth_file is specified, it is used for the auth'
		with NamedTemporaryFile(mode='w+t') as auth_file:
			auth_file.write('%s:%s' % (self.username, self.api_key))
			auth_file.flush()
			api = RESTfulApi(auth_file = auth_file.name)
			self.assertEqual(api._store['session'].auth.username, self.username, msg=msg)
			self.assertEqual(api._store['session'].auth.api_key, self.api_key, msg=msg)
		
		msg = 'When both the username and api_key and an auth_file are specified, it raises a ValueError'
		with self.assertRaises(ValueError, msg=msg):
			# Wheter the auth_file exist or not don't matter
			api = RESTfulApi(username = self.username, api_key = self.api_key, auth_file = 'auth_file')
	
		msg = 'When none of the username, api_key or auth_file are specified, the auth is not set'
		api = RESTfulApi()
		self.assertIsNone(api._store['session'].auth, msg=msg)
	
	def test_parse_auth_file(self):
		'''Test the parse_auth_file method'''
		
		msg = 'When a proper auth_file is specified, it return the username and api_key'
		with NamedTemporaryFile(mode='w+t') as auth_file:
			auth_file.write('%s:%s' % (self.username, self.api_key))
			auth_file.flush()
			username, api_key = RESTfulApi.parse_auth_file(auth_file.name)
			self.assertEqual(username, self.username, msg=msg)
			self.assertEqual(api_key, self.api_key, msg=msg)
		
		msg = 'When an improper auth_file is specified, it raises a RuntimeError'
		with NamedTemporaryFile(mode='w+t') as auth_file:
			auth_file.write('improper')
			auth_file.flush()
			with self.assertRaises(RuntimeError, msg=msg):
				username, api_key = RESTfulApi.parse_auth_file(auth_file.name)
		
		msg = 'When a non readable auth_file is specified, it raises a RuntimeError'
		with self.assertRaises(RuntimeError, msg=msg):
			username, api_key = RESTfulApi.parse_auth_file('not a readable file')

class TestApiKeyAuth(TestCase):
	'''Test the ApiKeyAuth class'''
	
	def setUp(self):
		super().setUp()
		self.username = 'username@test.com'
		self.api_key = 'my_api_key'
	
	def test_init(self):
		'''Test the __init__ method'''
		
		msg = '__init__ must save the username and api_key on the instance'
		auth = ApiKeyAuth(self.username, self.api_key)
		self.assertEqual(auth.username, self.username, msg = msg)
		self.assertEqual(auth.api_key, self.api_key, msg = msg)
	
	def test_call(self):
		'''Test the __call__ method'''
		
		msg = '__call__ must set the proper Authorization header on the request'
		auth = ApiKeyAuth(self.username, self.api_key)
		request = Request()
		request = auth(request)
		self.assertIn('Authorization', request.headers, msg = msg)
		self.assertEqual(request.headers['Authorization'], 'ApiKey %s:%s' % (self.username, self.api_key), msg = msg)

if __name__ == '__main__':
	main()
