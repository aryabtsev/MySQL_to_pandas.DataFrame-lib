# -*- coding: utf-8 -*-
# Lib for tranform MySQL response to pandas DataFrame

import pymysql
import os
from yaml import load
import pandas as pd
import sys
import functools
import time

curr_path = os.path.dirname( os.path.abspath(__file__) )

connection_try = 0

def db_error_handler_decorator(func):
	"""
	A decorator that processes errors while working with the MySQL database.
	If the data given is incorrect (login / password, table, invalid request), an error message will be returned.
	If it is impossible to connect (there is no connection or the login / password is incorrect) tries to reconnect twice
	delay of 5 seconds and in case of failure leaves
	"""
	@functools.wraps(func)
	def wrapper(*args, **kwargs):

		global connection_try
		try:
			return func(*args, **kwargs)
		except Exception as e:
			if e[0] == 1044:
				print ("\n\nInvalid Table name\n\n")
				print e
				sys.exit()

			elif e[0] == 1045:
				print ('\n\nUnable to log in - wrong username or password\n\n')
				print (e)
				sys.exit()

			elif e[0] == 2003:
				print ('\n\nCan not connect to the database, the address / port may be incorrectly specified, or there is no network connection\n\n')
				while connection_try < 3:
					print ('trying to reconnect {}...\n\n'.format(connection_try+1))
					time.sleep(5)

					try:
						return func(*args, **kwargs)
					except: 
						connection_try += 1
						continue
				print (e)
				sys.exit()

			elif e[0] == 1054:
				print ('\n\nInvalid SQL query. The required columns are not present in the table \n\n')
				print (e)
				sys.exit()
	return wrapper


class MySQLWorker:
	"""
	Quickstart:
	
	-- First you need to import this file

		import mysqlworker

	-- Then initialize the object encapsulating all the settings necessary to communicate with the database. Settings must be
	-- stored in the file config.yaml and located in the folder with the executable file. Otherwise, an additional
	-- pass the path to this file as a string using the configPath argument.

		obj = mysqlworker.MySQLWorker(key = 'user', db = 'test')	

	-- To get the required DataFrame, you need to call the sql_request_to_pandas method and pass the string as an argument,
	-- contains the SQL query

		df = obj.sql_request_to_pandas('SELECT * from test.test_table as t')


	_____________________________________________________________________________________________

	The structure of config.yaml should look like:

	mysql:
  	  default:
        dbuser: 'user1'
        dbpass: '123455'
        dbhost: 312.2221.38.1
        dbport: 3306
      anotherkey:
        dbuser: 'user2'
        dbpass: '0120012'
        dbhost: 821.256.129.34
        dbport: 3306
 


	"""
	@db_error_handler_decorator
	def __init__(self, key = 'default', db = 'test', configPath = '' ):

		"""
		A class that implements a connection to the mysql database
		Arguments needed for initialization:

		-- key - the key in the config file, which determines which settings (login, database, etc.) to use.
		-- db - the name of the database to which we will refer to the MySQL server. ($ use db)
		-- configPath - the path to the file config.yaml in which the MySQL user settings are stored
		
		"""
		global connection_try

		if configPath == '':
			configPath = curr_path
		
		try:
			config = load( file( os.path.join( configPath, 'config.yaml' ) ) )
		except: 
			print ("There's no config file at {}".format(os.path.join( configPath, 'config.yaml' )))
			sys.exit()

		try:
			self.user = config['mysql'][key]['dbuser']
			self.password = config['mysql'][key]['dbpass']

		except KeyError: 
			print ("There's no key = {} in config file".format(key))



		

		self.connection = pymysql.connect(host = config['mysql'][key]['dbhost'], port = config['mysql'][key]['dbport'], 
			user = self.user, passwd = self.password, database = db)
		connection_try = 0
		print ('\n\ninitialization, connection to the database was successful\n\n')
		self.cursor = self.connection.cursor()


		def semicolon_check(self, query, variate):

		'''
		The function to check for the presence or absence of a semicolon in the sql query
		Options:
		'query' is a string object representing the SQL query string
		'variate' is a boolean parameter. True - when we need a comma in this request,
		False - if not necessary.
		'''

		if variate == False:
			if query[-1] == ';':
				query = query[0,-1]
			else: pass
	
		if variate == True:
			if query[-1] == ';':
				pass
			else: query = query + ';'
		return query


	@db_error_handler_decorator
	def sql_request_to_pandas(self, query, headers = ''):
	
		"""

		This method allows you to convert a SQL query to a pandas dataframe
		The query argument is passed a string containing the SQL query.
		The function returns the object pandas.DataFrame
		If you want to get a frame across all the columns in the table, you do not need to pass the headers argument.
		In the case of a custom query, you must pass a list of string values to the headers
		columns of the final DataFrame in the order they are mentioned in the SQL query. For example:

		obj.sql_request_to_pandas(query = 'SELECT device, pageviews from test.test_table as t', headers = ['device', 'pageviews'])

		"""
		global connection_try

		query = self.semicolon_check(query = query, variate = True)
	
		with self.connection:
			cursor = self.cursor
			cursor.execute(query)
			if headers == '':
				headers = []
				for names in cursor.description:
					headers.append(names[0])
			else: pass
			connection_try = 0
			received_data = []
			for result in cursor:
				received_data.append(result)
			frame = pd.DataFrame.from_records(received_data, columns = headers)
			return frame
		self.close()


	@db_error_handler_decorator
	def get_db_size(self):
	
		"""

		A method that allows you to know the size of tables in the database in megabytes.
		Returns the object pandas.DataFrame

		"""
		global connection_try

		with self.connection:
			cursor = self.cursor
			cursor.execute('SELECT table_schema "database_name", sum( data_length + index_length )/1024/1024 "Data Base Size in MB" FROM information_schema.TABLES GROUP BY table_schema;')
			headers = []
			for names in cursor.description:
				headers.append(names[0])
			received_data = []
			for result in cursor:
				received_data.append(result)
			frame = pd.DataFrame.from_records(received_data, columns = headers)
			return frame
		connection_try = 0
		self.close()


	@db_error_handler_decorator
	def get_query_length_size(self, query):
		
		"""

		A method that allows you to estimate the number of rows of data returned by a query.
		Options:

		query - SQL query, for which you need to calculate the number of rows

		Example:

		obj.get_query_length_size(query = 'SELECT device, SUM(sessions) FROM test.test_table GROUP BY 1')


		"""
		
		global connection_try

		query = self.semicolon_check(query = query, variate = False)

		with self.connection:
			cursor = self.cursor
			request = str('SELECT COUNT(*) from ({}) t;'.format(query))
			cursor.execute(request)
			(number_of_rows,)=cursor.fetchone()
			return number_of_rows
		connection_try = 0
		self.close()

	
	@db_error_handler_decorator
	def close(self):
        
		"""

		Method for closing the current connection
		
		"""
		self.connection.close()

			

