#!/ms/dist/python/PROJ/core/2.7.3/bin/python  
import ms.version

ms.version.addpkg('python', 'ng-trunk', meta='ets')
ms.version.addpkg("ibm_db","2.0.4-9.5.5")
#ms.version.addpkg("ibm_db","2.0.4-9.7.4")
ms.version.addpkg("ms.db2","1.0.3")
ms.version.addpkg("ms.modulecmd", "1.0.4")
import ms.modulecmd
ms.modulecmd.load("ibmdb2/client/9.5.5") # load the module

import sys, time, os, re
import ms.db2 
from Logger import Logger
from Singleton import Singleton

@Singleton
class DB2Manager:
	''' Constructor '''
	def __init__(self):
		self.logger = Logger().getMyLogger()	
		self.connectionPool = {}
	
	''' Function to get connection object '''
	def getConnection(self, dbName):
		try:
			self.logger.info('Getting connection for db %s',dbName)
			if not self.connectionPool.has_key(dbName):				
				self.connectionPool[dbName] = self.connect(dbName)
			elif self.connectionPool[dbName] is None:
				self.connectionPool[dbName] = self.connect(dbName)
			#print 'DB Connection: ',self.connectionPool[dbName]
			return self.connectionPool[dbName]
		except Exception as e:
			self.logger.error('Error in getting db connection %s',e)
			print 'Error in getConnection ',e			
	
	'''Function to connect to database '''
	def connect(self, dbname):
		try:	
			self.logger.info('Connecting to database')
			connection = ms.db2.connect(dbname)
			connection.set_autocommit(True)
			#connection.autocommit = True
			return connection
		except Exception as e:
			self.logger.error('Error in database connection %s',e)
			print 'Error in database connection ',e
		
	''' Function for GET (select) operation '''
	def get(self, dbName, query):	
		result = ''
		self.logger.info('Working on query : %s', query)
		try:
			connection = self.getConnection(dbName)
			cursor = connection.cursor()
			cursor.execute(query)
			result = self.formatResult(cursor)			
		except Exception as e:
			self.logger.error('Error in query execution : %s', e)
			print 'Error in query execution : ', e
		return result
		
	''' Function for execute operation '''
	def execute(self, dbName, query):
		result = ''
		self.logger.info('Executing query : %s', query)
		try:
			connection = self.getConnection(dbName)
			cursor = connection.cursor()		
			cursor.execute(query)
			#connection.commit()
			#result = self.cursor.fetchall()
		except Exception as e:
			self.logger.error('Error in query execution : %s', e)
			print 'Error in query execution : ', e
		#return result		
		
	''' Function to format result : It returns list of dictionaries '''
	def formatResult(self, cursor):
		result = []
		try:
			numCols = len(cursor.description)
			colnames = [cursor.description[i][0] for i in range(numCols)]
			for row in cursor.fetchall():
				res = {}
				for i in range(numCols):
					res[colnames[i]] = row[i]
				result.append(res)
		except Exception as e:
			self.logger.error('Error in formating database records : %s', e)
		#self.logger.info('Query result: %s',str(result))
		return result
		
	''' Destructor 
	def __del__(self):
		try:
			self.logger.info('Disconnecting database')
			self.cursor.close()
			self.connection.close()
			self.logger.info('Database disconnected')
		except Exception as e:
			self.logger.error('Error in db disconnect : %s',e)
			print 'Error in db disconnect: ',e
	'''	