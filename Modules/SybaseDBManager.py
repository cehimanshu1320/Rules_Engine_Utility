#!/ms/dist/python/PROJ/core/2.7.3/bin/python 
import ms.version
ms.version.addpkg("sybase", "0.39-ms1-15.0.0.17")
ms.version.addpkg("ms.netkrb", "1.1.0")
ms.version.addpkg("kerberos", "1.1-ms5")
import Sybase

import sys, time, os, re
import ms.db2 
from Logger import Logger
from Singleton import Singleton

@Singleton
class SybaseDBManager():
	''' Constructor '''
	def __init__(self):
		self.logger = Logger().getMyLogger()	
		self.connectionPool = {}
	
	''' Function to get connection object '''
	def getConnection(self, server):
		try:
			self.logger.info('Getting connection for Sybase db %s',server)
			if not self.connectionPool.has_key(server):				
				self.connectionPool[server] = self.connect(server)
			elif self.connectionPool[server] is None:
				self.connectionPool[server] = self.connect(server)
			#print 'DB Connection: ',self.connectionPool[server]
			return self.connectionPool[server]
		except Exception as e:
			self.logger.error('Error in getting db connection %s',e)
			print 'Error in getConnection ',e			
	
	'''Function to connect to database '''
	def connect(self, server):
		try:	
			svr = None
			principal = None
			SERVER = server
			for line in open('//ms/dist/syb/dba/files/sgp.dat', "r"):	
				svr, principal = line.split(' ')
				if svr == SERVER:
					break
			principal = principal.strip()
			db = Sybase.connect(SERVER, '', '', delay_connect=1, datetime="auto", auto_commit = 1)
			db.set_property(Sybase.CS_SEC_NETWORKAUTH, Sybase.CS_TRUE)
			db.set_property(Sybase.CS_SEC_SERVERPRINCIPAL, principal)
			db.connect()
			dbCursor = db.cursor()
			return dbCursor
		except Exception as e:
			self.logger.error('Error in database connection %s',e)
			print 'Error in database connection ',e	
			
		
	''' Function for GET (select) operation '''
	def get(self, server, dbName, query):	
		result = ''
		self.logger.info('Working on query : %s', query)
		try:
			cursor = self.getConnection(server)
			cursor.execute("use " + dbName)
			cursor.execute(query)
			result = self.formatResult(cursor)			
		except Exception as e:
			self.logger.error('Error in query execution : %s', e)
			print 'Error in query execution : ', e
		return result
		
	''' Function for execute operation '''
	def execute(self, server, dbName, query):
		result = ''
		self.logger.info('Executing query : %s', query)
		try:
			cursor = self.getConnection(server)	
			cursor.execute("use " + dbName)
			cursor.execute(query)
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