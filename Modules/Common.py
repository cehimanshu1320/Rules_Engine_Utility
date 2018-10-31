import ConfigParser
from Logger import Logger
from DB2Manager import DB2Manager
from SybaseDBManager import SybaseDBManager
import os

class Common:
	''' Constructor '''
	def __init__(self):
		self.logger = Logger().getMyLogger()
	
	''' Function to read and get configuration for selected section and param '''
	def getConfiguration(self, confFile, section=None, param = None):
		try:
			self.logger.info('Reading configuration file: %s',confFile)
			#config = self.getConfigPath(confFile)  #It can be used if config file is given inside config directory
			config = ConfigParser.ConfigParser()
			if os.path.isfile(confFile):
				config.read(confFile)
			else:
				self.logger.error('Configuration file does not exist: %s',confFile)
				return
			if section:
				sectionDict = {}
				try:
					options = config.options(section)
					for option in options:
						sectionDict[option] = config.get(section, option)
						if sectionDict[option] == -1:
							print 'Skip option ',option
							self.logger.warning('Key %s is not found in configuration section %s', option, section)
				except Exception as e:
					print 'Error while reading config section: ',e
					self.logger.error('Error while reading config section: %s',e)
					return False
				if param:
					return sectionDict[param.toLower()]
				return sectionDict
			else:
				return config
		except Exception as e:
			self.logger.error('Error in reading configuration file: %s',e)
			return False

	''' Function to get db name from db configuration '''
	def getDBName(self):
		dbName = ''
		try:
			self.logger.info('Getting db name from db configuration')
			confFile = self.getConfigPath('Config.ini')
			dbConfig = self.getConfiguration(confFile, 'DB')
			dbName = dbConfig['database']
			self.logger.info('DB name is : %s', dbName)
		except Exception as e:
			self.logger.error('Error in getting db name from configuration : %s',e)
		return dbName

		
	''' Function to get path of Config location '''
	def getConfigPath(self, fileName):
		configPath = os.path.join(os.getcwd(), 'Config', fileName)
		self.logger.info('Reading configuration file : %s',configPath)
		return configPath

	''' Function to fetch records from db '''
	def getDBRecords(self, dbType, dbName, query, server=None):
		result = ''
		try:
			db = None
			if dbType.lower() == 'db2':
				db = DB2Manager()
			elif dbType.lower() == 'sybase':
				db = SybaseDBManager()
			if server:
				result = db.get(server,dbName, query)
			else:
				result = db.get(dbName, query)
		except Exception as e:
			self.logger.error('Error while fetching data from database : %s' , e)
		return result
		
	''' Function to execute db query '''
	def executeQuery(self, dbType, dbName, query, server=None):
		self.logger.info('Inside executeQuery')
		result = ''
		try:
			db = None
			if dbType.lower() == 'db2':
				db = DB2Manager()
			elif dbType.lower() == 'sybase':
				db = SybaseDBManager()
			if server:
				db.execute(server, dbName, query)
			else:
				db.execute(dbName, query)
		except Exception as e:
			self.logger.error('Error while fetching data from database : %s' , e)
		#return result