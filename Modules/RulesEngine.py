from Modules.Logger import Logger
from Modules.Common import Common
import Constants
import os
import re
import thread
import re
import time

class RulesEngine:
	''' Constructor '''
	def __init__(self):
		#self.logger = Logger().getMyLogger()
		#self.common = Common()
		self.config = {}
		self.result_Pass = {'Status':'Success','Message':'Execution Completed Succesfully','Details':''}
		self.result_Fail = {'Status':'Fail','Message':'Execution Failed', 'Error': ''}
		self.result_Warning = {'Status':'Completed with Warnings', 'Message':'Execution Completed with Warnings', 'Warning':''}
		self.unmatchedRecords = []

	''' Function to run rules engine step by step '''
	def startRulesEngine(self, configFile, batchName, overwrite):
		self.logger = Logger().getMyLogger()
		self.common = Common()
		self.config = self.common.getConfiguration(configFile)	# Get configuration from config file
		self.logger.info('Configuration file is: %s',configFile)
		self.logger.info('Batch name is: %s',batchName)	 
		self.logger.info('Overwrite option is: %s',overwrite)
		if not self.config:
			self.logger.error('Invalid configuration file: %s',configFile)
			self.result_Fail['Error'] = 'Invalid configuration file: %s'%configFile
			return self.result_Fail
		rules = self.getRules()								# Get rules data
		if not rules:
			self.logger.error('No rules found for app %s',self.config.get('RULES','APP_NAME'))
			self.result_Fail['Error'] = 'No rules found for app %s'%self.config.get('RULES','APP_NAME')
			return self.result_Fail
		if not batchName:
			batchName = self.config.get('BATCH','BATCHNAME')
		rules = self.sortList(rules, Constants.COLUMN_PRIORITY)
		result = self.applyRules(batchName, rules, overwrite)
		return result
	
	''' Function to fetch the input records '''
	def getInputData(self, query, overwrite='no'):
		result = ''
		try:
			self.logger.info('Fetching input records')
			dbType = self.config.get('RECORDS', 'DBTYPE')
			recordsDB = self.config.get('RECORDS', 'DATABASE')
			self.logger.info('Database for input records is %s',recordsDB)
			if(overwrite.lower() == 'no'):
				stamplingField = self.config.get('RECORDS', 'STAMPING_FIELD')
				defaultRule = self.config.get('RULES', 'DEFAULT_RULE')
				whereClause = " and ("+ stamplingField + " = '" + defaultRule +"' or "+stamplingField+"='' or " +stamplingField+ " is null)"
				query = query + whereClause
			if dbType.lower() == 'sybase':
				svr = self.config.get('RECORDS', 'SERVER')
				result = self.common.getDBRecords(dbType, recordsDB, query, server=svr)
			else:
				result = self.common.getDBRecords(dbType, recordsDB, query)
		except Exception as e:
			self.logger.error('Error in getting input data : %s',e)
			self.result_Fail['Error'] = 'Error in getting input data : %s'%e
		return result
		
	''' Function to get rules from database '''
	def getRules(self):
		result = ''
		try:
			self.logger.info('Fetching rules data')
			dbType = self.config.get('RULES', 'DBTYPE')
			rulesDB = self.config.get('RULES', 'DATABASE')
			self.logger.info('Database for rules is %s',rulesDB)			
			rulesTable = self.config.get('RULES','TABLE_NAME')
			self.logger.info('Table name for rules %s', rulesTable)
			appName = self.config.get('RULES','APP_NAME')
			self.logger.info('App name for fetching rules: %s',appName)			
			queryParams = (rulesTable, appName)
			query = Constants.GET_RULES_QUERY
			query = query%queryParams
			if dbType.lower() == 'sybase':
				svr = self.config.get('RULES', 'SERVER')
				result = self.common.getDBRecords(dbType, rulesDB, query, server=svr)
			else:
				result = self.common.getDBRecords(dbType, rulesDB, query)			
		except Exception as e:
			self.logger.error('Error in getting input data : %s',e)
			self.result_Fail['Error'] = 'Error in getting input data : %s'%e
		return result				

	''' Function to apply rules on error records '''
	def applyRules(self, batchName, rules, overwrite):
		self.logger.info('Applying rules')
		records = []
		result = {}
		for rule in rules:
			self.logger.info('\n************************************\n')
			self.logger.info('RULE ID = %s',rule['RULE_ID'])
			filterCondition = rule[Constants.COLUMN_FILTER_CONDITION]
			
			# Taken common code out for adding batchname condition
			recordsTable = self.config.get('RECORDS','TABLE_NAME')
			self.logger.info('Table name for input records is %s', recordsTable)		
			queryParams = (recordsTable, batchName)
			query = Constants.GET_INPUT_RECORDS_QUERY
			query = query%queryParams
			
			if filterCondition:
				querySplit = query.split('where ')
				if len(querySplit) > 1 :
					whereCondition = querySplit[1]
					query = filterCondition + ' and ' + whereCondition
			records = self.getInputData(query, overwrite)
			self.logger.info('\nRECORD COUNT = %s',len(records))
			for record in records:
				self.logger.info('\n**********\nCHECKING FOR RECORD ID = %s',record['RECORD_ID'])
				self.checkRulesForRecord(record, rule)
		message = 'Validated %d rules for batch %s'%(len(rules),batchName)
		if self.unmatchedRecords:
			self.result_Warning['Message'] = message
			self.result_Warning['Warning'] = 'No rule matches for records: %s'%str(self.unmatchedRecords)
			self.logger.error('No rule matches for records %s',str(self.unmatchedRecords))
			return self.result_Warning
		self.logger.info(message)
		self.result_Pass['Details'] = message
		return self.result_Pass	

	''' Function to get list of primary keys for records '''
	def getPrimaryKeys(self):
		pkColumns = []
		try:
			inputTablePK = self.config.get('RECORDS', 'INPUT_TABLE_PK')
			if inputTablePK.find(','):
				pkColumns = inputTablePK.split(',')
			else:
				pkColumns.append(inputTablePK)
		except Exception as e:
			self.logger.error('Error in getting primary key for input records: %s',e)
			self.result_Fail['Error'] = 'Error in getting primary key for input records: %s'%e
		return pkColumns
	
	''' Function to apply all rule on the record '''
	def checkRulesForRecord(self, record, rule):
		self.logger.info('Thread started for record: %s',record)
		try:
			finalResult = {}
			result = False			
			expectedValue = ''
			inputTablePK = self.config.get('RECORDS', 'INPUT_TABLE_PK')
			pkColumns = self.getPrimaryKeys()
			keyDict = {}					
			for columnName in pkColumns:
				keyDict[columnName] = record[columnName]
			identifier = tuple(keyDict.values())			
			source = rule[Constants.COLUMN_SOURCE]
			regex = rule[Constants.COLUMN_REGEX]
			assertionField = rule[Constants.COLUMN_ASSERTION_FIELD]
			self.logger.info('Soruce is %s',source)
			self.logger.info('Regex is %s',regex)
			actualValue = self.getActualValue(record, assertionField)
			self.logger.info('Actual value: %s',actualValue)				
			if source.lower() == 'db':
				expectedValue = self.getExpectedValueForSourceDB(rule, record)
			elif source.lower() == 'fixed':
				expectedValue = self.getExpectedValueForSourceFixed(rule)				
			else:
				self.logger.info('Source type %s is invalid source type.',source)
			self.logger.info('Expected value: %s',expectedValue)				
			if actualValue and expectedValue:
				result = self.compareValues(actualValue, expectedValue, regex)
			self.logger.info('Comparision Result: %s',str(result))
			if result:
				temp = {}
				if identifier in finalResult.keys():
					values = finalResult[identifier]['failuareCode']
					values = values + ' , ' + rule[Constants.COLUMN_FAILURE_POINT_BUCKET]
					temp['failuareCode'] = values
					temp['whereCondition'] = keyDict
					finalResult[identifier] = temp
				else:
					temp['failuareCode'] = rule[Constants.COLUMN_FAILURE_POINT_BUCKET]
					temp['whereCondition'] = keyDict
					finalResult[identifier] = temp
			else:
				if record[Constants.COLUMN_FAILURE_POINT_BUCKET] == self.config.get('RULES', 'DEFAULT_RULE'):
					return
				pkColumns = self.getPrimaryKeys()
				keyDict = {}
				for columnName in pkColumns:
					keyDict[columnName] = record[columnName]
				self.unmatchedRecords.append(keyDict)
				identifier = tuple(keyDict.values())	
				temp = {}
				temp['failuareCode'] = self.config.get('RULES', 'DEFAULT_RULE')
				temp['whereCondition'] = keyDict
				finalResult[identifier] = temp					
			self.updateResults(finalResult)	
		except Exception as e:
			self.logger.error('Error while checking rules: %s',e)
			self.result_Fail['Error'] = 'Error while checking rules: %s'%e

	''' Function to get actual value for comparison from record information '''
	def getActualValue(self, record, assertionField):
		actualValue = record[assertionField]
		return actualValue
	
	''' Function to get expected value for comparison when source is db '''
	def getExpectedValueForSourceDB(self, rule, record):
		try:
			result = ''
			command = rule[Constants.COLUMN_COMMAND]	# Get the command from database
			cmdList = command.split('|')
			db = cmdList[0]
			dbType = self.config.get(db, 'DBTYPE')
			dbName = self.config.get(db, 'DATABASE')
			query = cmdList[1]
			params = rule[Constants.COLUMN_PARAMETERS]
			paramValues = []
			paramList = []
			if params.find(','):
				paramList = params.split(',')
			else:
				paramList.append(params)
			if len(paramList) > 0:
				for param in paramList:
					param = (str(param)).strip()
					paramValues.append(record[param])
				queryParams = tuple(paramValues)
				query = query%queryParams
			if dbType.lower() == 'sybase':
				svr = self.config.get(db, 'SERVER')
				result = self.common.getDBRecords(dbType, dbName, query, server=svr)
			else:
				result = self.common.getDBRecords(dbType, dbName, query)			
			if result:
				return str(result[0].values()[0])
		except Exception as e:
			self.logger.error('Error while getting expected value when source is db: %s',e)
			self.result_Fail['Error'] = 'Error while getting expected value when source is db: %s'%e
		return ''
		
	''' Function to get expected value for comparison when source is fixed '''
	def getExpectedValueForSourceFixed(self, rule):
		try:
			command = rule[Constants.COLUMN_COMMAND]	# Get the command from database
			self.logger.info('Command: %s', command)
			return command
		except Exception as e:
			self.logger.error('Error while getting expected value when source is fixed: %s',e)
			self.result_Fail['Error'] = 'Error while getting expected value when source is fixed: %s'%e
		return ''
		
	
	''' Function to compare two values using regex '''
	def compareValues(self, actualValue, expectedValue, regex):
		try:
			checkActualValue = re.match(regex, actualValue, re.M | re.I)
			checkExpectedValue = re.match(regex, expectedValue, re.M | re.I)
			if checkActualValue and checkExpectedValue:
				return True
		except Exception as e:
			self.logger.error('Error while comparing actual and expected values: %s',e)
			self.result_Fail['Error'] = 'Error while comparing actual and expected values: %s'%e
		return False
	
	''' Function to update rule engine output to db ''' 
	def updateResults(self, finalResult):
		self.logger.info('Updating rule engine output to database')
		dbType = self.config.get('RECORDS', 'DBTYPE')
		recordsDB = self.config.get('RECORDS', 'DATABASE')
		recordsTable = self.config.get('RECORDS','TABLE_NAME')
		stampingField = self.config.get('RECORDS','STAMPING_FIELD')
		try:
			for key, value in finalResult.iteritems():
				whereCondition = self.prepareWhereCondition(value['whereCondition'])
				query = "update "+ recordsTable +" set "+ stampingField +" = '"+value['failuareCode']+"' where " + str(whereCondition)
				#print query
				if dbType.lower() == 'sybase':
					svr = self.config.get('RECORDS', 'SERVER')
					self.common.executeQuery(dbType, recordsDB, query, server=svr)
				else:
					self.common.executeQuery(dbType, recordsDB, query)
		except Exception as e:
			self.logger.error('Error while updating final result to db : %s',e)	
			self.result_Fail['Error'] = 'Error while updating final result to db : %s'%e

	def prepareWhereCondition(self, conditionDict):
		whereCondition = ''
		for key, value in conditionDict.iteritems():
			if isinstance(value, str) or isinstance(value, unicode):
				whereCondition = whereCondition + key + " = '" + value + "'"
			else:
				whereCondition = whereCondition + key + " = " + str(value)
			whereCondition = whereCondition + ' and '
		whereCondition = whereCondition[:-4]
		#print whereCondition
		return whereCondition
		
	def sortList(self, inputList, sortBy):
		return sorted(inputList, key=lambda k: k[sortBy])
