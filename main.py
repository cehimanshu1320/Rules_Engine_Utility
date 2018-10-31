#!/ms/dist/python/PROJ/core/2.7.3/bin/python 
import os,sys
import getopt
from Modules.Logger import Logger
from Modules.RulesEngine import RulesEngine
import time

if __name__ == '__main__':
	startTime = time.time()
	# Get logger:
	#logger = Logger().getMyLogger()
	print'Started execution'
	# Get command line arguments:
	configFile = ''
	batchName = ''
	overwrite = ''
	logPath = ''
	try:
		argv = sys.argv[1:]
		opts, args = getopt.getopt(argv,"c:b:o:l:",["config=","batch=","overwrite=","logPath="])
	except getopt.GetoptError:
		print"error..."
		print 'Error in getting command line parameters'
		#logger.error('Error in getting command line parameters')
		exit()
	print"args==>",args
	print"opts==>",opts
	if len(argv) < 2:
		print "Args count less"
		print 'Please provide: -c <config file path>'
		print 'Error: Insufficient command line arguments..aborting execution'
		#logger.error('Insufficient command line arguments..aborting execution')
		exit()
	print"opts===>",opts			
	for opt, arg in opts:
		print"opt==>",opt				
		if opt in ("-c", "--config"):
			print"got config==>",arg
			configFile = arg
		elif opt in ("-b", "--batch"):
			batchName = arg
		elif opt in ("-o", "--overwrite"):
			overwrite = arg
		elif opt in ("-l", "--logPath"):
			logPath = arg
		else:
			print"Yes..1"
			print 'Please provide: -c <config file path> -b batch name'
			print 'Error: Invalid command line arguments..aborting execution'
			#logger.error('Invalid command line arguments..aborting execution')
			exit()
	#logger.info('Configuration file is: %s',configFile)
	#logger.info('Batch name is: %s',batchName)	
	#logger.info('Overwrite option is: %s',overwrite)
	if not overwrite:
		overwrite = 'no'
	# Start Rules Engine:
	rulesEngine = RulesEngine()
	logger = Logger(logPath).getMyLogger()
	print"Config file in main.py == >",configFile
	print"Batch name in main.py == >",batchName
	result	= rulesEngine.startRulesEngine(configFile, batchName, overwrite)
	print '*****************************************************************'
	logger.info('Execution Completed')
	print 'Execution Completed'
	print 'Execution Status as below:'
	logger.info('Execution Status as below:')
	for key,value in result.iteritems():		
		logger.info('%s : %s',key,value)
		print key + " : " + value
	endTime = time.time()
	executionTime = endTime - startTime
	logger.info('Execution Time: %s seconds.',executionTime)
	print 'Execution Time: %s seconds.'%executionTime
	print '*****************************************************************'

	
