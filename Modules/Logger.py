import logging
import os
import time

class Logger:
	logFile = ''
	
	
	def __init__(self, logPath=''):
		#Get logPath and logFile name
		if not Logger.logFile:
			Logger.logFile = logPath + str(int(time.time())) + '.log'
			
	''' Function to get a logger '''
	def getMyLogger(self):
		logger = logging.getLogger(__name__)
		logger.setLevel(logging.INFO)
		
		
		# create a file handler
		logFile = os.path.join(os.getcwd(), 'Logs', Logger.logFile)
		handler = logging.FileHandler(logFile)
		handler.setLevel(logging.INFO)
		
		# Reset the logger.handlers if it already exists.
		if logger.handlers:
			logger.handlers = []

		# create a logging format
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		handler.setFormatter(formatter)

		# add the handlers to the logger
		logger.addHandler(handler)

		return logger