Rule Assertion Engine:
Language: 
Python 2.7
Database: 
DB2
Directory Structure:
custody_nucleus
	|__________  readme.txt
	|__________  main.py
	|__________  Config
	|	   	   |__________  Config.ini
|__________  Docs
|		   |__________  Code_Description.docx
	|__________  Logs
	|		   |__________  Main.log
	|__________  Modules
			   |__________  __init__.py
			   |__________  Common.py
			   |__________  Constants.py
			   |__________  DBManager.py
			   |__________  Logger.py
			   |__________  RulesEngine.py

Directories:
Config:
	Contains configuration files.

Docs:
	Contains code related documents.

Logs:
	Contains log files.
	
Modules:
	Contains all modules used in code.


Files:
readme.txt:
	Help text and description.

main.py:
	Execution point of the program.

Config.ini:
	Configuration file containing different section so that only required configuration can be loaded.

Code_Description.docx:
	Contains description about all the files and directories of code.

Main.log:
	Log file to store all log information. Log information is further classified into info, warning and error.

__init__.py:
	This file is required to consider the directory as package. This is needed when we import any module from this directory.

Common.py:
	Contains all common functions which can be called by other modules.
Constants.py:
	Contains all constants which can be used all over the code.

DBManager.py:
	Contains all database communication functions.

Logger.py:
	Contains function to configure and return logger which can be used all over the code to log everything in log file.

RulesEngine.py:
	Contains function to  run rule engine.




