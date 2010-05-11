###--- Directories ---###

INSTALL_DIR = None			# must be set for each installation

GATHER_DIR = INSTALL_DIR + 'gather/'
POPULATE_DIR = INSTALL_DIR + 'populate/'
DATA_DIR = INSTALL_DIR + 'data/'
SCHEMA_DIR = INSTALL_DIR + 'schema/'
CONTROL_DIR = INSTALL_DIR + 'control/'
LIB_DIR = INSTALL_DIR + 'lib/python/'
LOG_DIR = INSTALL_DIR + 'logs/'

###--- general ---###

MAX_SUBPROCESSES = 3		# maximum number of simultaneous subprocesses
CHUNK_SIZE = 100000		# number of records to process per chunk

###--- Sybase connection data (read-only) ---###

SYBASE_SERVER = 'DEV_MGI'
SYBASE_DATABASE = 'mgd'
SYBASE_USER = 'mgd_public'
SYBASE_PASSWORD = 'mgdpub'

###--- MySQL connection data (read-write) ---###

MYSQL_HOST = 'localhost'
MYSQL_DATABASE = 'fe'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'mysqlr00t'

###--- Logging Levels ---###

LOG_DEBUG = True
LOG_INFO = True

###--- automatically adjust Python library path ---###

import sys
sys.path.insert (0, SCHEMA_DIR)
sys.path.insert (0, LIB_DIR)
