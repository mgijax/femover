# Name: config.py
# Purpose: to extract configuration parameters from the environment and make
#	them available to scripts within femover

import os

error = 'config.Error'

###--- supporting functions (internal use only) ---###

def getPassword (passwordFile, username):
	# Purpose: read the password for the given 'username' from the given
	#	'passwordFile'
	# Returns: string (the password)
	# Modifies: nothing
	# Assumes: we can read from 'passwordFile'
	# Throws: any exceptions raised when we try to open the file
	# Notes: First, we try to read the file as a .pgpass-style file, where
	#	multiple users are contained in the file.  If we don't find a
	#	corresponding line, then we assume the file is an old-style
	#	password file, containing only the password on the first line.

	fp = open(passwordFile, 'r')
	lines = fp.readlines()
	fp.close()

	# pick password out of the last field of the user's line in a
	# .pgpass-style file
	for line in lines:
		if line.find(':%s:' % SOURCE_USER) >= 0:
			return line.split(':')[-1].strip()

	# if we didn't find it, assume it's an old-style password-only file
	return lines[0].strip() 

###--- directories and paths ---###

INSTALL_DIR = os.environ['FEMOVER']

GATHER_DIR = os.path.join(INSTALL_DIR, 'gather/')
POPULATE_DIR = os.path.join(INSTALL_DIR, 'populate/')
DATA_DIR = os.environ['DATA_DIR']
SCHEMA_DIR = os.path.join(INSTALL_DIR, 'schema/')
CONTROL_DIR = os.path.join(INSTALL_DIR, 'control/')
LIB_DIR = os.path.join(INSTALL_DIR, 'lib/python/')
LOG_DIR = os.environ['LOG_DIR']
MP_SLIMGRID_HEADERS = os.environ['MP_SLIMGRID_HEADERS']

# if we don't specify a GO_GRAPH_PATH, then just default to the standard one
if os.environ.has_key('GO_GRAPH_PATH'):
	GO_GRAPH_PATH = os.environ['GO_GRAPH_PATH']
else:
	GO_GRAPH_PATH = '/data/GOgraphs'

FULL_TEXT_LINKS = os.environ['FULL_TEXT_LINKS_PATH']

INCIDENTAL_MUTS_FILE = os.environ['INCIDENTAL_MUTS_FILE']

GLOSSARY_FILE = os.environ['GLOSSARY_PATH']

###--- general ---###

CHUNK_SIZE = int(os.environ['CHUNK_SIZE'])
IMSR_COUNT_TIMEOUT = int(os.environ['IMSR_COUNT_TIMEOUT'])
IMSR_COUNT_URL = os.environ['IMSR_COUNT_URL']
BUILDS_IN_SYNC = int(os.environ['BUILDS_IN_SYNC'])
VISTA_POINT_URL = os.environ['VISTA_POINT_URL']
GENE_TREE_URL = os.environ['GENE_TREE_URL']

###--- source database connection (read-only) ---###

SOURCE_TYPE = os.environ['SOURCE_TYPE'].lower()

if SOURCE_TYPE == 'postgres':
	prefix = 'PG'
	prefixPW = 'PG'
elif SOURCE_TYPE == 'mysql':
	prefix = 'MYSQL'
	prefixPW = 'MYSQL'
else:
	raise error, 'Unknown SOURCE_TYPE (%s)' % SOURCE_TYPE

SOURCE_HOST = os.environ['%s_DBSERVER' % prefix]
SOURCE_DATABASE = os.environ['%s_DBNAME' % prefix]
SOURCE_USER = os.environ['%s_DBUSER' % prefix]
SOURCE_PASSWORD = getPassword(os.environ['%s_DBPASSWORDFILE' % prefixPW],
		SOURCE_USER)

###--- target database connection (read-write) ---###

TARGET_TYPE = os.environ['TARGET_TYPE'].lower()

if TARGET_TYPE == 'postgres':
	prefix = 'PG_FE'
	prefixPW = 'PG'
elif TARGET_TYPE == 'mysql':
	prefix = 'MYSQL_FE'
	prefixPW = 'MYSQL'
else:
	raise error, 'Unknown TARGET_TYPE (%s)' % TARGET_TYPE

TARGET_HOST = os.environ['%s_DBSERVER' % prefix]
TARGET_DATABASE = os.environ['%s_DBNAME' % prefix]
TARGET_USER = os.environ['%s_DBUSER' % prefix]
TARGET_PASSWORD = getPassword(os.environ['%s_DBPASSWORDFILE' % prefixPW],
	TARGET_USER)

###--- logging levels ---###

LOG_DEBUG = True
LOG_INFO = True

if os.environ['LOG_DEBUG'].lower() != 'true':
	LOG_DEBUG = False
if os.environ['LOG_INFO'].lower() != 'true':
	LOG_INFO = False

###--- parallelization options ---###

CONCURRENT_CREATE = int(os.environ['CONCURRENT_CREATE'])
CONCURRENT_DROP = int(os.environ['CONCURRENT_DROP'])
CONCURRENT_GATHERER = int(os.environ['CONCURRENT_GATHER'])
CONCURRENT_BCPIN = int(os.environ['CONCURRENT_BCPIN'])
CONCURRENT_CLUSTERED_INDEX = int(os.environ['CONCURRENT_CLUSTERED_INDEX'])
CONCURRENT_CLUSTER = int(os.environ['CONCURRENT_CLUSTER'])
CONCURRENT_OPTIMIZE = int(os.environ['CONCURRENT_OPTIMIZE'])
CONCURRENT_COMMENT = int(os.environ['CONCURRENT_COMMENT'])
CONCURRENT_INDEX = int(os.environ['CONCURRENT_INDEX'])
CONCURRENT_FK = int(os.environ['CONCURRENT_FOREIGN_KEY'])

###--- automatically adjust Python library path ---###

import sys
sys.path.insert (1, SCHEMA_DIR)
sys.path.insert (1, LIB_DIR)
sys.path.insert (1, '.')
