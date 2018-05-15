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
if os.environ.has_key('GO_GRAPH_PATH'):
	INSTALL_DIR = os.environ['FEMOVER']
else:
	INSTALL_DIR = ".."

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

if os.environ.has_key('FULL_TEXT_LINKS_PATH'):
	FULL_TEXT_LINKS = os.environ['FULL_TEXT_LINKS_PATH']
else:
	FULL_TEXT_LINKS_PATH = "fullTextLinks.txt"

if os.environ.has_key('INCIDENTAL_MUTS_FILE'):
	INCIDENTAL_MUTS_FILE = os.environ['INCIDENTAL_MUTS_FILE']
else:
	INCIDENTAL_MUTS_FILE="/bhmgiapp01data/loads/mgi/incidental_muts/curator_INC.txt"

if os.environ.has_key('GLOSSARY_PATH'):
	GLOSSARY_FILE = os.environ['GLOSSARY_PATH']
else:
	GLOSSARY_PATH="glossary.rcd"

###--- general ---###
if os.environ.has_key('CHUNK_SIZE'):
	CHUNK_SIZE = int(os.environ['CHUNK_SIZE'])
else:
	CHUNK_SIZE = 100000

if os.environ.has_key('IMSR_COUNT_TIMEOUT'):
	IMSR_COUNT_TIMEOUT = int(os.environ['IMSR_COUNT_TIMEOUT'])
else:
	IMSR_COUNT_TIMEOUT = 300

if os.environ.has_key('IMSR_COUNT_URL'):
	IMSR_COUNT_URL = os.environ['IMSR_COUNT_URL']
else:
	IMSR_COUNT_URL = "http://www.findmice.org/report/mgiCounts.txt"

if os.environ.has_key('IMSR_STRAIN_URL'):
	IMSR_STRAIN_URL = os.environ['IMSR_STRAIN_URL']
else:
	IMSR_STRAIN_URL = "http://www.findmice.org/report.txt?query=&states=Any&_states=1&types=Any&_types=1&repositories=Any&_repositories=1&_mutations=on&results=500000&startIndex=0&sort=score&dir"

if os.environ.has_key('MPD_STRAIN_URL'):
	MPD_STRAIN_URL = os.environ['MPD_STRAIN_URL']
else:
	MPD_STRAIN_URL = "https://phenomedoc.jax.org/MPD_downloads/straininfo.csv"

if os.environ.has_key('BUILDS_IN_SYNC'):
	BUILDS_IN_SYNC = int(os.environ['BUILDS_IN_SYNC'])
else:
	BUILDS_IN_SYNC = 1

if os.environ.has_key('VISTA_POINT_URL'):
	VISTA_POINT_URL = os.environ['VISTA_POINT_URL']
else:
	VISTA_POINT_URL = "http://pipeline.lbl.gov/tbrowser/tbrowser/?pos=chr<chromosome>:<startCoordinate>-<endCoordinate>&base=<version>&run=21507#&base=2730&run=21507&genes=refseq&indx=0&cutoff=50"

if os.environ.has_key('GENE_TREE_URL'):
	GENE_TREE_URL = os.environ['GENE_TREE_URL']
else:
	GENE_TREE_URL = "http://useast.ensembl.org/Mus_musculus/Gene/Summary?db=core;g=@@@@"

###--- source database connection (read-only) ---###

if os.environ.has_key('SOURCE_TYPE'):
	SOURCE_TYPE = os.environ['SOURCE_TYPE'].lower()
else:
	SOURCE_TYPE = 'postgres'

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
SOURCE_PASSWORD = getPassword(os.environ['%s_DBPASSWORDFILE' % prefixPW], SOURCE_USER)

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
