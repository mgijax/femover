# Name: config.py
# Purpose: to extract configuration parameters from the environment and make
#       them available to scripts within femover

import os

error = 'config.Error'

###--- supporting functions (internal use only) ---###

def getPassword (passwordFile, username):
        # Purpose: read the password for the given 'username' from the given
        #       'passwordFile'
        # Returns: string (the password)
        # Modifies: nothing
        # Assumes: we can read from 'passwordFile'
        # Throws: any exceptions raised when we try to open the file
        # Notes: First, we try to read the file as a .pgpass-style file, where
        #       multiple users are contained in the file.  If we don't find a
        #       corresponding line, then we assume the file is an old-style
        #       password file, containing only the password on the first line.

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
if 'GO_GRAPH_PATH' in os.environ:
        INSTALL_DIR = os.environ['FEMOVER']
else:
        INSTALL_DIR = ".."

GATHER_DIR = os.path.join(INSTALL_DIR, 'gather/')
POPULATE_DIR = os.path.join(INSTALL_DIR, 'populate/')
DATA_DIR = os.environ['DATA_DIR']
CACHE_DIR = os.environ['CACHE_DIR']
SCHEMA_DIR = os.path.join(INSTALL_DIR, 'schema/')
CONTROL_DIR = os.path.join(INSTALL_DIR, 'control/')
LIB_DIR = os.path.join(INSTALL_DIR, 'lib/python/')
LOG_DIR = os.environ['LOG_DIR']

# if we don't specify a GO_GRAPH_PATH, then just default to the standard one
if 'GO_GRAPH_PATH' in os.environ:
        GO_GRAPH_PATH = os.environ['GO_GRAPH_PATH']
else:
        GO_GRAPH_PATH = '/data/GOgraphs'

if 'FULL_TEXT_LINKS_PATH' in os.environ:
        FULL_TEXT_LINKS = os.environ['FULL_TEXT_LINKS_PATH']
else:
        FULL_TEXT_LINKS_PATH = "fullTextLinks.txt"

if 'INCIDENTAL_MUTS_FILE' in os.environ:
        INCIDENTAL_MUTS_FILE = os.environ['INCIDENTAL_MUTS_FILE']
else:
        INCIDENTAL_MUTS_FILE="/bhmgiapp01data/loads/mgi/incidental_muts/curator_INC.txt"

if 'GLOSSARY_PATH' in os.environ:
        GLOSSARY_FILE = os.environ['GLOSSARY_PATH']
else:
        GLOSSARY_PATH="glossary.rcd"

###--- general ---###
if 'CHUNK_SIZE' in os.environ:
        CHUNK_SIZE = int(os.environ['CHUNK_SIZE'])
else:
        CHUNK_SIZE = 100000

if 'IMSR_COUNT_FILE' in os.environ:
        IMSR_COUNT_FILE = os.environ['IMSR_COUNT_FILE']
else:
        IMSR_COUNT_FILE = "/data/downloads/www.findmice.org/report/mgiCounts.txt"

if 'IMSR_STRAIN_FILE' in os.environ:
        IMSR_STRAIN_FILE = os.environ['IMSR_STRAIN_FILE']
else:
        IMSR_STRAIN_FILE = "/data/downloads/www.findmice.org/strains.txt"

if 'MPD_STRAIN_FILE' in os.environ:
        MPD_STRAIN_FILE = os.environ['MPD_STRAIN_FILE']
else:
        MPD_STRAIN_FILE = "/data/downloads/phenome.jax.org/phenomedoc?name=MPD_downloads/straininfo.csv"

if 'BUILDS_IN_SYNC' in os.environ:
        BUILDS_IN_SYNC = int(os.environ['BUILDS_IN_SYNC'])
else:
        BUILDS_IN_SYNC = 1

###--- source database connection (read-only) ---###

if 'SOURCE_TYPE' in os.environ:
        SOURCE_TYPE = os.environ['SOURCE_TYPE'].lower()
else:
        SOURCE_TYPE = 'postgres'

if SOURCE_TYPE == 'postgres':
        prefix = 'PG'
        prefixPW = 'PG'
else:
        raise error('Unknown SOURCE_TYPE (%s)' % SOURCE_TYPE)

SOURCE_HOST = os.environ['%s_DBSERVER' % prefix]
SOURCE_DATABASE = os.environ['%s_DBNAME' % prefix]
SOURCE_USER = os.environ['%s_DBUSER' % prefix]
SOURCE_PASSWORD = getPassword(os.environ['%s_DBPASSWORDFILE' % prefixPW], SOURCE_USER)
RUN_CONTAINS_PRIVATE = os.environ['RUN_CONTAINS_PRIVATE']

###--- target database connection (read-write) ---###

TARGET_TYPE = os.environ['TARGET_TYPE'].lower()

if TARGET_TYPE == 'postgres':
        prefix = 'PG_FE'
        prefixPW = 'PG'
else:
        raise error('Unknown TARGET_TYPE (%s)' % TARGET_TYPE)

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
