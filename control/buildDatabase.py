#!/usr/local/bin/python

# Name: buildDatabase.py
# Purpose: to build all or sections of a front-end database
# Notes:  The rough outline for this script is:
#	1. drop existing tables
#	2. create new tables
#	3. gather data into files (extract it from the source database)
#	4. convert the files into an appropriate format for the target table
#	5. load the data files into the new tables
#	6. create indexes on the data in the new tables
#    Steps 3-6 run in parallel.  Once data has been gathered for a table, its
#    conversion is started.  Once its file conversion finishes, its bulk
#    load is started.  Once its load completes, its indexing process is begun.
#    Each table's indexes are created in parallel as well.  The number of
#    concurrent gatherers, concurrent data loads, and concurrent indexing
#    opeartions are configurable values to allow for performance tuning.

import config
import sys
import Dispatcher
import logger
import dbManager
import time
import os
import traceback
import database_info
import types
import getopt

###--- Globals ---###

USAGE = '''Usage: %s [-a|-A|-b|-c|-g|-h|-i|-m|-n|-p|-r|-s|-x] [-G <gatherer to run>]
    Data sets to (re)generate:
	-a : Alleles
	-A : Accession IDs
	-b : Batch Query tables
	-c : Cre (Recombinases)
	-g : Genotypes
	-h : IMSR counts (via HTTP)
	-i : Images
	-m : Markers
	-n : Annotations
	-p : Probes
	-s : Sequences
	-r : References
	-v : Vocabularies
	-x : eXpression (GXD Data plus GXD Literature Index)
	-G : run a single, specified gatherer (useful for testing)
    If no data sets are specified, the whole front-end database will be
    (re)generated.  Any existing contents of the database will be wiped.
''' % sys.argv[0]

# databaseInfoTable object
dbInfoTable = database_info.table

# time (in seconds) at which we start the build
START_TIME = time.time()

# float; time in seconds of last call to dispatcherReport()
REPORT_TIME = time.time()

# values for the GATHER_STATUS, CONVERT_STATUS, BCPIN_STATUS, and INDEX_STATUS
# global variables
NOTYET = 0		# this piece has not started yet
WORKING = 1		# this piece is running
ENDED = 2		# this piece has finished

# Dispatcher objects, for controlling the various sections running in parallel
GATHER_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_GATHERER)
CONVERT_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_CONVERT)
BCPIN_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_BCPIN)
INDEX_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_INDEX)

# lists of not-yet-finished processes for each type of operation.  Each list
# item is a tuple of (descriptive string, dispatcher process id).
GATHER_IDS = []
CONVERT_IDS = []
BCPIN_IDS = []
INDEX_IDS = []

# integer status values for each type of operation.
GATHER_STATUS = NOTYET
CONVERT_STATUS = NOTYET
BCPIN_STATUS = NOTYET
INDEX_STATUS = NOTYET

# lists of strings, each of which is a table to be created for that
# particular data type:
ACCESSION = [ 'accession',
	]
ALLELES = [ 'allele', 'allele_id', 'allele_counts', 'allele_note',
		'allele_sequence_num', 'allele_annotation',
		'allele_to_reference', 'allele_synonym', 'allele_mutation',
		'allele_to_sequence',
	]
ANNOTATIONS = [ 'annotation'
	]
BATCHQUERY = [ 'batch_marker_terms', 'batch_marker_alleles',
	]
CRE = [ 'allele_recombinase_systems', 'recombinase', 
	]
GENOTYPES = [ 'allele_to_genotype', 'genotype', 'genotype_sequence_num',
	'disease',
	]
EXPRESSION = [ 'expression_index', 'expression_index_stages',
		'expression_index_map', 'expression_index_sequence_num',
		'expression_index_counts', 'expression_assay',
		'marker_to_expression_assay',
		'marker_tissue_expression_counts',
	]
IMAGES = [ 'image', 'image_sequence_num', 'image_alleles',
		'genotype_to_image', 'marker_to_phenotype_image', 
		'expression_imagepane', 'allele_to_image', 'image_id'
	]
IMSR = [ 'imsr',
	]
MARKERS = [ 'marker', 'marker_id', 'marker_synonym', 'marker_to_allele',
		'marker_to_sequence', 'marker_to_reference',
		'marker_orthology', 'marker_location', 'marker_counts',
		'marker_note', 'marker_sequence_num', 'marker_annotation',
		'marker_to_probe', 'marker_count_sets', 'marker_alias',
	]
PROBES = [ 'probe', 'probe_clone_collection', 'probe_to_sequence',
	]
REFERENCES = [ 'reference', 'reference_abstract', 'reference_book', 
		'reference_counts', 'reference_id', 'reference_sequence_num', 
		'reference_to_sequence', 'reference_individual_authors',
	]
SEQUENCES = [ 'sequence', 'sequence_counts', 'sequence_gene_model',
		'sequence_gene_trap', 'sequence_id', 'sequence_location',
		'sequence_source', 'sequence_sequence_num',
		'sequence_clone_collection', 'sequence_provider_map',
	]

VOCABULARIES = [ 'vocabulary', 'term_id', 'term_synonym', 'term_descendent',
	]

# list of high priority gatherers, in order of precedence
# (these will be moved up in the queue of to-do items, as they are the
# critical path)
HIGH_PRIORITY_TABLES = [ 'accession', 'sequence', 'sequence_sequence_num',
	'sequence_id', ]

# dictionary mapping each command-line flag to the list of gatherers that it
# would regenerate
FLAGS = { '-c' : CRE,		'-m' : MARKERS,		'-r' : REFERENCES,
	'-s' : SEQUENCES,	'-a' : ALLELES,		'-p' : PROBES,
	'-i' : IMAGES,		'-v' : VOCABULARIES,	'-x' : EXPRESSION,
	'-h' : IMSR,		'-g' : GENOTYPES,	'-b' : BATCHQUERY,
	'-n' : ANNOTATIONS,	'-A' : ACCESSION,
	}

# boolean; are we doing a build of the complete front-end database?
FULL_BUILD = True

# standard exception thrown by this script
error = 'buildDatabase.error'

# get the correct dbManager, depending on the type of target database
if config.TARGET_TYPE == 'mysql':
	DBM = dbManager.mysqlManager (config.TARGET_HOST,
		config.TARGET_DATABASE, config.TARGET_USER,
		config.TARGET_PASSWORD)
elif config.TARGET_TYPE == 'postgres':
	DBM = dbManager.postgresManager (config.TARGET_HOST,
		config.TARGET_DATABASE, config.TARGET_USER,
		config.TARGET_PASSWORD)
else:
	raise error, 'Unknown value for config.TARGET_TYPE'

###--- Functions ---###

def bailout (s,			# string; error message to print
	showUsage = True	# boolean; show the Usage statement?
	):
	# Purpose: give an error message on stderr and exit the script
	# Returns: does not return; exits to OS
	# Assumes: we can write to stderr
	# Modifies: writes to stderr
	# Throws: SystemExit to exit to the OS

	if showUsage:
		sys.stderr.write (USAGE)
	sys.stderr.write ('Error: %s\n' % s)
	sys.exit(1)

def processCommandLine():
	# Purpose: process the command-line to extract necessary data on how
	#	the script should run
	# Returns: list of gatherers to be run, sorted alphabetically
	# Assumes: nothing
	# Modifies: nothing
	# Throws: propagates SystemExit from bailout() in case of errors

	global FULL_BUILD

	try:
		flags = ''.join (map (lambda x : x[1], FLAGS.keys()) )
		options, args = getopt.getopt (sys.argv[1:], flags + 'G:')
	except getopt.GetoptError:
		bailout ('Invalid command-line arguments')

	if args:
		bailout ('Extra command-line argument(s): %s' % \
			' '.join (args))

	# list of gatherer names (strings), allowing for duplicates
	withDuplicates = []

	# if there were any command-line flags, process them

	if options:
		for (option, value) in options:
			# specify a particular gatherer

			if option == '-G':
				gatherer = value.replace('_gatherer', '').replace ('.py', '')
				withDuplicates.append (gatherer)
			else:
				withDuplicates = withDuplicates + FLAGS[option]

		FULL_BUILD = False

		logger.info ('Processed command-line with %d flags' % \
			len(options))
	else:
		# no flags -- do a full build of all gatherers

		FULL_BUILD = True
		for gatherers in FLAGS.values():
			withDuplicates = withDuplicates + gatherers
		logger.info ('No command-line flags; building full db')
	
	# list of gatherer names, with no duplicates allowed
	uniqueGatherers = []

	# collapse the list of potentially-duplicated gatherers into a list of
	# unique gatherer names

	for gatherer in withDuplicates:
		if gatherer not in uniqueGatherers:
			uniqueGatherers.append (gatherer)

			# check that we have a gatherer script for the
			# specified gatherer

			script = os.path.join (config.GATHER_DIR,
				'%s_gatherer.py' % gatherer)
			if not os.path.exists (script):
				bailout ('Unknown gatherer: %s' % gatherer)

	uniqueGatherers.sort()
	logger.info ('Found %d gatherers to run' % len(uniqueGatherers))
	return uniqueGatherers

def dropTables (
	tables		# list of table names (strings) to be regenerated
	):
	# Purpose: to drop the tables that need to be recreated
	# Returns: nothing
	# Assumes: nothing
	# Modifies: drops tables from the database
	# Throws: 'error' if problems are detected

	# We will drop the tables in parallel, handling up to CONCURRENT_DROP
	# operations at a time.

	dropDispatcher = Dispatcher.Dispatcher (config.CONCURRENT_DROP)

	items = []	# list of outstanding drop operations, each
			# ...a tuple of (table name, integer id)

	# if we are doing a full build, then we need to drop all tables from
	# the target database.

	if FULL_BUILD:
		# get the list of all tables from the target database

		if config.TARGET_TYPE == 'mysql':
			cmd = 'show tables'
		elif config.TARGET_TYPE == 'postgres':
			cmd = "select TABLE_NAME from information_schema.tables where table_type='BASE TABLE' and table_schema='public'"
		else:
			raise error, 'Unknown value for TARGET_TYPE'

		(columns, rows) = DBM.execute (cmd)

		# schedule the 'drop' operation for each table

		dropDispatcher = Dispatcher.Dispatcher(config.CONCURRENT_DROP)

		dbExecute = os.path.join (config.CONTROL_DIR, 'dbExecute.py')
		for row in rows:
			id = dropDispatcher.schedule (
				"%s 'drop table %s'" % (dbExecute, row[0]))
			items.append ( (row[0], id) )
	else:
		# specific tables were specified, so just delete them
		for table in tables:
			script = os.path.join (config.SCHEMA_DIR, '%s.py' % \
				table)
			id = dropDispatcher.schedule ('%s --dt' % script)
			items.append ( (table, id) )

	# wait for all the 'drop' operations to finish
	dropDispatcher.wait()

	# look for any tables that were not dropped correctly; if any,
	# report and bail out

	for (table, id) in items:
		if dropDispatcher.getReturnCode(id):
			print '\n'.join (dropDispatcher.getStderr(id))
			raise error, 'Failed to drop %s table %s' % (
				config.TARGET_TYPE, table)

	# report how many tables were dropped
	if FULL_BUILD:
		logger.info ('Dropped all %d tables' % len(items))
	else:
		logger.info ('Dropped %d table(s): %s' % (len(items), 
			', '.join (tables) ))
	return

def createTables (
	tables		# list of table names (string)
	):
	# Purpose: create table in the database for each of the given tables
	# Returns: nothing
	# Assumes: we can create tables in the database
	# Modifies: creates tables in the database
	# Throws: global 'error' if an error occurs

	# 'create table' operations are processed in parallel

	createDispatcher = Dispatcher.Dispatcher (config.CONCURRENT_CREATE)

	items = []

	if type(tables) == types.StringType:
		tables = [ tables ]

	for table in tables:
		script = os.path.join (config.SCHEMA_DIR, '%s.py' % table)
		id = createDispatcher.schedule ('%s --ct' % script)
		items.append ( (table, id) )

	# wait for all the operations to finish

	createDispatcher.wait()

	# look for and report errors; if one is found, exit

	for (table, id) in items:
		if createDispatcher.getReturnCode(id):
			print '\n'.join (createDispatcher.getStderr(id))
			raise error, 'Failed to create %s table %s' % (
				config.TARGET_TYPE, table)

	logger.info ('Created %d table(s): %s' % (len(items),
		', '.join (tables)) )
	return

def dispatcherReport():
	# Purpose: periodically produce a report of activity for the various
	#	Dispatchers
	# Returns: nothing
	# Assumes: we can write to stderr
	# Modifies: updates global REPORT_TIME, writes to stderr
	# Throws: nothing

	global REPORT_TIME

	# if we are not writing debug output, skip it
	if not config.LOG_DEBUG:
		return

	# only produce a report every 60 seconds
	if (time.time() - 60) <= REPORT_TIME:
		return

	# produce reports for the three concurrent processes

	dispatchers = [ ('gather', GATHER_DISPATCHER),
			('convert', CONVERT_DISPATCHER),
			('load', BCPIN_DISPATCHER),
			('index', INDEX_DISPATCHER) ]

	for (name, dispatcher) in dispatchers:
		logger.debug ('%s : %d active : %d waiting' % (name,
			dispatcher.getActiveProcessCount(),
			dispatcher.getWaitingProcessCount() ) )

	REPORT_TIME = time.time() 	# update time of last report
	return

def shuffle (
	gatherers		# list of gatherers (strings)
	):
	# Purpose: to shuffle the 'gatherers' to bring any high priority ones
	#	to the front of the list
	# Returns: a re-ordered copy of 'gatherers'
	# Assumes: nothing
	# Modifies: nothing
	# Throws: nothing

	toDo = []
	for table in HIGH_PRIORITY_TABLES:
		if table in gatherers:
			toDo.append(table)

	for table in gatherers:
		if table not in toDo:
			toDo.append(table)
	return toDo 

def scheduleGatherers (
	gatherers		# list of table names (strings)
	):
	# Purpose: to begin the data gathering stage for the various
	#	'gatherers'
	# Returns: nothing
	# Assumes: nothing
	# Modifies: uses a Dispatcher to fire off multiple subprocesses
	# Throws: nothing

	global GATHER_DISPATCHER, GATHER_STATUS, GATHER_IDS

	for table in gatherers:
		script = os.path.join (config.GATHER_DIR, '%s_gatherer.py' % \
			table)
		id = GATHER_DISPATCHER.schedule (script)
		GATHER_IDS.append ( (table, id) )

	GATHER_STATUS = WORKING
	logger.info ('Scheduled %d gatherers' % len(gatherers)) 
	return

def scheduleConversion (
	table,		# string; table name for which to schedule load for
	path		# string; path to file to load
	):
	global CONVERT_DISPATCHER, CONVERT_IDS, CONVERT_STATUS

	# if this is our first conversion process, report it
	if CONVERT_STATUS == NOTYET:
		CONVERT_STATUS = WORKING
		logger.debug ('Began converting files')

	if config.TARGET_TYPE == 'postgres':
		script = os.path.join (config.CONTROL_DIR, 
			'postgresConverter.sh')
	elif config.TARGET_TYPE == 'mysql':
		script = os.path.join (config.CONTROL_DIR,
			'mysqlConverter.sh')

	id = CONVERT_DISPATCHER.schedule ('%s %s' % (script, path))
	CONVERT_IDS.append ( (table, path, id) )
	return

def checkForFinishedGathering():
	# Purpose: look for finished data gatherers and schedule the data
	#	load for any that have finished
	# Returns: nothing
	# Assumes: nothing
	# Modifies: updates globals listed below; uses a Dispatcher to
	#	schedule execution of data loads as subprocesses
	# Throws: nothing

	global GATHER_DISPATCHER, GATHER_STATUS, GATHER_IDS

	# walk backward through the list of unfinished gathering processes,
	# so as we delete some the loop is not disturbed

	i = len(GATHER_IDS) - 1
	while (i >= 0):
		(table, id) = GATHER_IDS[i]

		# if we find a finished gatherer, then we remove it from the
		# unfinished list and schedule its file conversion

		if GATHER_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
			del GATHER_IDS[i]
			if GATHER_DISPATCHER.getReturnCode(id) == 0:
				for line in GATHER_DISPATCHER.getStdout(id):
					line = line.strip()

					[ inputFile, table ] = line.split()

					if not FULL_BUILD:
						dropTables( [table] )
					createTables (table)
					scheduleConversion (table, inputFile)

					logger.debug ('Scheduled conversion of %s for %s' % (inputFile, table))
			else:
				# an error occurred in gathering, so bail out
				print '\n'.join (
					GATHER_DISPATCHER.getStderr(id) )
				raise error, 'Gathering failed for %s' % table
		i = i - 1

	# if the last gatherer finished, then report that our gathering stage
	# has completed

	if not GATHER_IDS:
		GATHER_STATUS = ENDED
		dbInfoTable.setInfo ('status', 'finished gathering')
		logger.debug ('Last gatherer finished')
	return

def scheduleLoad (
	table,		# string; table name for which to schedule load for
	path		# string; path to file to load
	):
	# Purpose: to schedule the data load for the given 'table'
	# Returns: nothing
	# Assumes: the data file for 'table' is ready
	# Modifies: uses a Dispatcher to fire off a subprocess; updates
	#	globals listed below
	# Throws: nothing

	global BCPIN_DISPATCHER, BCPIN_IDS, BCPIN_STATUS

	# if this is our first load process, report it
	if BCPIN_STATUS == NOTYET:
		BCPIN_STATUS = WORKING
		logger.debug ('Began bulk loading')

	script = os.path.join (config.SCHEMA_DIR, table + '.py')

	id = BCPIN_DISPATCHER.schedule ('%s --lf %s' % (script, path))
	BCPIN_IDS.append ( (table, path, id) )
	return

def checkForFinishedConversion():
	# Purpose: look for finished data loads
	# Returns: nothing
	# Assumes: nothing
	# Modifies: alters globals listed below; schedules index creation
	# Throws: 'error' if a table failed to load

	global CONVERT_DISPATCHER, CONVERT_STATUS, CONVERT_IDS

	# walk backward through the list of unfinished file conversion
	# processes, so as we delete some the loop is not disturbed

	i = len(CONVERT_IDS) - 1
	while (i >= 0):
		(table, path, id) = CONVERT_IDS[i]

		# if a file conversion operation finished, then remove it from
		# the list of unfinished processes, schedule the file to be
		# loaded

		if CONVERT_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
			del CONVERT_IDS[i]
			if CONVERT_DISPATCHER.getReturnCode(id) == 0:
				scheduleLoad(table, path)
			else:
				# conversion failed, so report it and bail out

				print '\n'.join (
					CONVERT_DISPATCHER.getStderr(id))
				raise error, 'Conversion failed for %s' % \
					table
		i = i - 1

	# if the gathering stage finished and there are no more unfinished
	# conversions, then the conversion stage has finished -- report it

	if (GATHER_STATUS == ENDED) and (not CONVERT_IDS):
		CONVERT_STATUS = ENDED
		dbInfoTable.setInfo ('status',
			'finished file conversions')
		logger.debug ('Last file conversion finished')
	return

def scheduleIndexes (
	table		# string; name of database table
	):
	# Purpose: to schedule the creation of the indexes on the given table
	# Returns: nothing
	# Assumes: the data has been loaded into the database for 'table'
	# Modifies: creates indexes on 'table'; updates globals listed below
	# Throws: 'error' if we cannot discover what indexes to create

	global INDEX_IDS, INDEX_STATUS, INDEX_DISPATCHER

	# ask the table's script for its list of indexes

	script = os.path.join (config.SCHEMA_DIR, table + '.py')
	indexDispatcher = Dispatcher.Dispatcher()
	id = indexDispatcher.schedule ('%s --si' % script)
	indexDispatcher.wait()

	if indexDispatcher.getReturnCode(id):
		print '\n'.join (indexDispatcher.getStderr(id))
		raise error, 'Failed to get index creation scripts for %s' % \
			table

	# each index creation statement will appear on a separate line; as we
	# find them, schedule execution of that 'create index' command

	dbExecute = os.path.join (config.CONTROL_DIR, 'dbExecute.py')

	for stmt in indexDispatcher.getStdout(id):
		id = INDEX_DISPATCHER.schedule ("%s '%s'" % (dbExecute,
			stmt.strip() ) )
		INDEX_IDS.append ( (stmt.strip(), id) )

	# if this is our first index creation, report it

	if INDEX_STATUS != WORKING:
		INDEX_STATUS = WORKING
		logger.debug ('Began indexing')
	return

def checkForFinishedLoad():
	# Purpose: look for finished data loads
	# Returns: nothing
	# Assumes: nothing
	# Modifies: alters globals listed below; schedules index creation
	# Throws: 'error' if a table failed to load

	global BCPIN_DISPATCHER, BCPIN_STATUS, BCPIN_IDS

	# walk backward through the list of unfinished load processes,
	# so as we delete some the loop is not disturbed

	i = len(BCPIN_IDS) - 1
	while (i >= 0):
		(table, path, id) = BCPIN_IDS[i]

		# if a load operation finished, then remove it from the list
		# of unfinished processes, schedule creation of that
		# table's indexes, and remove the data file that was loaded

		if BCPIN_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
			del BCPIN_IDS[i]
			if BCPIN_DISPATCHER.getReturnCode(id) == 0:
				scheduleIndexes(table)
				os.remove(path)
			else:
				# the load failed, so report it and bail out

				print '\n'.join (
					BCPIN_DISPATCHER.getStderr(id))
				raise error, 'Load failed for %s' % table

		i = i - 1

	# if the file conversion stage finished and there are no more
	# unfinished loads, then the load stage has finished -- report it

	if (CONVERT_STATUS == ENDED) and (not BCPIN_IDS):
		BCPIN_STATUS = ENDED
		logger.debug ('Last bulk load finished')
		dbInfoTable.setInfo ('status', 'finished loading data')
	return

def checkForFinishedIndexes():
	# Purpose: look for any index creation processes that have finished
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed index

	global INDEX_IDS, INDEX_DISPATCHER, INDEX_STATUS

	# walk backward through the list of unfinished indexing processes,
	# so as we delete some the loop is not disturbed

	i = len(INDEX_IDS) - 1
	while (i >= 0):
		(cmd, id) = INDEX_IDS[i]

		# if we detect a finished indexing process, remove it from
		# the list of unfinished processes and look to see if it
		# failed.  if it failed, then report it and bail out

		if INDEX_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
			del INDEX_IDS[i]
			if INDEX_DISPATCHER.getReturnCode(id) != 0:
				print '\n'.join ( \
					INDEX_DISPATCHER.getStderr(id))
				raise error, 'Indexing failed on: %s' % cmd
		i = i - 1

	# if the load processes have all finished and there are no unfinished
	# indexing processes, then the indexing stage is done -- report it

	if (BCPIN_STATUS == ENDED) and (not INDEX_IDS) and (INDEX_STATUS != ENDED):
		INDEX_STATUS = ENDED
		logger.debug ('Last index finished')
		dbInfoTable.setInfo ('status', 'finished indexing')
	return

def getMgiDbInfo():
	# Purpose: get data about our source database and drop it into the
	#	dbInfoTable
	# Returns: nothing
	# Assumes: we can read from the source database, write to the target
	#	datqabase, etc.
	# Modifies: the target database
	# Throws: propagates 'error' if problems occur in updating dbInfoTable

	dbInfoDispatcher = Dispatcher.Dispatcher()
	id = dbInfoDispatcher.schedule (os.path.join (config.CONTROL_DIR,
		'reportMgiDbInfo.py'))
	dbInfoDispatcher.wait()

	dbDate = 'unknown'
	pubVersion = 'unknown'
	schemaVersion = 'unknown' 

	if not dbInfoDispatcher.getReturnCode(id):
		for line in dbInfoDispatcher.getStdout(id):
			line = line.strip()
			items = line.split()
			fieldname = items[0]
			value = ' '.join(items[1:])

			if fieldname == 'lastdump_date':
				dbDate = value
			elif fieldname == 'public_version':
				pubVersion = value
			elif fieldname == 'schema_version':
				schemaVersion = value
	
	dbInfoTable.setInfo ('built from mgd database date', dbDate)
	dbInfoTable.setInfo ('built from mgd schema version', schemaVersion)
	dbInfoTable.setInfo ('built from mgd public version', pubVersion)
	return

def main():
	# Purpose: main program (main logic of the script)
	# Returns: nothing
	# Assumes: we can read from the source database, write to the target
	#	database, write to the file system, etc.
	# Modifies: the target database, writes files to the file system
	# Throws: propagates 'error' if problems occur

	logger.info ('Beginning %s script' % sys.argv[0])
	gatherers = shuffle(processCommandLine())
	dbInfoTable.dropTable()
	if FULL_BUILD:
		dropTables(gatherers)

	dbInfoTable.createTable()
	dbInfoTable.setInfo ('status', 'starting')
	dbInfoTable.setInfo ('source', '%s:%s:%s' % (config.SOURCE_TYPE,
		config.SOURCE_HOST, config.SOURCE_DATABASE))
	dbInfoTable.setInfo ('target', '%s:%s:%s' % (config.TARGET_TYPE,
		config.TARGET_HOST, config.TARGET_DATABASE))
	dbInfoTable.setInfo ('log directory', config.LOG_DIR)

	if FULL_BUILD:
		dbInfoTable.setInfo ('build type', 'full build')
	else:
		dbInfoTable.setInfo ('build type',
			'partial build, options: %s' % \
			', '.join (sys.argv[1:]))

	dbInfoTable.setInfo ('build started', time.strftime (
		'%m/%d/%Y %H:%M:%S', time.localtime(START_TIME)) )

	scheduleGatherers(gatherers)
	dbInfoTable.setInfo ('status', 'gathering data')

	while WORKING in (GATHER_STATUS, CONVERT_STATUS, BCPIN_STATUS):
		if GATHER_STATUS != ENDED:
			checkForFinishedGathering()
		if CONVERT_STATUS != ENDED:
			checkForFinishedConversion()
		if BCPIN_STATUS != ENDED:
			checkForFinishedLoad()
		checkForFinishedIndexes()
		dispatcherReport()
		time.sleep(0.5)

	# while waiting, pick up select contents of MGI_dbInfo
	getMgiDbInfo()

	GATHER_DISPATCHER.wait()
	CONVERT_DISPATCHER.wait()
	BCPIN_DISPATCHER.wait()
	INDEX_DISPATCHER.wait(dispatcherReport)
	checkForFinishedIndexes()		# do final reporting
	return

def hms (x):
	# Purpose: convert x seconds into hh:mm:ss notation

	x = int(x)
	h = x / 3600
	m = (x % 3600) / 60
	s = x % 60
	return '%0.2d:%0.2d:%0.2d' % (h, m, s)
	
###--- Main program ---###

if __name__ == '__main__':
	excType = None
	excValue = None
	excTraceback = None

	try:
		main()
		status = 'succeeded'
	except:
		(excType, excValue, excTraceback) = sys.exc_info()
		traceback.print_exception (excType, excValue, excTraceback)
		status = 'failed'

	elapsed = hms(time.time() - START_TIME)
	dbInfoTable.setInfo ('status', status)
	dbInfoTable.setInfo ('build ended', time.strftime (
		'%m/%d/%Y %H:%M:%S', time.localtime()) )
	dbInfoTable.setInfo ('build total time', elapsed)

	report = 'Build %s in %s' % (status, elapsed)
	print report
	logger.info (report)

	if excType != None:
		raise excType, excValue
	logger.close()
