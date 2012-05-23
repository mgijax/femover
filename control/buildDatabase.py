#!/usr/local/bin/python

# Name: buildDatabase.py
# Purpose: to build all or sections of a front-end database
# Notes:  The rough outline for this script is:
#	1. drop existing tables
#	2. create new tables
#	3. gather data into files (extract it from the source database)
#	4. convert the files into an appropriate format for the target table
#	5. load the data files into the new tables
#	6. create the clustering index on the table (if it has one)
#	7. cluster the data in the table (if it had a clustering index)
#	8. vacuum and analyze the tables to clean them up
#	9. create any other indexes on the data in the new tables
#      10. create foreign keys on the tables
#      11. add comments to tables, columns, and indexes
#    Steps 3-11 run in parallel.  Once data has been gathered for a table, its
#    conversion is started.  Once its file conversion finishes, its bulk
#    load is started.  Once its load completes, its clustering index is begun.
#    Once the clustering index is complete, the data is clustered by it...
#    and so on.  Many of these steps have allow a configurable amount of
#    parallelism (how many of them to execute at a time).

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

if '.' not in sys.path:
	sys.path.insert (0, '.')

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

# tables waiting for indexing; table -> [ index process ids ]
INDEXING_TABLES = {}

# maps from index process id to table name
TABLE_BY_INDEX_ID = {}

# list of table names that have their creation, load, and indexes finished
DONE_TABLES = []

# tables waiting for foreign keys; table -> [ foreign key process ids ]
FK_TABLES = {}

# maps from foreign key process id to table name
TABLE_BY_FK_ID = {}

# list of strings, each of which is an 'add foreign key' statement that we're
# waiting to schedule.  (We wait until both involved tables have had their
# indexes completed.)
WAITING_FK = []

# error messages about failed foreign keys (just collect them, as foreign keys
# are not critical)
FAILED_FK = []

# error messages about failed comments (just collect them, as comments
# are not critical)
FAILED_COMMENTS = []

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
FK_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_FK)
CLUSTERED_INDEX_DISPATCHER = Dispatcher.Dispatcher (
	config.CONCURRENT_CLUSTERED_INDEX)
CLUSTER_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_CLUSTER)
COMMENT_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_COMMENT)
OPTIMIZE_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_OPTIMIZE)

# lists of not-yet-finished processes for each type of operation.  Each list
# item is a tuple of (descriptive string, dispatcher process id).
GATHER_IDS = []
CONVERT_IDS = []
BCPIN_IDS = []
INDEX_IDS = []
FK_IDS = []
CLUSTERED_INDEX_IDS = []
CLUSTER_IDS = []
COMMENT_IDS = []
OPTIMIZE_IDS = []

# integer status values for each type of operation.
GATHER_STATUS = NOTYET
CONVERT_STATUS = NOTYET
BCPIN_STATUS = NOTYET
INDEX_STATUS = NOTYET
FK_STATUS = NOTYET
CLUSTERED_INDEX_STATUS = NOTYET
CLUSTER_STATUS = NOTYET
COMMENT_STATUS = NOTYET
OPTIMIZE_STATUS = NOTYET

# lists of strings, each of which is a table to be created for that
# particular data type:
ACCESSION = [ 'accession', 'actual_database',
	]
ALLELES = [ 'allele', 'allele_id', 'allele_counts', 'allele_note',
		'allele_sequence_num', 'allele_to_sequence',
		'allele_to_reference', 'allele_synonym', 'allele_mutation',
	]
ANNOTATIONS = [ 'annotation'
	]
BATCHQUERY = [ 'batch_marker_terms', 'batch_marker_alleles',
		'batch_marker_snps', 'batch_marker_go_annotations',
		'batch_marker_mp_annotations',
	]
CRE = [ 'allele_recombinase_systems', 'recombinase', 
	]
GENOTYPES = [ 'allele_to_genotype', 'genotype', 'genotype_sequence_num',
	'disease',
	]
EXPRESSION = [ 'expression_index', 'expression_index_stages',
		'expression_index_map', 'expression_index_sequence_num',
		'expression_index_counts', 'expression_assay',
		'marker_to_expression_assay', 'expression_assay_sequence_num',
		'marker_tissue_expression_counts',
		'anatomy_structures_synonyms', 'expression_result_summary',
	]
IMAGES = [ 'image', 'image_sequence_num', 'image_alleles',
		'genotype_to_image', 'marker_to_phenotype_image', 
		'expression_imagepane', 'allele_to_image', 'image_id',
		'marker_to_expression_image',
	]
IMSR = [ 'imsr',
	]
MARKERS = [ 'marker', 'marker_id', 'marker_synonym', 'marker_to_allele',
		'marker_to_sequence', 'marker_to_reference',
		'marker_orthology', 'marker_location', 'marker_counts',
		'marker_note', 'marker_sequence_num', 
		'marker_to_probe', 'marker_count_sets', 'marker_alias',
		'marker_biotype_conflict', 'marker_searchable_nomenclature',
	]
PROBES = [ 'probe', 'probe_clone_collection', 'probe_to_sequence',
	]
REFERENCES = [ 'reference', 'reference_abstract', 'reference_book', 
		'reference_counts', 'reference_id', 'reference_sequence_num', 
		'reference_to_sequence', 'reference_individual_authors',
		'reference_note',
	]
SEQUENCES = [ 'sequence', 'sequence_counts', 'sequence_gene_model',
		'sequence_gene_trap', 'sequence_id', 'sequence_location',
		'sequence_source', 'sequence_sequence_num',
		'sequence_clone_collection', 'sequence_provider_map',
	]

VOCABULARIES = [ 'vocabulary', 'term_id', 'term_synonym', 'term_descendent',
	'term_sequence_num', 'term_ancestor_simple',
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

def checkStderr (dispatcher, id, message):
	# check the return code for 'id' in 'dispatcher', and if it is errant
	# then dump stderr and show the given 'message'

	if dispatcher.getReturnCode(id) != 0:
		print '\n'.join (dispatcher.getStderr(id))
		raise error, message
	return

def dbExecuteCmd (cmd):
	# return a string that is the unix command for calling dbExecute.py
	# for executing the given database command 'cmd'

	dbExecute = os.path.join (config.CONTROL_DIR, 'dbExecute.py')
	return "%s '%s'" % (dbExecute, cmd)

def dropForeignKeyConstraints(table):
	# Purpose: to drop the foreign key constraints that refer to the 
	#	given table
	# Returns: nothing
	# Assumes: nothing
	# Modifies: drops constraints from tables from the database
	# Throws: 'error' if problems are detected

	cmd = '''select fk.table_name as fk_table,
			cu.column_name as fk_column, 
			pk.table_name as pk_table,
			pt.column_name as pk_column,
			c.constraint_name as constraint_name
		from information_schema.referential_constraints c
		inner join information_schema.table_constraints fk
			on c.constraint_name = fk.constraint_name
		inner join information_schema.table_constraints pk
			on c.unique_constraint_name = pk.constraint_name
		inner join information_schema.key_column_usage cu
			on c.constraint_name = cu.constraint_name
		inner join (select i1.table_name, i2.column_name
			from information_schema.table_constraints i1
			inner join information_schema.key_column_usage i2
				on i1.constraint_name = i2.constraint_name
			where lower(i1.constraint_type) = 'primary key') pt
				on pt.table_name = pk.table_name
		where lower(pk.table_name) = '%s' ''' % table.lower()

	(columns, rows) = DBM.execute(cmd)
	
	dropFKDispatcher = Dispatcher.Dispatcher(1)

	if not columns:
		return

	tableCol = columns.index('fk_table')
	nameCol = columns.index('constraint_name') 

	for row in rows:
	    fk_table = row[tableCol]
	    constraint_name = row[nameCol]

	    s = dbExecuteCmd ('alter table %s drop constraint if exists %s' \
		% (fk_table, constraint_name) )

	    id = dropFKDispatcher.schedule(s)
	    dropFKDispatcher.wait() 

	    checkStderr (dropFKDispatcher, id,
		'Failed to drop constraint: %s' % constraint_name)
	return

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
			cmd = "select TABLE_NAME from information_schema.tables where table_type='BASE TABLE' and table_schema='fe'"
		else:
			raise error, 'Unknown value for TARGET_TYPE'

		(columns, rows) = DBM.execute (cmd)

		# schedule the 'drop' operation for each table

		dropDispatcher = Dispatcher.Dispatcher(config.CONCURRENT_DROP)

		for row in rows:
			dropForeignKeyConstraints(row[0]) 

		for row in rows:
			id = dropDispatcher.schedule (dbExecuteCmd (
				'drop table %s cascade' % row[0]))
			items.append ( (row[0], id) )
	else:
		# specific tables were specified, so just delete them
		for table in tables:
			dropForeignKeyConstraints(table) 

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
		checkStderr (dropDispatcher, id, 
			'Failed to drop %s table %s' % (config.TARGET_TYPE,
				table) )

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
		checkStderr (createDispatcher, id, 
			'Failed to create %s table %s' % (
				config.TARGET_TYPE, table) )

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

	# produce reports for the concurrent processes

	dispatchers = [ ('gather', GATHER_DISPATCHER),
			('convert', CONVERT_DISPATCHER),
			('load', BCPIN_DISPATCHER),
			('cluster index', CLUSTERED_INDEX_DISPATCHER),
			('cluster', CLUSTER_DISPATCHER),
			('analyze', OPTIMIZE_DISPATCHER),
			('index', INDEX_DISPATCHER),
			('foreign keys', FK_DISPATCHER),
			('comment', COMMENT_DISPATCHER),
			]

	for (name, dispatcher) in dispatchers:
		logger.debug ('%-13s : %d active : %d waiting' % (name,
			dispatcher.getActiveProcessCount(),
			dispatcher.getWaitingProcessCount() ) )

	if WAITING_FK:
		logger.debug ('%-13s : %d not scheduled' % (
			'foreign keys', len(WAITING_FK)) )

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

			checkStderr (GATHER_DISPATCHER, id,
				'Gathering failed for %s' % table)

			for line in GATHER_DISPATCHER.getStdout(id):
				line = line.strip()

				[ inputFile, table ] = line.split()

				if not FULL_BUILD:
					dropTables( [table] )
				createTables (table)
				scheduleConversion (table, inputFile)

				logger.debug (
					'Scheduled conversion of %s for %s' \
						% (inputFile, table) )
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

			checkStderr (CONVERT_DISPATCHER, id,
				'Conversion failed for %s' % table)

			logger.debug ('Finished conversion of %s' % table)
			scheduleLoad(table, path)

		i = i - 1

	# if the gathering stage finished and there are no more unfinished
	# conversions, then the conversion stage has finished -- report it

	if (GATHER_STATUS == ENDED) and (not CONVERT_IDS):
		CONVERT_STATUS = ENDED
		dbInfoTable.setInfo ('status',
			'finished file conversions')
		logger.debug ('Last file conversion finished')
	return

def askTable (table, flag, description):
	# interrogate the script representing 'table' for the given
	# command-line 'flag' and return its output, if any.  In case of
	# error, use 'description' to compose an error message.

	script = os.path.join (config.SCHEMA_DIR, table + '.py')
	myDispatcher = Dispatcher.Dispatcher()
	id = myDispatcher.schedule ('%s %s' % (script, flag))
	myDispatcher.wait()

	checkStderr (myDispatcher, id, 'Failed to %s for %s' % (description,
		table) )

	lines = myDispatcher.getStdout(id)
	return lines 
		
def scheduleClusteredIndex (table):
	global CLUSTERED_INDEX_DISPATCHER, CLUSTERED_INDEX_IDS
	global CLUSTERED_INDEX_STATUS

	# ask the table's script for its clustered index

	stmts = askTable(table, '--sci', 'get clustered index statement')

	hasClusteredIndex = False

	if stmts:
		stmt = stmts[0].strip()
		if stmt:
			id = CLUSTERED_INDEX_DISPATCHER.schedule (
				dbExecuteCmd (stmt))
			CLUSTERED_INDEX_IDS.append ( (table, stmt, id) )
			hasClusteredIndex = True

			if CLUSTERED_INDEX_STATUS != WORKING:
				CLUSTERED_INDEX_STATUS = WORKING
				logger.debug ('Began first clustered index') 

	if not hasClusteredIndex:
		# if there was no clustered index on this table, then we do
		# not need the cluster and optimize steps, either, so skip
		# ahead and add the other indexes

		scheduleIndexes(table)
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
	global INDEXING_TABLES, TABLE_BY_INDEX_ID

	# ask the table's script for its list of indexes

	statements = askTable (table, '--si', 'get index creation scripts')

	# each index creation statement will appear on a separate line; as we
	# find them, schedule execution of that 'create index' command

	INDEXING_TABLES[table] = []

	hadIndex = False
	for stmt in statements:
		id = INDEX_DISPATCHER.schedule (dbExecuteCmd (stmt.strip() ) )
		INDEX_IDS.append ( (stmt.strip(), id) )
		INDEXING_TABLES[table].append(id)
		TABLE_BY_INDEX_ID[id] = table
		hadIndex = True

	if not hadIndex:
		scheduleForeignKeys(table)
		scheduleComments(table)

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

			checkStderr (BCPIN_DISPATCHER, id,
				'Load failed for %s' % table)

			logger.debug ('Finished loading %s' % table)
			scheduleClusteredIndex(table)
			os.remove(path)

		i = i - 1

	# if the file conversion stage finished and there are no more
	# unfinished loads, then the load stage has finished -- report it

	if (CONVERT_STATUS == ENDED) and (not BCPIN_IDS):
		BCPIN_STATUS = ENDED
		logger.debug ('Last bulk load finished')
		dbInfoTable.setInfo ('status', 'finished loading data')
	return

def scheduleClustering (table):
	global CLUSTER_DISPATCHER, CLUSTER_IDS, CLUSTER_STATUS

	# ask the table's script for its data clustering command

	stmts = askTable(table, '--scl', 'get clustering statement')

	hasClustering = False

	if stmts:
		stmt = stmts[0].strip()
		if stmt:
			id = CLUSTER_DISPATCHER.schedule (dbExecuteCmd (stmt))
			CLUSTER_IDS.append ( (table, stmt, id) )
			hasClustering = True

			if CLUSTER_STATUS != WORKING:
				CLUSTER_STATUS = WORKING
				logger.debug ('Began clustering data') 

	if not hasClustering:
		# if there was no clustering for this table, then we do not
		# need the optimize step, so skip ahead and add other indexes

		scheduleIndexes(table)
	return

def scheduleOptimization (table):
	global OPTIMIZE_DISPATCHER, OPTIMIZE_IDS, OPTIMIZE_STATUS

	# ask the table's script for its table optimization command

	script = os.path.join (config.SCHEMA_DIR, '%s.py' % table)
	id = OPTIMIZE_DISPATCHER.schedule ('%s --opt' % script)
	OPTIMIZE_IDS.append ( (table, id) )

	if OPTIMIZE_STATUS != WORKING:
		OPTIMIZE_STATUS = WORKING
		logger.debug ('Began optimizing tables') 
	return

def checkForFinishedClusteredIndexes():
	# Purpose: look for any clustered index creation processes that have
	#	finished
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed index

	global CLUSTERED_INDEX_IDS, CLUSTERED_INDEX_DISPATCHER
	global CLUSTERED_INDEX_STATUS

	# walk backward through the list of unfinished indexing processes,
	# so as we delete some the loop is not disturbed

	i = len(CLUSTERED_INDEX_IDS) - 1
	while (i >= 0):
		(table, cmd, id) = CLUSTERED_INDEX_IDS[i]

		# if we detect a finished indexing process, remove it from
		# the list of unfinished processes and look to see if it
		# failed.  if it failed, then report it and bail out

		if CLUSTERED_INDEX_DISPATCHER.getStatus(id) == \
				Dispatcher.FINISHED:

			del CLUSTERED_INDEX_IDS[i]

			checkStderr (CLUSTERED_INDEX_DISPATCHER, id,
				'Indexing failed on: %s' % cmd)

			logger.debug('Finished clustered index on %s' % table)
			scheduleClustering(table)

		i = i - 1

	# if the load processes have all finished and there are no unfinished
	# indexing processes, then the indexing stage is done -- report it

	if (BCPIN_STATUS == ENDED) and (not CLUSTERED_INDEX_IDS) and \
			(CLUSTERED_INDEX_STATUS != ENDED):
		CLUSTERED_INDEX_STATUS = ENDED
		logger.debug ('Last clustered index finished')
		dbInfoTable.setInfo ('status', 'finished clustered indexes')
	return

def checkForFinishedClustering():
	# Purpose: look for any data clustering processes that have finished
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed command

	global CLUSTER_IDS, CLUSTER_DISPATCHER, CLUSTER_STATUS

	# walk backward through the list of unfinished cluster processes,
	# so as we delete some the loop is not disturbed

	i = len(CLUSTER_IDS) - 1
	while (i >= 0):
		(table, cmd, id) = CLUSTER_IDS[i]

		# if we detect a finished clustering process, remove it from
		# the list of unfinished processes and look to see if it
		# failed.  if it failed, then report it and bail out

		if CLUSTER_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:

			del CLUSTER_IDS[i]

			checkStderr (CLUSTER_DISPATCHER, id,
				'Clustering failed on: %s' % cmd)

			logger.debug('Finished clustering %s' % table)
			scheduleOptimization(table)

		i = i - 1

	# if the clustered index processes have all finished and there are no
	# unfinished clustering processes, then the clustering stage is done
	# -- report it

	if (CLUSTERED_INDEX_STATUS == ENDED) and (not CLUSTER_IDS) and \
			(CLUSTER_STATUS != ENDED):
		CLUSTER_STATUS = ENDED
		logger.debug ('Last clustering finished')
		dbInfoTable.setInfo ('status', 'finished clustering data')
	return

def checkForFinishedOptimization():
	# Purpose: look for table optimization processes that have finished
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed command

	global OPTIMIZE_IDS, OPTIMIZE_DISPATCHER, OPTIMIZE_STATUS

	# walk backward through the list of unfinished cluster processes,
	# so as we delete some the loop is not disturbed

	i = len(OPTIMIZE_IDS) - 1
	while (i >= 0):
		(table, id) = OPTIMIZE_IDS[i]

		# if we detect a finished clustering process, remove it from
		# the list of unfinished processes and look to see if it
		# failed.  if it failed, then report it and bail out

		if OPTIMIZE_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:

			del OPTIMIZE_IDS[i]

			checkStderr (OPTIMIZE_DISPATCHER, id,
				'Table optimization failed on: %s' % table)

			logger.debug('Finished optimizing %s' % table)
			scheduleIndexes(table)

		i = i - 1

	# if the clustering processes have all finished and there are no
	# unfinished optimization processes, then the optimizing stage is done
	# -- report it

	if (CLUSTER_STATUS == ENDED) and (not OPTIMIZE_IDS) and \
			(OPTIMIZE_STATUS != ENDED):
		OPTIMIZE_STATUS = ENDED
		logger.debug ('Last optimization finished')
		dbInfoTable.setInfo ('status', 'finished optimizing tables')
	return

def checkForFinishedIndexes():
	# Purpose: look for any index creation processes that have finished
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed index

	global INDEX_IDS, INDEX_DISPATCHER, INDEX_STATUS
	global INDEXING_TABLES, TABLE_BY_INDEX_ID, DONE_TABLES

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

			checkStderr (INDEX_DISPATCHER, id, 
				'Indexing failed on: %s' % cmd)

			# see if this table's indexes are done

			table = TABLE_BY_INDEX_ID[id]
			del TABLE_BY_INDEX_ID[id]

			INDEXING_TABLES[table].remove(id)
			if not INDEXING_TABLES[table]:
				DONE_TABLES.append(table)
				del INDEXING_TABLES[table]
				scheduleForeignKeys(table)
				scheduleComments(table)
				logger.debug ('Finished indexing %s' % table)
		i = i - 1

	# if the load processes have all finished and there are no unfinished
	# indexing processes, then the indexing stage is done -- report it

	if (BCPIN_STATUS == ENDED) and (not INDEX_IDS) and (INDEX_STATUS != ENDED):
		INDEX_STATUS = ENDED
		logger.debug ('Last index finished')
		dbInfoTable.setInfo ('status', 'finished indexing')
	return

def scheduleComments (table):
	# Purpose: to schedule the creation of the comments on the given
	#	table
	# Returns: nothing
	# Assumes: the given 'table' has had its indexing finished
	# Modifies: creates comments for 'table', its columns, and its
	#	indexes; updates globals listed	below
	# Throws: 'error' if we cannot discover what comments to create

	global COMMENT_IDS, COMMENT_STATUS, COMMENT_DISPATCHER

	# ask the table's script for its list of comments

	statements = askTable (table, '--sc', 'get comment statements')

	# each comment statement will appear on a separate line

	for stmt in statements:
		id = COMMENT_DISPATCHER.schedule(dbExecuteCmd(stmt))
		COMMENT_IDS.append ( (table, stmt, id) )

	# if this is our first comment creation, report it

	if (COMMENT_STATUS != WORKING) and statements:
		COMMENT_STATUS = WORKING
		logger.debug ('Began adding schema comments')
	return

def scheduleForeignKeys(table = None, doAll = False):
	# Purpose: to schedule the creation of the foreign keys on the given
	#	table
	# Returns: nothing
	# Assumes: the given 'table' has had its indexing finished
	# Modifies: creates foreign keys for 'table'; updates globals listed
	#	below
	# Throws: 'error' if we cannot discover what foreign keys to create

	global FK_IDS, FK_STATUS, FK_DISPATCHER, WAITING_FK
	global FK_TABLES, TABLE_BY_FK_ID

	# ask the table's script for its list of foreign keys

	if table:
		statements = askTable (table, '--sk',
			'get foreign key creation statements')

		# each fk creation statement will appear on a separate line;
		# add them to the list of waiting commands.  We shouldn't
		# schedule them until the tables have both been indexed, for
		# performance reasons.

		for stmt in statements:
			items = stmt.strip().split()
			table1 = items[2]
			table2 = items[10]

			WAITING_FK.append ( (table1, table2, stmt) )

	# now, schedule whichever foreign-key commands have both their tables
	# indexed

	ranOne = False

	i = len(WAITING_FK) - 1
	while i >= 0:
		(table1, table2, cmd) = WAITING_FK[i]
		if ((table1 in DONE_TABLES) and (table2 in DONE_TABLES)) or \
		    doAll:
			id = FK_DISPATCHER.schedule (dbExecuteCmd (cmd.strip() ))
			FK_IDS.append ( (cmd.strip(), id) )
			del WAITING_FK[i] 

			TABLE_BY_FK_ID[id] = table1
			if not FK_TABLES.has_key(table1):
				FK_TABLES[table1] = [ id ]
			else:
				FK_TABLES[table1].append(id)

			ranOne = True
		i = i - 1

	# if this is our first foreign key creation, report it

	if (FK_STATUS != WORKING) and ranOne:
		FK_STATUS = WORKING
		logger.debug ('Began creating foreign keys')
	return

def checkForFinishedComments():
	# Purpose: look for any comment creation processes that are done
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed statement

	global COMMENT_IDS, COMMENT_DISPATCHER, COMMENT_STATUS
	global FAILED_COMMENTS

	# walk backward through the list of unfinished comment processes,
	# so as we delete some the loop is not disturbed

	i = len(COMMENT_IDS) - 1
	while (i >= 0):
		(table, cmd, id) = COMMENT_IDS[i]

		# if we detect a finished process, remove it from
		# the list of unfinished processes and look to see if it
		# failed.  if it failed, then remember it so we can report it
		# later.  We do not want the whole build to fail just for a
		# comment failure.

		if COMMENT_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
			del COMMENT_IDS[i]
			if COMMENT_DISPATCHER.getReturnCode(id) != 0:
				FAILED_COMMENTS.append ('\n'.join ( \
					COMMENT_DISPATCHER.getStderr(id)) )

			# see if this table has any other outstanding comments

			appearsAgain = False
			for (table2, cmd2, id2) in COMMENT_IDS:
				if table == table2:
					appearsAgain = True
					break

			if not appearsAgain:
				logger.debug ('Finished comments for %s' % \
					table)
		i = i - 1

	# if the indexing processes have all finished and there are no
	# unfinished comment processes, then the comment stage is done --
	# report it

	if (INDEX_STATUS == ENDED) and (not COMMENT_IDS) and \
			(COMMENT_STATUS != ENDED):
		COMMENT_STATUS = ENDED
		logger.debug ('Last comment was added')
		dbInfoTable.setInfo ('status', 'finished adding comments')
	return

def checkForFinishedForeignKeys():
	# Purpose: look for any foreign key creation processes that are done
	# Returns: nothing
	# Assumes: nothing
	# Modifies: globals shown below
	# Throws: 'error' if we detect a failed statement

	global FK_IDS, FK_DISPATCHER, FK_STATUS, FAILED_FK

	# walk backward through the list of unfinished foreign key processes,
	# so as we delete some the loop is not disturbed

	i = len(FK_IDS) - 1
	while (i >= 0):
		(cmd, id) = FK_IDS[i]

		# if we detect a finished process, remove it from
		# the list of unfinished processes and look to see if it
		# failed.  if it failed, then remember it so we can report it
		# later.  We do not want the whole build to fail just for a
		# foreign key failure.

		if FK_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
			del FK_IDS[i]
			if FK_DISPATCHER.getReturnCode(id) != 0:
				FAILED_FK.append ('\n'.join ( \
					FK_DISPATCHER.getStderr(id)) )

			# see if this table's foreign keys are done

			table = TABLE_BY_FK_ID[id]
			del TABLE_BY_FK_ID[id]

			FK_TABLES[table].remove(id)
			if not FK_TABLES[table]:
				del FK_TABLES[table]
				logger.debug ('Finished foreign keys for %s' % table)
		i = i - 1

	# if the indexing processes have all finished and there are no
	# unfinished foreign key processes, then the foreign key stage is
	# done -- report it

	if (INDEX_STATUS == ENDED) and (not FK_IDS) and (FK_STATUS != ENDED):
		FK_STATUS = ENDED
		logger.debug ('Last foreign key finished')
		dbInfoTable.setInfo ('status', 'finished foreign keys')
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
		'reportMgiDbInfo.sh') + ' ' + config.SOURCE_TYPE)
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

	logger.info ('source: %s:%s:%s' % (config.SOURCE_TYPE,
		config.SOURCE_HOST, config.SOURCE_DATABASE))
	logger.info ('target: %s:%s:%s' % (config.TARGET_TYPE,
		config.TARGET_HOST, config.TARGET_DATABASE))
	logger.info ('log directory: %s' % config.LOG_DIR)

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

	while WORKING in (GATHER_STATUS, CONVERT_STATUS, BCPIN_STATUS, INDEX_STATUS, CLUSTERED_INDEX_STATUS, CLUSTER_STATUS, OPTIMIZE_STATUS):
		if GATHER_STATUS != ENDED:
			checkForFinishedGathering()
		if CONVERT_STATUS != ENDED:
			checkForFinishedConversion()
		if BCPIN_STATUS != ENDED:
			checkForFinishedLoad()
		if CLUSTERED_INDEX_STATUS != ENDED:
			checkForFinishedClusteredIndexes()
		if CLUSTER_STATUS != ENDED:
			checkForFinishedClustering()
		if OPTIMIZE_STATUS != ENDED:
			checkForFinishedOptimization()
		checkForFinishedIndexes()
		checkForFinishedForeignKeys()
		checkForFinishedComments()
		dispatcherReport()
		time.sleep(0.5)

	# while waiting, pick up select contents of MGI_dbInfo
	getMgiDbInfo()

	GATHER_DISPATCHER.wait()
	CONVERT_DISPATCHER.wait()
	BCPIN_DISPATCHER.wait()
	CLUSTERED_INDEX_DISPATCHER.wait()
	CLUSTER_DISPATCHER.wait()
	OPTIMIZE_DISPATCHER.wait()
	INDEX_DISPATCHER.wait()
	scheduleForeignKeys(doAll = True)	# any remaining ones
	COMMENT_DISPATCHER.wait(dispatcherReport)
	checkForFinishedComments()
	FK_DISPATCHER.wait(dispatcherReport)
	checkForFinishedForeignKeys()

	if FAILED_FK:
		for item in FAILED_FK:
			logger.debug ('Failed FK: %s' % item)
	if FAILED_COMMENTS:
		for item in FAILED_COMMENTS:
			logger.debug ('Failed comment: %s' % item)
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

		# terminate any existing subprocesses

		GATHER_DISPATCHER.terminateProcesses()
		CONVERT_DISPATCHER.terminateProcesses()
		BCPIN_DISPATCHER.terminateProcesses()
		CLUSTERED_INDEX_DISPATCHER.terminateProcesses()
		CLUSTER_DISPATCHER.terminateProcesses()
		OPTIMIZE_DISPATCHER.terminateProcesses()
		INDEX_DISPATCHER.terminateProcesses()
		COMMENT_DISPATCHER.terminateProcesses()
		FK_DISPATCHER.terminateProcesses()

	elapsed = hms(time.time() - START_TIME)
	try:
		dbInfoTable.setInfo ('status', status)
		dbInfoTable.setInfo ('build ended', time.strftime (
			'%m/%d/%Y %H:%M:%S', time.localtime()) )
		dbInfoTable.setInfo ('build total time', elapsed)
	except:
		pass

	report = 'Build %s in %s' % (status, elapsed)
	print report
	logger.info (report)

	if excType != None:
		raise excType, excValue
	logger.close()
