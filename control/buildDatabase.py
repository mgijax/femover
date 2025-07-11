#!./python

# Name: buildDatabase.py
# Purpose: to build all or sections of a front-end database
# Notes:  The rough outline for this script is:
#       1. drop existing tables
#       2. create new tables
#       3. gather data into files (extract it from the source database)
#       5. load the data files into the new tables
#       6. create the clustering index on the table (if it has one)
#       7. cluster the data in the table (if it had a clustering index)
#       8. vacuum and analyze the tables to clean them up
#       9. create any other indexes on the data in the new tables
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
import getopt
import top
import re
import SanityChecks

if '.' not in sys.path:
        sys.path.insert (0, '.')

###--- Globals ---###

USAGE = '''Usage: %s [-a|-A|-b|-c|-C|-d|-g|-h|-i|-m|-M|-n|-o|-p|-r|-s|-x|-X] [-G <gatherer to run>]
    Data sets to (re)generate:
        -a : Alleles
        -A : Accession IDs
        -b : Batch Query tables
        -c : Cre (Recombinases)
        -C : Print out the config used without running
        -d : Disease tables (disease detail page)
        -g : Genotypes
        -l : Glossary Index
        -h : IMSR counts (via HTTP)
        -i : Images
        -l : Other
        -m : Markers
        -M : mapping data
        -n : Annotations
        -o : Human Disease Portal
        -p : Probes
        -s : Sequences
        -S : Strains
        -r : References
        -u : Universal expression data (classical + RNA-Seq)
        -v : Vocabularies
        -x : eXpression (GXD Data plus GXD Literature Index)
        -X : high-throughput expression data (from ArrayExpress)
        -t : Test data
        -G : run a single, specified gatherer (useful for testing)
    If no data sets are specified, the whole front-end database will be
    (re)generated.  Any existing contents of the database will be wiped.
''' % sys.argv[0]

CONFIG = '''This is the configuration:

Source Database Info:
Driver:   %s
Server:   %s
Database: %s
User:     %s
Password: %s

Target Database Info:
Driver:   %s
Server:   %s
Database: %s
User:     %s
Password: %s
''' % (config.SOURCE_TYPE, config.SOURCE_HOST, config.SOURCE_DATABASE, config.SOURCE_USER, config.SOURCE_PASSWORD, config.TARGET_TYPE, config.TARGET_HOST, config.TARGET_DATABASE, config.TARGET_USER, config.TARGET_PASSWORD)

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

# list of (dispatcher_id, error_message)
FAILED_DISPATCHERS = []

# time (in seconds) at which we start the build
START_TIME = time.time()

# float; time in seconds of last call to dispatcherReport()
REPORT_TIME = time.time()

# dictionary; maps from table name to list of tuples with (table with FK defined, foreign key constraint name)
FK_BY_TABLE = None

# values for the GATHER_STATUS, BCPIN_STATUS, and INDEX_STATUS
# global variables
NOTYET = 0              # this piece has not started yet
WORKING = 1             # this piece is running
ENDED = 2               # this piece has finished

# Dispatcher objects, for controlling the various sections running in parallel
GATHER_DISPATCHER = Dispatcher.Dispatcher (config.CONCURRENT_GATHERER)
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
BCPIN_IDS = []
INDEX_IDS = []
FK_IDS = []
CLUSTERED_INDEX_IDS = []
CLUSTER_IDS = []
COMMENT_IDS = []
OPTIMIZE_IDS = []

# integer status values for each type of operation.
GATHER_STATUS = NOTYET
BCPIN_STATUS = NOTYET
INDEX_STATUS = NOTYET
FK_STATUS = NOTYET
CLUSTERED_INDEX_STATUS = NOTYET
CLUSTER_STATUS = NOTYET
COMMENT_STATUS = NOTYET
OPTIMIZE_STATUS = NOTYET

# list of profiling data from finished gatherers.  Each entry is a list with:
#    0. gatherer name
#    1. max memory used (float - bytes)
#    2. average memory used (float - bytes)
#    3. max processor percentage (float)
#    4. average processor percentage (float)
#    5. elapsed time (float - seconds)
GATHER_PROFILES = []

# lists of strings, each of which is a table to be created for that
# particular data type:
ACCESSION = [ 'accession' ]
ALLELES = [ 'allele', 'allele_id', 'allele_counts', 'allele_note',
                'allele_sequence_num', 'allele_to_sequence',
                'allele_to_reference', 'allele_synonym', 'allele_mutation',
                'mp_annotation', 'allele_cell_line','allele_summary',
                'incidental_mutations', 'allele_related_marker'
        ]
ANNOTATIONS = [ 'annotation'
        ]
BATCHQUERY = [ 'batch_marker_terms', 'batch_marker_alleles',
                'batch_marker_snps', 'batch_marker_go_annotations',
                'batch_marker_mp_annotations',
        ]
CRE = [ 'recombinase', 'driver', 'recombinase_expression'
        ]
DISEASE = [ 'disease_detail' ]
GENOTYPES = [ 'allele_to_genotype', 'genotype', 'genotype_sequence_num',
        'disease', 'marker_to_genotype',
        ]
HDPORTAL = [ 'hdp_annotation', 'hdp_marker_to_reference', 'hdp_genocluster',
                'hdp_gridcluster', 'hdp_term_to_reference'
        ]
EXPRESSION = [ 'expression_index', 'expression_index_stages',
                'expression_index_map', 'expression_index_sequence_num',
                'expression_index_counts', 'expression_assay',
                'marker_to_expression_assay', 'expression_assay_sequence_num',
                'expression_result_summary', 'antibody_to_reference',
                'expression_specimen','expression_gellane', 'antibody',
                'antigen', 'marker_to_antibody', 'expression_result_cell_type',
        ]
HT_EXPRESSION = [ 'expression_ht_experiment', 'expression_ht_experiment_id',
        'expression_ht_experiment_note', 'expression_ht_experiment_property',
        'expression_ht_experiment_variable', 'expression_ht_sample_note',
        'expression_ht_experiment_sequence_num', 'expression_ht_sample',
        'expression_ht_consolidated_sample', 'expression_ht_consolidated_sample_measurement',
        'expression_ht_sample_map', 'expression_ht_sample_measurement',
        ]
UNIFIED_GXD = [
        'universal_expression_result', 'uni_by_age', 'uni_by_assaytype', 'uni_by_detected',
        'uni_by_reference', 'uni_by_structure', 'uni_by_symbol',
        ]
GLOSSARY = [ 'glossary',
        ]
IMAGES = [ 'image', 'image_sequence_num', 'image_alleles',
                'genotype_to_image', 'marker_to_phenotype_image', 
                'expression_imagepane', 'allele_to_image', 'image_id',
        ]
OTHER = [ 'glossary', 'statistic'
        ]
IMSR = [ 'imsr',
        ]
MAPPING = [ 'mapping_experiment', 'mapping_id', 'mapping_link', 'mapping_to_marker', 'mapping_rirc',
                'mapping_cross', 'mapping_hybrid', 'mapping_insitu', 'mapping_fish', 'mapping_table',
        ]
MARKERS = [ 'marker', 'marker_id', 'marker_synonym', 'marker_to_allele',
                'marker_to_sequence', 'marker_to_reference', 'marker_link',
                'marker_location', 'marker_counts', 'marker_polymorphism',
                'marker_slimgrid', 'marker_polymorphism_allele', 'strain_marker_gene_model',
                'marker_note', 'marker_sequence_num', 'marker_flags', 'strain_marker', 
                'marker_to_probe', 'marker_count_sets', 
                'marker_biotype_conflict', 'marker_searchable_nomenclature',
                'homology_cluster', 'marker_qtl_experiments', 'marker_wksilvers',
                'marker_microarray', 'marker_to_term', 'marker_related_marker',
                'marker_interaction', 'marker_mp_annotation'
        ]
PROBES = [ 'probe', 'probe_clone_collection', 'probe_to_sequence', 'probe_relative', 'probe_id',
                'probe_cdna', 'probe_primers', 'probe_sequence_num', 'probe_note', 'probe_to_reference',
                'probe_alias', 'probe_polymorphism', 'probe_polymorphism_details', 'probe_reference_sequence',
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
STRAINS = [ 'strain', 'strain_id', 'strain_to_reference', 'strain_mutation', 'strain_attribute',
                'strain_synonym', 'strain_qtl', 'strain_sequence_num', 'strain_imsr_data', 'strain_snp',
                'strain_mpd_data', 'strain_annotation', 'strain_collection', 'strain_grid',
        ]
VOCABULARIES = [ 'vocabulary', 'term_id', 'term_synonym', 'term_descendent', 'term_child',
        'term_sequence_num', 'term_ancestor', 'queryform_option', 'term', 'term_counts',
        'term_emap', 'term_emaps_child', 'go_evidence_category', 'term_note', 'term_sibling',
        'term_to_header', 'term_to_term', 'term_default_parent', 'term_annotation_counts',
        ]

TESTS = ['test_stats']

# list of high priority gatherers, in order of precedence
# (these will be moved up in the queue of to-do items, as they are the
# critical path)
HIGH_PRIORITY_TABLES = os.environ['HIGH_PRIORITY_TABLES'].split(' ')

# list of single priority gatherers
# those that run as single/non-multitasked events
SINGLE_PRIORITY_TABLES = os.environ['SINGLE_PRIORITY_TABLES'].split(' ')

# dictionary mapping each command-line flag to the list of gatherers that it
# would regenerate
FLAGS = { '-c' : CRE,           '-m' : MARKERS,         '-r' : REFERENCES,
        '-s' : SEQUENCES,       '-a' : ALLELES,         '-p' : PROBES,
        '-i' : IMAGES,          '-v' : VOCABULARIES,    '-x' : EXPRESSION,
        '-h' : IMSR,            '-g' : GENOTYPES,       '-b' : BATCHQUERY,
        '-n' : ANNOTATIONS,     '-A' : ACCESSION,       '-d' : DISEASE,
        '-t' : TESTS,           '-l' : OTHER,   '-o' : HDPORTAL,
        '-X' : HT_EXPRESSION,   '-M' : MAPPING, '-S' : STRAINS,
        '-u' : UNIFIED_GXD,
        }

# boolean; are we doing a build of the complete front-end database?
FULL_BUILD = True

# standard prefix for exceptions thrown by this script
error = 'buildDatabase.error'

# get the correct dbManager, depending on the type of target database
if config.TARGET_TYPE == 'postgres':
        DBM = dbManager.postgresManager (config.TARGET_HOST,
                config.TARGET_DATABASE, config.TARGET_USER,
                config.TARGET_PASSWORD)
else:
        raise Exception('%s: Unknown value for config.TARGET_TYPE' % error)

SOURCE_DBM = dbManager.postgresManager (config.SOURCE_HOST, config.SOURCE_DATABASE,
        config.SOURCE_USER, config.SOURCE_PASSWORD)

###--- Functions ---###

def bailout (s,                 # string; error message to print
        showUsage = True        # boolean; show the Usage statement?
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
        #       the script should run
        # Returns: list of gatherers to be run, sorted alphabetically
        # Assumes: nothing
        # Modifies: nothing
        # Throws: propagates SystemExit from bailout() in case of errors

        global FULL_BUILD

        try:
                flags = ''.join ([x[1] for x in list(FLAGS.keys())] )
                options, args = getopt.getopt (sys.argv[1:], flags + 'CG:')
        except getopt.GetoptError:
                bailout ('Invalid command-line arguments')

        if args:
                bailout ('Extra command-line argument(s): %s' % ' '.join (args))

        # list of gatherer names (strings), allowing for duplicates
        withDuplicates = []

        # if there were any command-line flags, process them

        if options:
                for (option, value) in options:
                        # specify a particular gatherer

                        if option == '-G':
                                gatherer = value.replace('_gatherer', '').replace ('.py', '')
                                withDuplicates.append (gatherer)
                        elif option == '-C':
                                sys.stderr.write ('Configuration: %s\n' % CONFIG)
                                sys.exit(0)
                        else:
                                withDuplicates = withDuplicates + FLAGS[option]


                FULL_BUILD = False

                logger.info ('Processed command-line with %d flags' % \
                        len(options))
        else:
                # no flags -- do a full build of all gatherers

                FULL_BUILD = True
                for gatherers in list(FLAGS.values()):
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
        global FAILED_DISPATCHERS
        # check the return code for 'id' in 'dispatcher', and if it is errant
        # then dump stderr and show the given 'message'

        if dispatcher.getReturnCode(id) != 0:
                errMsg = '\n'.join (dispatcher.getStderr(id))
                print("Warning: %s"%message)
                print(errMsg)
                print("carrying on...")
                FAILED_DISPATCHERS.append((id,message))
        return

def dbExecuteCmd (cmd):
        # return a string that is the unix command for calling dbExecute.py
        # for executing the given database command 'cmd'

        dbExecute = os.path.join (config.CONTROL_DIR, 'dbExecute.py')
        return "%s '%s'" % (dbExecute, cmd)

def cacheForeignKeyNames():
        # Purpose: look up the foreign keys for each table and cache them
	
        global FK_BY_TABLE
	
        cmd = '''select pk.table_name as pk_table,
            fk.table_name as fk_table,
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
            on pt.table_name = pk.table_name'''

        (columns, rows) = DBM.execute(cmd)
	
        dropFKDispatcher = Dispatcher.Dispatcher(1)

        if not columns:
            return

        pkTableCol = columns.index('pk_table')
        fkTableCol = columns.index('fk_table')
        nameCol = columns.index('constraint_name') 

        FK_BY_TABLE = {}
        for row in rows:
            pkTable = row[fkTableCol]
            fkTable = row[fkTableCol]
            constraintName = row[nameCol]

            if pkTable not in FK_BY_TABLE:
                FK_BY_TABLE[pkTable] = []
            FK_BY_TABLE[pkTable].append( (fkTable, constraintName) )

        logger.info('Cached %d FK for %d tables' % (len(rows), len(FK_BY_TABLE)) )
        return

def dropForeignKeyConstraints(table):
        # Purpose: to drop the foreign key constraints that refer to the
        #	given table
        # Returns: nothing
        # Assumes: nothing
        # Modifies: drops constraints from tables from the database
        # Throws: 'error' if problems are detected

        if FK_BY_TABLE == None:
            cacheForeignKeyNames()
		
        if table in FK_BY_TABLE:
            for (fkTable, constraintName) in FK_BY_TABLE[table]:
                dropFKDispatcher = Dispatcher.Dispatcher(1)
                
                s = dbExecuteCmd ('alter table %s drop constraint if exists %s' % (fkTable, constraintName) )
                id = dropFKDispatcher.schedule(s)
                dropFKDispatcher.wait()
                
                checkStderr (dropFKDispatcher, id, 'Failed to drop constraint: %s' % constraintName)
        return

def dropTables (
        tables          # list of table names (strings) to be regenerated
        ):
        # Purpose: to drop the tables that need to be recreated
        # Returns: nothing
        # Assumes: nothing
        # Modifies: drops tables from the database
        # Throws: 'error' if problems are detected

        # We will drop the tables in parallel, handling up to CONCURRENT_DROP
        # operations at a time.

        dropDispatcher = Dispatcher.Dispatcher (config.CONCURRENT_DROP)

        items = []      # list of outstanding drop operations, each
                        # ...a tuple of (table name, integer id)

        # if we are doing a full build, then we need to drop all tables from
        # the target database.

        if FULL_BUILD:
                # get the list of all tables from the target database
                logger.debug("Dropping all tables.")

                if config.TARGET_TYPE == 'postgres':
                        cmd = "select TABLE_NAME from information_schema.tables where table_type='BASE TABLE' and table_schema='fe'"
                else:
                        raise Exception('%s: Unknown value for TARGET_TYPE' % error)

                (columns, rows) = DBM.execute (cmd)

                # schedule the 'drop' operation for each table

                dropDispatcher = Dispatcher.Dispatcher(config.CONCURRENT_DROP)

                tables = [row[0] for row in rows]
                logger.debug("Dropping tables: " + ", ".join(tables))

                for row in rows:
                        dropForeignKeyConstraints(row[0]) 

                for row in rows:
                        id = dropDispatcher.schedule (dbExecuteCmd (
                                'drop table %s cascade' % row[0]))
                        items.append ( (row[0], id) )
        else:
                logger.debug("Dropping tables: " + ", ".join(tables))

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
        tables          # list of table names (string)
        ):
        # Purpose: create table in the database for each of the given tables
        # Returns: nothing
        # Assumes: we can create tables in the database
        # Modifies: creates tables in the database
        # Throws: global 'error' if an error occurs

        # 'create table' operations are processed in parallel

        createDispatcher = Dispatcher.Dispatcher (config.CONCURRENT_CREATE)

        items = []

        if type(tables) == type(''):
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
        #       Dispatchers
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

        REPORT_TIME = time.time()       # update time of last report
        return

def shuffle (
        gatherers               # list of gatherers (strings)
        ):
        # Purpose: to shuffle the 'gatherers' to bring any high priority ones
        #       to the front of the list
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
                        if FULL_BUILD and table not in SINGLE_PRIORITY_TABLES:
                                toDo.append(table)
                        elif not FULL_BUILD:
                                toDo.append(table)

        #print toDo
        return toDo 

def scheduleGatherers (
        gatherers               # list of table names (strings)
        ):
        # Purpose: to begin the data gathering stage for the various
        #       'gatherers'
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

def checkForFinishedGathering():
        # Purpose: look for finished data gatherers and schedule the data
        #       load for any that have finished
        # Returns: nothing
        # Assumes: nothing
        # Modifies: updates globals listed below; uses a Dispatcher to
        #       schedule execution of data loads as subprocesses
        # Throws: nothing

        global GATHER_DISPATCHER, GATHER_STATUS, GATHER_IDS, GATHER_PROFILES

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

                        # collect memory and processor usage information for
                        # the finished process

                        GATHER_PROFILES.append( [
                                table,
                                GATHER_DISPATCHER.getMaxMemory(id),
                                GATHER_DISPATCHER.getAverageMemory(id),
                                GATHER_DISPATCHER.getMaxProcessor(id),
                                GATHER_DISPATCHER.getAverageProcessor(id),
                                GATHER_DISPATCHER.getElapsedTime(id),
                                ] )

                        # get the list of tables that need to be processed and
                        # loaded due to this gatherer's run

                        for line in GATHER_DISPATCHER.getStdout(id):
                                line = line.strip()

                                [ inputFile, table ] = line.rsplit(" ",1)

                                if not FULL_BUILD:
                                        dropTables( [table] )
                                createTables (table)
                                scheduleLoad(table, inputFile)

                                logger.debug (
                                        'Scheduled load of %s into %s' \
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
        table,          # string; table name for which to schedule load for
        path            # string; path to file to load
        ):
        # Purpose: to schedule the data load for the given 'table'
        # Returns: nothing
        # Assumes: the data file for 'table' is ready
        # Modifies: uses a Dispatcher to fire off a subprocess; updates
        #       globals listed below
        # Throws: nothing

        global BCPIN_DISPATCHER, BCPIN_IDS, BCPIN_STATUS

        # if this is our first load process, report it
        if BCPIN_STATUS == NOTYET:
                BCPIN_STATUS = WORKING
                logger.debug ('Began bulk loading')

        script = os.path.join (config.SCHEMA_DIR, table + '.py')

        id = BCPIN_DISPATCHER.schedule ([script,"--lf",path])
        BCPIN_IDS.append ( (table, path, id) )
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
        table           # string; name of database table
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
                # table's indexes

                if BCPIN_DISPATCHER.getStatus(id) == Dispatcher.FINISHED:
                        del BCPIN_IDS[i]

                        checkStderr (BCPIN_DISPATCHER, id,
                                'Load failed for %s' % table)

                        logger.debug ('Finished loading %s' % table)
                        scheduleClusteredIndex(table)

                i = i - 1

        # if the file conversion stage finished and there are no more
        # unfinished loads, then the load stage has finished -- report it

        if (GATHER_STATUS == ENDED) and (not BCPIN_IDS) and (BCPIN_STATUS != ENDED):
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
        #       finished
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

        if (OPTIMIZE_STATUS == ENDED) and (not INDEX_IDS) and (INDEX_STATUS != ENDED):
                INDEX_STATUS = ENDED
                logger.debug ('Last index finished')
                dbInfoTable.setInfo ('status', 'finished indexing')
        return

def scheduleComments (table):
        # Purpose: to schedule the creation of the comments on the given
        #       table
        # Returns: nothing
        # Assumes: the given 'table' has had its indexing finished
        # Modifies: creates comments for 'table', its columns, and its
        #       indexes; updates globals listed below
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
        #       table
        # Returns: nothing
        # Assumes: the given 'table' has had its indexing finished
        # Modifies: creates foreign keys for 'table'; updates globals listed
        #       below
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
                        if table1 not in FK_TABLES:
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

        if (COMMENT_STATUS == ENDED) and (not FK_IDS) and (FK_STATUS != ENDED):
                FK_STATUS = ENDED
                logger.debug ('Last foreign key finished')
                dbInfoTable.setInfo ('status', 'finished foreign keys')
        return

def getMgiDbInfo():
        # Purpose: get data about our source database and drop it into the
        #       dbInfoTable
        # Returns: nothing
        # Assumes: we can read from the source database, write to the target
        #       datqabase, etc.
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

def formatTime (sec):
        # Purpose: take a raw (float) number of seconds and convert it to a
        #       more human-readable format (hh:mm:ss)
        # Returns: string (hh:mm:ss)
        # Assumes: sec is > 0
        # Modifies: nothing
        # Throws: nothing
        if sec < 0.0:
                return ''
        base = int(round(sec))
        hh = '%2.0f' % (base / 3600)
        base = base % 3600
        mm = '%2.0f' % (base / 60)
        ss = '%2.0f' % (base % 60)
        return '%s:%s:%s' % (hh.zfill(2), mm.zfill(2), ss.zfill(2))

def floatToString(f, precision=1):
        # Purpose: convert float 'f' to a string with the given number of places
        #       after the decimal point
        # Returns: string
        # Notes: 1. if f is None, treats it as 0
        
        if (f == None) or (f == ''):
                f = 0.0
        if (type(f) == type('')) or (type(f) == int):
                f = float(f)
        formatString = '%0.' + str(precision) + 'f'
        return formatString % f
        
def logProfilingData():
        # Purpose: produce a file in the logs directory with profiling data
        #       for the various gatherers
        # Returns: nothing
        # Assumes: we've collected the data in GATHER_PROFILES and we can
        #       write to the log directory
        # Modifies: adds a file to the log directory
        # Throws: nothing

        try:
                fp = open(os.path.join(config.LOG_DIR,
                        'gatherer_profiles.txt'), 'w')

                cols = [ 'Gatherer name', 'Max RAM', 'Max RAM (bytes)',
                        'Average RAM', 'Average RAM (bytes)',
                        'Max CPU %', 'Average CPU %', 
                        'Elapsed Time (hh:mm:ss.uuu)', 'Elapsed Time (sec)'
                        ]

                fp.write('\t'.join(cols))
                fp.write('\n')

                for (name, maxMem, avgMem, maxCpu, avgCpu, time) in GATHER_PROFILES:
                        mmString = '0'
                        mmInt = 0
                        if maxMem != None:
                                mmString = top.displayMemory(maxMem)
                                mmInt = int(maxMem)

                        amString = '0'
                        amFloat = '0.0'
                        if (avgMem != None) and (type(avgMem) == type(0.1)):
                                amString = top.displayMemory(avgMem)
                                amFloat = '%0.3f' % avgMem

                        cols = [
                                name,
                                mmString,
                                mmInt,
                                amString,
                                amFloat,
                                floatToString(maxCpu),
                                floatToString(avgCpu),
                                formatTime(time),
                                floatToString(time, 3)
                                ]

                        fp.write('\t'.join(map(str,cols)))
                        fp.write('\n')

                fp.close()
        except:
                logger.debug('Could not write gatherer_profiles.txt')
                traceback.print_exc()
        return

def readTimings():
        # Purpose: if a profiling file exists in the log directory, read it in
        #       and get the timings for each gatherer, so we can use them for
        #       optimizing the order in which gatherers run.
        # Returns: { gatherer name : seconds of runtime }
        #       or an empty dictionary if file doesn't exist
        # Assumes: nothing
        # Modifies: nothing
        # Throws: nothing

        timings = {}
        inputFile = os.path.join(config.LOG_DIR, 'gatherer_profiles.txt')

        if os.path.exists(inputFile):
                try:
                        fp = open(inputFile, 'r')
                        lines = list(map(lambda line : line.rstrip, fp.readlines()))
                        fp.close()

                        for line in lines[1:]:
                                cols = line.split('\t')

                                if len(cols) > 8:
                                        timings[cols[0]] = float(cols[8])
                except:
                        logger.debug('Could not read %s, working around it' % inputFile)

        return timings

def getLastRuntime(gatherer):
        # Purpose: get the runtime (in seconds) of the last recorded run of
        #       the specified gatherer -- from its log file, not from the
        #       profiling file
        # Returns: float or None
        # Assumes: nothing
        # Modifies: reads from the file system
        # Throws: nothing

        inputFile = os.path.join(config.LOG_DIR, '%s_gatherer.log' % gatherer)

        if os.path.exists(inputFile):
                try:
                        fp = open(inputFile, 'r')
                        lines = fp.readlines()
                        fp.close()

                        r = re.compile(r': +([0-9]+\.[0-9]{3}) sec :')   

                        match = r.search(lines[-1])
                        if match:
                                return float(match.group(1))
                except:
                        pass
        return None 

def sortGatherers(gatherers):
        # Purpose: sort the gatherers in order of decreasing runtime, to try
        #       to optimize scheduling (no long-running ones hanging around
        #       running solo for their final hour...)
        # Returns: list of gatherer names
        # Assumes: nothing
        # Modifies: nothing
        # Throws: nothing
        # Notes: This is not designed to find the optimal arrangement of
        #       gatherers, just a "good enough" grouping that will be a
        #       reasonable approximation.

        timings = readTimings()
        highPriority = 99999999

        fromProfile = 0
        fromLogfile = 0
        highPriority = 0
        lowPriority = 0

        # prioritization:
        # 1. if we have a time for a gatherer, use it
        # 2. if we can get a runtime from the gatherer's log file, use it
        # 2. if a gatherer is in the high-priority list, put it higher than
        #       the max (run sooner)
        # 3. otherwise, put it lower (run later)

        toOrder = []
        for gatherer in gatherers:
                if gatherer in timings:
                        toOrder.append( (timings[gatherer], gatherer) )
                        fromProfile = fromProfile + 1
                else:
                        myTime = getLastRuntime(gatherer)
                        if myTime:
                                toOrder.append( (myTime, gatherer) )
                                fromLogfile = fromLogfile + 1
                        elif gatherer in HIGH_PRIORITY_TABLES:
                                toOrder.append( (highPriority, gatherer) )
                                highPriority = highPriority + 1
                        else:
                                toOrder.append( (0, gatherer) )
                                lowPriority = lowPriority + 1

        toOrder.sort()
        toOrder.reverse()       # highest first

        logger.info('Ordered gatherers -- %d from profile, %d from log, %d high priority, %d other' % (fromProfile, fromLogfile, highPriority, lowPriority) )

        return [x[1] for x in toOrder]

def sourceContainsPrivateData():
        # Purpose: determine if the source database still has private data in it (a common
        #       error, when we forget to run the MGI_deletePrivateData script in development)
        # Returns: boolean (True if private data exists, False if not)
        # Assumes: we can read from the source database
        # Modifies: nothing
        # Throws: propagates any exceptions raised during database access
        
        # Any alleles without an Autoload or Approved status should have been removed by the
        # process to delete private data.
        
        cmd = '''select count(1)
                from all_allele a, voc_term t
                where a._Allele_Status_key = t._Term_key
                        and t.term not in ('Approved', 'Autoload')'''
        (columns, rows) = SOURCE_DBM.execute(cmd)
        
        # Just as a second check, look for a couple of note types that should also have been
        # removed.
        
        cmd2 = '''select count(1)
                from mgi_note
                where _NoteType_key in (1004,1009)'''
        (columns2, rows2) = SOURCE_DBM.execute(cmd2)
        
        return (rows[0][0] != 0) or (rows2[0][0] != 0)
        
def main():
        # Purpose: main program (main logic of the script)
        # Returns: nothing
        # Assumes: we can read from the source database, write to the target
        #       database, write to the file system, etc.
        # Modifies: the target database, writes files to the file system
        # Throws: propagates 'error' if problems occur

        logger.info ('Beginning %s script' % sys.argv[0])
        #gatherers = shuffle(processCommandLine())
        gatherers = sortGatherers(processCommandLine())
        logger.info ('source: %s:%s:%s' % (config.SOURCE_TYPE, config.SOURCE_HOST, config.SOURCE_DATABASE))
        logger.info ('target: %s:%s:%s' % (config.TARGET_TYPE, config.TARGET_HOST, config.TARGET_DATABASE))
        
        # to determine if the source database still has private data...
        if config.RUN_CONTAINS_PRIVATE == '1':
                if sourceContainsPrivateData():
                        raise Exception('%s: Source database contains private data.  Need to run MGI_deletePrivateData.csh' % error)

        dbInfoTable.dropTable()
        if FULL_BUILD:
                dropTables(gatherers)

        dbInfoTable.createTable()
        dbInfoTable.grantSelect()
        dbInfoTable.setInfo ('status', 'starting')
        dbInfoTable.setInfo ('source', '%s:%s:%s' % (config.SOURCE_TYPE, config.SOURCE_HOST, config.SOURCE_DATABASE))
        dbInfoTable.setInfo ('target', '%s:%s:%s' % (config.TARGET_TYPE, config.TARGET_HOST, config.TARGET_DATABASE))
        dbInfoTable.setInfo ('log directory', config.LOG_DIR) 
        logger.info ('source: %s:%s:%s' % (config.SOURCE_TYPE, config.SOURCE_HOST, config.SOURCE_DATABASE))
        logger.info ('target: %s:%s:%s' % (config.TARGET_TYPE, config.TARGET_HOST, config.TARGET_DATABASE))
        logger.info ('log directory: %s' % config.LOG_DIR)

        if FULL_BUILD:
                dbInfoTable.setInfo ('build type', 'full build')
        else:
                dbInfoTable.setInfo ('build type', 'partial build, options: %s' % ', '.join (sys.argv[1:]))

        dbInfoTable.setInfo ('build started', time.strftime ( '%m/%d/%Y %H:%M:%S', time.localtime(START_TIME)) )

        scheduleGatherers(gatherers)
        dbInfoTable.setInfo ('status', 'gathering data')

        while WORKING in (GATHER_STATUS, BCPIN_STATUS, INDEX_STATUS, CLUSTERED_INDEX_STATUS, CLUSTER_STATUS, OPTIMIZE_STATUS):
                if GATHER_STATUS != ENDED:
                        checkForFinishedGathering()
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
        BCPIN_DISPATCHER.wait()
        CLUSTERED_INDEX_DISPATCHER.wait()
        CLUSTER_DISPATCHER.wait()
        OPTIMIZE_DISPATCHER.wait()
        INDEX_DISPATCHER.wait()
        scheduleForeignKeys(doAll = True)       # any remaining ones
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
        logProfilingData()
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
                for err in traceback.format_exception (excType, excValue, excTraceback):
                    logger.info(err)
                status = 'failed'

                # terminate any existing subprocesses

                GATHER_DISPATCHER.terminateProcesses()
                BCPIN_DISPATCHER.terminateProcesses()
                CLUSTERED_INDEX_DISPATCHER.terminateProcesses()
                CLUSTER_DISPATCHER.terminateProcesses()
                OPTIMIZE_DISPATCHER.terminateProcesses()
                INDEX_DISPATCHER.terminateProcesses()
                COMMENT_DISPATCHER.terminateProcesses()
                FK_DISPATCHER.terminateProcesses()

        if FAILED_DISPATCHERS:
                status = 'failed'
                excType = error
                excValue = '\n\nWARNING: The Following tasks failed and may need to be rerun:\n\t-'
                excValue += '\n\t-'.join([l[1] for l in FAILED_DISPATCHERS])
                print("excType: %s, excValue: %s"%(excType,excValue))

        # do basic sanity checks on the database produced to look for obvious errors
        if status == 'succeeded':
               if not SanityChecks.databaseIsValid(DBM):
                       status = 'failed'

        elapsed = hms(time.time() - START_TIME)
        try:
                dbInfoTable.setInfo ('status', status)
                dbInfoTable.setInfo ('build ended', time.strftime (
                        '%m/%d/%Y %H:%M:%S', time.localtime()) )
                dbInfoTable.setInfo ('build total time', elapsed)
        except:
                pass

        report = 'Build %s in %s' % (status, elapsed)
        print(report)
        logger.info (report)
        logger.info ('Ending Date/Time: %s' % (time.asctime()))

        if excType != None:
                raise Exception("%s %s" % (excType, excValue))
        logger.close()
