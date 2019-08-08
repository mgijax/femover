#!/usr/local/bin/python
# 
# Pre-computes sort values for the expression_ht_consolidated_sample_measurement table and
# use them to update the corresponding *_sequence_num table in the database.  As RNA-Seq
# data grow (quickly), doing all this in-memory would quickly go beyond our RAM limit.
# So, we'll use the database for the bulk of the work.

import sys
sys.path.insert(0, '../lib/python')
import logger
import dbAgnostic
import gc
import os

USAGE = '''Usage: %s <sort type> <destination file>
    <sort type> : type of output column (key, bySymbol, byAge, byStructure,
    	byExpressed, byExperiment)
''' % sys.argv[0]

if len(sys.argv) != 3:
    print USAGE
    print 'Error: wrong number of arguments'
    sys.exit(1)

if sys.argv[1] not in [ 'key', 'bySymbol', 'byAge', 'byStructure',
    'byExpressed', 'byExperiment' ]:
    print USAGE
    print 'Error: illegal sort type (%s)' % sys.argv[1]
    sys.exit(2)

SORT_TYPE = sys.argv[1]
OUTPUT_FILENAME = sys.argv[2]

logger.setLogFile('%s.%s.log' % (os.path.basename(sys.argv[0]).replace('.py', ''), SORT_TYPE))

# Do our post-processing on the front-end database.  (dbAgnostic is set up to look at
# the production-style database by default.)
dbAgnostic.setConnectionFE()

###--- Globals ---###

indexCount = 0

###--- Functions ---###

def createIndex(table, fields, isUnique = False):
    # Create a uniquely-named index on the (string) fields from the given (string) table.
    # Specify isUnique = True to enforce uniqueness in the index.
    global indexCount
    
    indexCount = indexCount + 1
    unique = ''
    if isUnique:
        unique = ' unique '
        
    cmd = 'create %s index tmpIdx%d on %s(%s)' % (unique, indexCount, table, fields)
    dbAgnostic.execute(cmd)
    if isUnique:
        logger.debug('Indexed %s (%s) - unique' % (table, fields))
    else:
        logger.debug('Indexed %s (%s)' % (table, fields))
    return

def buildTempTable(table, cmd):
    # Build the temp table with the given name using the given SQL command (into which we'll
    # insert the table name).
    
    dbAgnostic.execute(cmd % table)
    cols, rows = dbAgnostic.execute('select count(1) from %s' % table)
    logger.debug('Built temp table %s with %d rows' % (table, rows[0][0]))
    return

def initialize():
    # Precompute the two common orderings for consolidated sample parts:
    #    1. age min, age max, structure, stage
    #    2. structure, stage, age min, age max
    # This should be a speed-up, as we only need to consider one field rather than all four
    # for later queries.  Also, joining to the temp table rather than the base table gives us
    # a much denser set of data to work with (no unneeded rows, no unneeded columns).
    cmd1 = '''select cs.consolidated_sample_key, 
            row_number() over (order by cs.age_min, cs.age_max, ts.by_dfs, cs.theiler_stage::int) as by_age_structure_stage,
            row_number() over (order by ts.by_dfs, cs.theiler_stage::int, cs.age_min, cs.age_max) as by_structure_stage_age
        into temporary table %s
        from expression_ht_consolidated_sample cs, term_sequence_num ts
        where cs.emapa_key = ts.term_key'''
    buildTempTable('age_ss', cmd1)
    createIndex('age_ss', 'consolidated_sample_key', True)
    
    # Bring into a temp table just the by-symbol sequence numbers for official mouse markers.
    # This should be a speed-up, as there'll be more packed in each page of data from the db 
    # (no unneeded rows, no unneeded columns).
    cmd2 = '''select s.marker_key, s.by_symbol
        into temporary table %s
        from marker_sequence_num s
        where exists (select 1 from expression_ht_consolidated_sample_measurement sm
                where s.marker_key = sm.marker_key)'''
    buildTempTable('marker_seqnum', cmd2)
    createIndex('marker_seqnum', 'marker_key', True)
    
    # Build a temp table that maps from a measurement key to its corresponding sample key, experiment key,
    # and flag for expressed (1) or not (0).  Imposing ordering will make joins more efficient later on.
    cmd3 = '''select csm.consolidated_measurement_key, csm.consolidated_sample_key, cs.experiment_key,
            case when csm.level = 'Below Cutoff' then 0
            else 1
            end as expressed
        into temporary table %s
        from expression_ht_consolidated_sample_measurement csm, expression_ht_consolidated_sample cs
        where csm.consolidated_sample_key = cs.consolidated_sample_key
        order by 1'''
    buildTempTable('csm_subset', cmd3)
    createIndex('csm_subset', 'consolidated_measurement_key', True)
    createIndex('csm_subset', 'consolidated_sample_key')
    createIndex('csm_subset', 'experiment_key')
    
    # Build a temp table that maps from an experiment key to its ordering based on its primary ID
    # and (secondarily) its name.
    cmd4 = '''select experiment_key, row_number() over (order by by_primary_id, by_name) as by_experiment
        into temporary table %s
        from expression_ht_experiment_sequence_num'''
    buildTempTable('expt_seqnum', cmd4)
    createIndex('expt_seqnum', 'experiment_key', True)
    return

#def createResultsTable(destinationTable):
#    # fill the expression_ht_consolidated_sample_measurement_sequence_num  table for storing
#    # the precomputed sort values for each measurement key.  Imposing ordering will make
#    # joins more efficient later on.
#    dbAgnostic.execute('delete from %s' % destinationTable)
#    logger.debug('Dropped rows from %s' % destinationTable)
#
#    cmd = '''insert into %s
#    	select consolidated_measurement_key, 1, 1, 1, 1, 1
#        from expression_ht_consolidated_sample_measurement
#        order by 1'''
#
#    buildTempTable(destinationTable, cmd)
#    return

def createSortableTable():
    # Create a scratch table for sorting the set of measurements different ways.  Impose ordering to
    # help make joins to similarly ordered tables efficient.
    cmd = '''select csm.consolidated_measurement_key, cs.experiment_key, m.by_symbol,
            t.by_age_structure_stage, t.by_structure_stage_age, cs.expressed, e.by_experiment
        into temporary table %s
        from expression_ht_consolidated_sample_measurement csm,
            csm_subset cs,
            marker_seqnum m,
            age_ss t,
            expt_seqnum e
        where csm.marker_key = m.marker_key
            and csm.consolidated_measurement_key = cs.consolidated_measurement_key
            and csm.consolidated_sample_key = t.consolidated_sample_key
            and cs.experiment_key = e.experiment_key
        order by 1'''
    buildTempTable('results', cmd)
    createIndex('results', 'consolidated_measurement_key', True)
    return

def sortBy(destinationTable, destinationField, sortFields):
    # update the destination field in the destination table to have a precomputed integer field
    # for sorting by the given (string) of sort fields
    
    # create a temp table that's properly sorted using the given sortFields with row numbers
    # yielding the sort values
    cmd1 = '''select consolidated_measurement_key, row_number() over (order by %s) as seq_num
        into temporary table %%s
        from results
        order by 1''' % sortFields
    buildTempTable('sorting_table', cmd1)
#    createIndex('sorting_table', 'consolidated_measurement_key', True)
    
#    # apply those row numbers to the specified destination field in the destination table
#    cmd2 = '''update %s
#        set %s = sorting_table.seq_num
#        from sorting_table
#        where %s.consolidated_measurement_key = sorting_table.consolidated_measurement_key''' % (
#            destinationTable, destinationField, destinationTable)
#    dbAgnostic.execute(cmd2)
#    logger.debug('Updated %s.%s' % (destinationTable, destinationField))
#    
#    # and drop the temp table we used to precompute those sort values
#    cmd3 = 'drop table sorting_table'
#    dbAgnostic.execute(cmd3)
#    logger.debug('Dropped sorting_table')
#
#    dbAgnostic.commit()
    return
    
def dumpColumn(table, column):
    cmd = 'select count(1) from %s' % table
    cols, rows = dbAgnostic.execute(cmd)
    rowCount = rows[0][0]
    logger.debug('Dumping %d values from %s.%s' % (rowCount, table, column))

    offset = 0
    limit = 5000000
    cmd2 = 'select %s from %s limit %d offset %d'

    fp = open(OUTPUT_FILENAME, 'w')

    while offset < rowCount:
        cols, rows = dbAgnostic.execute(cmd2 % (column, table, limit, offset))
	for row in rows:
	    fp.write('%s\n' % row[0])
	offset = offset + limit

    fp.close()
    return

###--- Main ---###

if __name__ == '__main__':
    csm = 'expression_ht_consolidated_sample_measurement_sequence_num'

    initialize()
#    createResultsTable(csm)
    createSortableTable()
    
    if SORT_TYPE == 'key':
        dumpColumn('results', 'consolidated_measurement_key')
    else:
        if SORT_TYPE == 'bySymbol':
            sortBy(csm, 'by_gene_symbol', 'by_symbol, by_age_structure_stage, expressed, experiment_key')
	
	elif SORT_TYPE == 'byAge':
            sortBy(csm, 'by_age', 'by_age_structure_stage, expressed, by_symbol, experiment_key')
	
	elif SORT_TYPE == 'byStructure':
            sortBy(csm, 'by_structure', 'by_structure_stage_age, expressed, by_symbol, experiment_key')

	elif SORT_TYPE == 'byExpressed':
            sortBy(csm, 'by_expressed', 'expressed, by_symbol, by_age_structure_stage, experiment_key')
	
	elif SORT_TYPE == 'byExperiment':
            sortBy(csm, 'by_experiment', 'by_experiment, by_symbol, by_age_structure_stage, experiment_key')

	dumpColumn('sorting_table', 'seq_num')
