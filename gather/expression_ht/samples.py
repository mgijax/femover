# Name: expression_ht/experiments.py
# Purpose: provides functions for dealing with experiment retrieval across multiple expression_ht* gatherers

import dbAgnostic
import logger
import constants as C
from experiments import getRowCount, getExperimentTempTable

###--- Globals ---###

sampleTable = None

###--- Public Functions ---###

def getSampleTempTable():
    # Get the name of a temp table that contains the samples we need to move to the front-end database.
    
    global sampleTable

    if sampleTable:             # already built the table?  If so, just return the name.
        return sampleTable
    
    sampleTable = 'gxdht_samples_to_move'
    cmd0 = '''create temp table %s (
                _Sample_key        int    not null,
                _Experiment_key    int    not null
                )''' % sampleTable
                
    cmd1 = '''insert into %s
                select s._Sample_key, e._Experiment_key
                from %s e, gxd_htsample s
                where e._Experiment_key = s._Experiment_key''' % (sampleTable, getExperimentTempTable())
                
    cmd2 = 'create unique index gets1 on %s (_Sample_key)' % sampleTable
    cmd3 = 'create unique index gets2 on %s (_Experiment_key)' % getExperimentTempTable()
    
    dbAgnostic.execute(cmd0)
    logger.debug('Created temp table %s' % sampleTable)
    
    dbAgnostic.execute(cmd1)
    logger.debug('Populated %s with %d rows' % (sampleTable, getRowCount(sampleTable)))
    
    dbAgnostic.execute(cmd2)
    dbAgnostic.execute(cmd3)
    logger.debug('Indexed %s' % sampleTable)
    return sampleTable

def getSampleCountsByExperiment():
    # Get a dictionary mapping from each experiment key to the count of its samples (for experiments
    # that are to be moved into the front-end database).
    
    cmd0 = 'select _Experiment_key from %s' % getExperimentTempTable()
# TODO -- uncomment this query and remove the hacked one below, once we have curated samples
#    cmd1 = '''select _Experiment_key, count(1) as ct 
#                from %s
#                group by _Experiment_key''' % getSampleTempTable()
    cmd1 = '''select t._Experiment_key, count(distinct f.sample_id) as ct 
                from %s t, acc_accession a, gxd_htsample_fields f
                where t._Experiment_key = a._Object_key
                    and a._MGIType_key = 42
                    and a.preferred = 1
                    and a.accID = f.experiment_id
                group by t._Experiment_key''' % getExperimentTempTable()
                
    counts = {}

    # We initialize all experiment counts to zero first, just in case there are some experiments
    # without samples.  Then we come back and overwrite the counts for those with samples.
    
    cols, rows = dbAgnostic.execute(cmd0)
    for row in rows:
        counts[row[0]] = 0
    logger.debug('Initialized sample counts for %d experiments' % len(rows))
    
    cols, rows = dbAgnostic.execute(cmd1)
    exptKeyCol = dbAgnostic.columnNumber(cols, '_Experiment_key')
    countCol = dbAgnostic.columnNumber(cols, 'ct')
    
    for row in rows:
        counts[row[exptKeyCol]] = row[countCol]
    logger.debug('Got final sample counts for %d experiments' % len(rows))
    return counts
