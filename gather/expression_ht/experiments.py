# Name: expression_ht/experiments.py
# Purpose: provides functions for dealing with experiment retrieval across multiple expression_ht* gatherers

import dbAgnostic
import logger
import constants as C

###--- Globals ---###

experimentTable = None

###--- Private Functions ---###

def getRowCount(tableName):
    # return the row count for the given 'tableName'
    
    cmd0 = 'select count(1) from %s' % tableName
    cols, rows = dbAgnostic.execute(cmd0)
    return rows[0][0] 

###--- Public Functions ---###

def getExperimentTempTable():
    # Get the name of a temp table that contains the experiments we need to move to the front-end database.
    
    global experimentTable
    
    if experimentTable:             # already built the table?  If so, just return the name.
        return experimentTable
    
    experimentTable = 'gxdht_experiments_to_move'
    cmd0 = '''create temp table %s (
                _Experiment_key    int    not null
                )''' % experimentTable
                
# TODO:
#    cmd1 = '''insert into %s
#        select e._Experiment_key
#        from gxd_htexperiment e, voc_term t
#        where e._CurationState_key = t._Term_key
#            and t.term = '%s' ''' % (experimentTable, C.DONE)
    
    # replace with above command to switch to only-curated data
    cmd1 = '''insert into %s
                select e._Experiment_key
                from gxd_htexperiment e''' % experimentTable
    
    cmd2 = 'create unique index getm1 on %s (_Experiment_key)' % experimentTable

    dbAgnostic.execute(cmd0)
    logger.debug('Created temp table %s' % experimentTable)

    dbAgnostic.execute(cmd1)
    logger.debug('Populated %s with %d rows' % (experimentTable, getRowCount(experimentTable)))
                
    dbAgnostic.execute(cmd2)
    logger.debug('Indexed %s' % experimentTable)
    return experimentTable

def getExperimentIDs(onlyPrimaryIDs = False):
    # Get a tuple of (columns, rows) -- as returned by dbAgnostic.execute() -- for the accession IDs
    # related to the set of experiments that need to move to the front-end database.  If 'onlyPrimaryIDs' is
    # true, then only rows for those IDs will be returned.
    
    preferredClause = ''
    if onlyPrimaryIDs:
        preferredClause = ' and a.preferred = 1 '
        
    cmd0 = '''select e._Experiment_key,
                    a.accID,
                    l.name as logical_db,
                    a.preferred,
                    a.private
                from %s e, acc_accession a, acc_logicaldb l
                where e._Experiment_key = a._Object_key
                    and a._MGIType_key = %d
                    and a._LogicalDB_key = l._LogicalDB_key
                    %s''' % (getExperimentTempTable(), C.MGITYPE_EXPERIMENT, preferredClause)
                    
    return dbAgnostic.execute(cmd0)

# TODO -- get rid of this hack, as it's only temporary until we have curated data
allIDs = None
def getExperimentKey(experimentID):
    global allIDs
    if allIDs == None:
        cols, rows = getExperimentIDs()
        keyCol = dbAgnostic.columnNumber(cols, '_Experiment_key')
        idCol = dbAgnostic.columnNumber(cols, 'accID')
        
        allIDs = {}
        for row in rows:
            allIDs[row[idCol]] = row[keyCol]
    
    if experimentID in allIDs:
        return allIDs[experimentID]
    return None 
    
# TODO -- get rid of this hack, as it's only temporary until we have curated data
def getStudyTypeHack(studyType, description):
    if studyType:
        return studyType

	if description and description.find('baseline') >= 0:
		studyType = 'baseline'
	else:
		studyType = 'differential'

    return studyType
