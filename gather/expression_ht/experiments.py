# Name: expression_ht/experiments.py
# Purpose: provides functions for dealing with experiment retrieval across multiple expression_ht* gatherers

import dbAgnostic
import logger
from . import constants as C

###--- Globals ---###

experimentTable = None
experimentReferenceTable = None

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
                _Experiment_key    int    not null,
                is_in_atlas        int    not null
                )''' % experimentTable
                
    cmd1 = '''insert into %s
        select e._Experiment_key, 0
        from gxd_htexperiment e, voc_term t
        where e._CurationState_key = t._Term_key
            and t.term = '%s' ''' % (experimentTable, C.DONE)
    
    cmd2 = 'create unique index getm1 on %s (_Experiment_key)' % experimentTable

    cmd3 = '''update %s as x
        set is_in_atlas = 1
        where exists (select 1
            from mgi_setmember msm, mgi_set ms
            where x._Experiment_key = msm._Object_key
                and msm._Set_key = ms._Set_key
                and ms.name = 'Expression Atlas Experiments')''' % experimentTable

    dbAgnostic.execute(cmd0)
    logger.debug('Created temp table %s' % experimentTable)

    dbAgnostic.execute(cmd1)
    dbAgnostic.execute(cmd2)
    dbAgnostic.execute(cmd3)
    logger.debug('Populated %s with %d rows' % (experimentTable, getRowCount(experimentTable)))
    return experimentTable

# Create a temp table joining GXD HT experiments and their references. Uses the experiments' PubMed ID properties to find the
# references (_refs_keys). The temp table has these columns:
#   _Experiment_key - key of an HT experiment (that occurs in the experiment temp table, above).
#   pubmedID - the pubmed id property value
#   _Refs_key       - key of the reference with that pubmed id
#   jnumID          - jnum for the reference (for convenience)
#
def getExperimentReferenceTempTable () :
    global experimentReferenceTable
    
    if experimentReferenceTable:             # already built the table?  If so, just return the name.
        return experimentReferenceTable
    
    experimentReferenceTable = 'gxdht_experiments_to_references'

    cmd0 = '''
        select p._object_key as _Experiment_key, c._Refs_key, p.value as pubmedID, c.jnumid as jnumID
        into temp table %s
        from MGI_Property p, BIB_Citation_Cache c, VOC_Term t, %s h
        where p._propertyterm_key = t._term_key
        and t.term = 'PubMed ID'
        and p.value = c.pubmedid
        and c.jnumid is not null
        and p._object_key = h._experiment_key
        ''' % (experimentReferenceTable, getExperimentTempTable())
    dbAgnostic.execute(cmd0)
    logger.debug('Created temp table %s' % experimentReferenceTable)
    logger.debug('Populated %s with %d rows' % (experimentReferenceTable, getRowCount(experimentReferenceTable)))
    return experimentReferenceTable

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

def getExperimentIDsAsList(onlyPrimaryIDs = False):
    # Take output from getExperimentIDs() and package it into a list of dictionaries to be
    # returned.
    
    cols, rows = getExperimentIDs(onlyPrimaryIDs)
    out = []
    
    for row in rows:
        id = {}
        c = 0
        for col in cols:
            id[col] = row[c]
            c = c + 1
        out.append(id)
    return out
