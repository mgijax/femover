# Module: StrainUtils.py
# Purpose: to provide handy utility functions for dealing with mouse strains

import dbAgnostic
import logger

###--- Globals ---###

STRAIN_TEMP_TABLE = None        # temp table containing strain keys selected for front-end database
STRAIN_REF_TEMP_TABLE = None    # temp table containins strain key / reference key pairs for front-end db
STRAIN_ID_TEMP_TABLE = None     # temp table containing strain key / primary MGI ID pairs for front-end db

###--- Functions ---###

def getStrainTempTable():
    # Returns: the name of a temp table that contains only those strain keys selected to be
    #   part of the front-end database.  Creates the table first, if it doesn't exist yet.'
    
    global STRAIN_TEMP_TABLE
    
    if STRAIN_TEMP_TABLE:
        return STRAIN_TEMP_TABLE
    
    STRAIN_TEMP_TABLE = 'selected_strains'
    
    # Strains to move to the front-end database...
    # 1. are not flagged as private (these should already have been removed by delete private data script)
    # 2. do not contain "involves", "either", " and ", or " or "
    # 3. have at least one attribute other than "Not Applicable" and "Not Specified"
    
    cmd0 = '''select s._Strain_key
        into temp table %s
        from prb_strain s
        where s.private = 0
            and s.strain not ilike '%%involves%%'
            and s.strain not ilike '%%either%%'
            and s.strain not ilike '%% and %%'
            and s.strain not ilike '%% or %%'
            and exists (select 1 from voc_annot va, voc_term t
                where va._AnnotType_key = 1009
                and va._Term_key = t._Term_key
                and t.term != 'Not Applicable'
                and t.term != 'Not Specified'
                and va._Object_key = s._Strain_key)''' % STRAIN_TEMP_TABLE
                
    cmd1 = 'select count(1) as ct from %s' % STRAIN_TEMP_TABLE
    cmd2 = 'select count(1) as ct from prb_strain'
    cmd3 = 'create unique index %s_key on %s (_Strain_key)' % (STRAIN_TEMP_TABLE, STRAIN_TEMP_TABLE)
    
    cols0, rows0 = dbAgnostic.execute(cmd0)
    cols1, rows1 = dbAgnostic.execute(cmd1)
    cols2, rows2 = dbAgnostic.execute(cmd2)
    cols3, rows3 = dbAgnostic.execute(cmd3)
    
    logger.debug('Created strain temp table with %d of %d strains' % (rows1[0][0], rows2[0][0]))
    
    return STRAIN_TEMP_TABLE

def getStrainReferenceTempTable():
    # Returns: the name of a temp table that contains strain key / reference key pairs.  Note: Only
    #   includes those strain keys selected to be part of the front-end database.  Creates the table
    #   first, if it doesn't exist yet.'
    
    global STRAIN_REF_TEMP_TABLE
    
    if STRAIN_REF_TEMP_TABLE:
        return STRAIN_REF_TEMP_TABLE
    
    STRAIN_REF_TEMP_TABLE = 'strains_with_references'
    
        # 0. collect (in a temp table) the list of references for each strain.  Largely adapted from
        #       PRB_getStrainReferences() stored procedure in pgmgddbschema product.
    cmd0 = '''  select v._strain_key, v._refs_key
                into temp table <<temp>>
                from prb_strain_reference_view v, %s t
                where v._strain_key = t._strain_key
                union
                select mra._object_key as _strain_key, mra._refs_key
                from mgi_reference_assoc mra, %s t
                where mra._refassoctype_key in (1009, 1010, 1031)
                        and mra._object_key = t._strain_key'''
            
    cmd0 = cmd0.replace('%s', getStrainTempTable())
    cmd0 = cmd0.replace('<<temp>>', STRAIN_REF_TEMP_TABLE)

    dbAgnostic.execute(cmd0)
    logger.debug('Created strain/ref table')
    
        # 1-2. index the temp table
    cmd1 = 'create index strainIndex1 on %s (_Strain_key)' % STRAIN_REF_TEMP_TABLE
    cmd2 = 'create index strainIndex2 on %s (_Refs_key)' % STRAIN_REF_TEMP_TABLE
    dbAgnostic.execute(cmd1)
    dbAgnostic.execute(cmd2)
    logger.debug(' - added indexes')

    # 3. measurements
    cmd3 = 'select count(1) from %s' % STRAIN_REF_TEMP_TABLE
    cols, rows = dbAgnostic.execute(cmd3)
    logger.debug(' - has %d rows' % rows[0][0])
    return STRAIN_REF_TEMP_TABLE
    
def getStrainIDTempTable():
    # Returns: the name of a temp table that contains strain key / primary MGI ID pairs to be
    #   part of the front-end database.  Creates the table first, if it doesn't exist yet.'
    
    global STRAIN_ID_TEMP_TABLE
    
    if STRAIN_ID_TEMP_TABLE:
        return STRAIN_ID_TEMP_TABLE
    
    STRAIN_ID_TEMP_TABLE = 'strain_ids'
    
    cmd0 = '''select f._Strain_key, a.accID as strain_id
        into temp table %s
        from acc_accession a, %s f
        where a._MGIType_key = 10
            and a._Object_key = f._Strain_key
            and a._LogicalDB_key = 1
            and a.preferred = 1''' % (STRAIN_ID_TEMP_TABLE, getStrainTempTable())
    dbAgnostic.execute(cmd0)
            
    cmd1 = 'create index strainIDIndex1 on %s (_Strain_key)' % STRAIN_ID_TEMP_TABLE
    dbAgnostic.execute(cmd1)
    
    cmd2 = 'select count(1) from %s' % STRAIN_ID_TEMP_TABLE
    cols, rows = dbAgnostic.execute(cmd2)
    
    logger.debug('Put %d strain IDs in temp table' % rows[0][0])
    return STRAIN_ID_TEMP_TABLE
