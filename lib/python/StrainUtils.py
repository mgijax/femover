# Module: StrainUtils.py
# Purpose: to provide handy utility functions for dealing with mouse strains

import dbAgnostic
import logger
import gc

###--- Globals ---###

STRAIN_TEMP_TABLE = None        # temp table containing strain keys selected for front-end database

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
    cmd2 = 'select count(1) as ct from %s' % STRAIN_TEMP_TABLE
    cmd3 = 'create unique index %s_key on %s (_Strain_key)' % (STRAIN_TEMP_TABLE, STRAIN_TEMP_TABLE)
    
    cols0, rows0 = dbAgnostic.execute(cmd0)
    cols1, rows1 = dbAgnostic.execute(cmd1)
    cols2, rows2 = dbAgnostic.execute(cmd2)
    cols3, rows3 = dbAgnostic.execute(cmd3)
    
    logger.debug('Created strain temp table with %d of %d strains' % (rows1[0][0], rows2[0][0]))
    
    return STRAIN_TEMP_TABLE