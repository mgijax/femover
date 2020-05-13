# Module: SequenceUtils.py

# Purpose: utility functions for working with sequences

import logger
import dbAgnostic

###--- Globals ---###

SMC = None              # optimized copy of seq_marker_cache table
MSM = None              # optimized copy of mrk_strainmarker (plus more)
MRK = None              # temp table with markers that have data we need
ACC = None              # optimized data from acc_accesion

###--- Functions ---###

def buildTempTable (name, buildCmd, indexCmds = []):
        # build the temp table with the given 'name' using the given 'buildCmd', then index it using
        # the given list of 'indexCmds'

        logger.debug('Building %s' % name)
        dbAgnostic.execute(buildCmd)
        logger.debug(' - populated')
        
        for cmd in indexCmds:
                dbAgnostic.execute(cmd)
        logger.debug(' - indexed')
        return name
        
def getSequenceMarkerTable():
        # SEQ_Marker_Cache is clustered by the cache key (boooo).  Let's make an optimized copy for
        # our purposes and clustered as we need.  Note that we are joining to the marker and sequence
        # tables just to ensure that the keys are valid.  Returns the table name.
    
    global SMC
    if SMC != None:         # if we already created the table, just return its name
        return SMC
    
    SMC = 'faux_seq_marker_cache'
    
    smc0 = '''select smc._Marker_key, smc._Sequence_key, smc._Refs_key, smc._Qualifier_key
                into temp table %s
                from seq_marker_cache smc, mrk_marker m, seq_sequence s
                where smc._Marker_key = m._Marker_key
                        and smc._Sequence_key = s._Sequence_key
                order by smc._Marker_key''' % SMC
        
    smc1 = 'create index smc1 on %s(_Marker_key)' % SMC
    smc2 = 'create index smc2 on %s(_Sequence_key)' % SMC
    
    return buildTempTable(SMC, smc0, [ smc1, smc2 ])

def getStrainMarkerToSequenceTable():
        # Need to build a temp table mapping strain marker keys to sequence keys via matching ID.
        # Using seq_genemodel to narrow the set of sequence IDs we need to consider (for performance).
    # Returns the table name.
    
    global ACC
    if ACC != None:
        return ACC      # if we already created the table, just return its name
    ACC = 'faux_acc_accession'
        
    acc0 = '''select msm._Object_key as _StrainMarker_key, seq._Object_key as _Sequence_key,
                        msm._Accession_key
                into temp table %s
                from acc_accession msm, acc_accession seq, seq_genemodel gm
                where msm._MGIType_key = 44
                        and gm._Sequence_key = seq._Object_key
                        and seq._MGIType_key = 19
                        and msm.accID = seq.accID''' % ACC
                        
    acc1 = 'create index acc1 on %s(_StrainMarker_key)' % ACC
    acc2 = 'create index acc2 on %s(_Sequence_key)' % ACC
    acc3 = 'create index acc3 on %s(_Accession_key)' % ACC
        
    return buildTempTable(ACC, acc0, [ acc1, acc2, acc3 ])

def getStrainMarkers():
        # MRK_StrainMarker doesn't have everything we need, so let's make a customized, optimized copy.
    # The new table will relate canonical markers with the gene model sequences for the strain markers.
    # Returns the table name. 
        
    global MSM
    if MSM != None:         # if already generated, return existing table
        return MSM
    MSM = 'faux_mrk_strainmarker'
        
    msm0 = '''select msm._Marker_key, acc._Sequence_key, r._Refs_key, 615422 as _Qualifier_key
                into temp table %s
                from mrk_strainmarker msm
                inner join mrk_marker mm on (msm._Marker_key = mm._Marker_key)
                inner join %s acc on (msm._StrainMarker_key = acc._StrainMarker_key)
                inner join seq_sequence ss on (acc._Sequence_key = ss._Sequence_key)
                left outer join acc_accessionreference r on (acc._Accession_key = r._Accession_key)
                order by msm._Marker_key''' % (MSM, getStrainMarkerToSequenceTable())
        
    msm1 = 'create index msm1 on %s(_Marker_key)' % MSM
    msm2 = 'create index msm2 on %s(_Sequence_key)' % MSM
        
    buildTempTable(MSM, msm0, [ msm1, msm2 ])
    return MSM

def getMarkersWithSequences():
    # build a temp table of markers that have either traditional sequence data or have strain makrkers
    # with gene model sequences.  Returns the name of the table.  Table contains a row_num column for
    # easy iteration over the set of markers.
    
    global MRK
    if MRK != None:         # if already generated, just return table name
        return MRK

    MRK = 'faux_mrk_marker'                             # temp table with markers that have data we need
    
        # MRK_Marker has way more data than we need, including non-mouse markers and mouse markers with no
        # sequences, so let's strip it down to only what we need and add an ID column that we can use to
        # efficiently walk through the set of markers in step with our new, ordered temp tables.
        
    mrk0 = '''select row_number() over (order by m._Marker_key) as row_num, m._Marker_key
                into temp table %s
                from mrk_marker m
                where exists (select 1 from %s a where m._Marker_key = a._Marker_key)
                        or exists (select 1 from %s b where m._Marker_key = b._Marker_key)''' % (MRK,
                getSequenceMarkerTable(), getStrainMarkers())

    mrk1 = 'create index mrk1 on %s(row_num)' % MRK
    mrk2 = 'create unique index mrk2 on %s(_Marker_key)' % MRK

    return buildTempTable(MRK, mrk0, [ mrk1, mrk2 ])
