# Module: GOFilter.py
# Purpose: to provide an easy means to determine which GO annotations should
#       be included (and counted) in the front-end database, and which should
#       be omitted.  The basic rules are:
#       1. All annotations should be included if they have an evidence code
#               other than ND (No Data).
#       2. Annotations with an ND evidence code should only be included if
#               there are no other annotations to that GO DAG (process,
#               function, component) for that marker.
#
# We also now need to (optionally) be able to filter out ALL ND (No Data)
# annotations.  This will initially be used to compute a count of non-ND
# annotations for the marker detail page.

import dbAgnostic
import logger
import gc

###--- Globals ---###

# exception raised by this module
error = 'GOFilter.error'

# keep annotations with a NOT qualifier?
KEEP_NOTS = True

# remove all ND (No Data) annotations, not only those specified by the two
# rules specified at the top of this library.
REMOVE_ALL_ND = False

# temp table for annotations which pass filtering
KEEPER_TABLE = 'keepers'
BUILT_KEEPER_TABLE = False

# temp table for distinct GO annotations which can be counted per marker
BY_MARKER_TABLE = 'countable_by_marker'
BUILT_BY_MARKER_TABLE = False

# temp table for distinct GO annotations which can be counter per reference
BY_REFERENCE_TABLE = 'countable_by_reference'
BUILT_BY_REFERENCE_TABLE = False

# annotation count per marker
BY_MARKER= None

# annotation count per reference
BY_REFERENCE = None

# dictionary of annotation keys to keep
KEEPERS = None

###--- Private Functions for Computing in Database ---###

def _buildKeeperTable():
        global BUILT_KEEPER_TABLE

        if BUILT_KEEPER_TABLE:
                return

        # first, we populate the keepers table with all GO annotations that
        # do not have a "No Data" (ND) evidence code.  And index it.

        cmd0 = '''select distinct a._Object_key as _Marker_key,
                        a._Term_key, n._DAG_key, a._Annot_key,
                        a._Qualifier_key, e.inferredFrom, e._EvidenceTerm_key,
                        e._Refs_key
                into temporary table %s
                from voc_annot a, voc_evidence e, dag_node n
                where a._AnnotType_key = 1000
                        and a._Annot_key = e._Annot_key
                        and a._Term_key = n._Object_key
                        and n._DAG_key in (1,2,3)
                        and e._EvidenceTerm_key != 118''' % KEEPER_TABLE

        cmd1 = 'create index mkey on %s(_Marker_key)' % KEEPER_TABLE
        cmd2 = 'create index dkey on %s(_DAG_key)' % KEEPER_TABLE

        for cmd in [ cmd0, cmd1, cmd2 ]:
                dbAgnostic.execute(cmd)

        cmd3 = 'select count(1) from %s' % KEEPER_TABLE
        cols, rows = dbAgnostic.execute(cmd3)

        logger.debug('Added %d rows to %s' % (rows[0][0], KEEPER_TABLE))

        if REMOVE_ALL_ND:
            logger.debug('Skipping all ND annotations')

        else:
            # second, gather all the annotations with "No Data" (ND) evidence
            # codes and put them in a temp table, then index it.

            ndTable = 'go_nd_annotations'

            cmd4 = '''select distinct a._Object_key as _Marker_key,
                        a._Term_key, n._DAG_key, a._Annot_key,
                        a._Qualifier_key, e.inferredFrom, e._EvidenceTerm_key,
                        e._Refs_key
                into temporary table %s
                from voc_annot a, voc_evidence e, dag_node n
                where a._AnnotType_key = 1000
                        and a._Annot_key = e._Annot_key
                        and a._Term_key = n._Object_key
                        and n._DAG_key in (1,2,3)
                        and e._EvidenceTerm_key = 118''' % ndTable

            cmd5 = 'create index mkey2 on %s(_Marker_key)' % ndTable
            cmd6 = 'create index dkey2 on %s(_DAG_key)' % ndTable

            for cmd in [ cmd4, cmd5, cmd6 ]:
                dbAgnostic.execute(cmd)

            cmd7 = 'select count(1) from %s' % ndTable
            cols, rows = dbAgnostic.execute(cmd7)

            logger.debug('Added %d rows to %s' % (rows[0][0], ndTable))

            # break down the counts for the "No Data" (ND) annotations (solely
            # for reporting purposes)

            cmd8 = '''select count(1)
                from %s g
                where exists (select 1 from %s k
                        where g._Marker_key = k._Marker_key
                        and g._DAG_key = k._DAG_key)''' % (
                                ndTable, KEEPER_TABLE)

            cols, rows = dbAgnostic.execute(cmd8)
            logger.debug('Found %d ND rows to skip' % rows[0][0])

            cmd9 = '''select count(1)
                from %s g
                where not exists (select 1 from %s k
                        where g._Marker_key = k._Marker_key
                        and g._DAG_key = k._DAG_key)''' % (
                                ndTable, KEEPER_TABLE)

            cols, rows = dbAgnostic.execute(cmd9)
            logger.debug('Found %d ND rows to keep' % rows[0][0])

            # now add the ND rows we want to keep into the keepers table

            cmd10 = '''insert into %s
                select *
                from %s g
                where not exists (select 1 from %s k
                        where g._Marker_key = k._Marker_key
                        and g._DAG_key = k._DAG_key)''' % (KEEPER_TABLE,
                                ndTable, KEEPER_TABLE)

            cmd11 = 'drop table %s' % ndTable

            for cmd in [ cmd10, cmd11 ]:
                dbAgnostic.execute(cmd)

            dbAgnostic.commit()

        cmd12 = 'select count(1) from %s' % KEEPER_TABLE

        cols, rows = dbAgnostic.execute(cmd12)

        keeperTotal = rows[0][0]
        logger.debug('Total of %d GO annotations kept in %s' % (
                keeperTotal, KEEPER_TABLE))

        # if we want to exclude annotations with a NOT qualifier, remove those
        # with terms starting with "NOT"

        if not KEEP_NOTS:
                cmd13 = '''delete from %s
                        where _Qualifier_key in (select _Term_key
                                from voc_term
                                where _Vocab_key = 52
                                        and term like 'NOT%%')''' % \
                                                KEEPER_TABLE
                dbAgnostic.execute(cmd13)

                cmd14 = 'select count(1) from %s' % KEEPER_TABLE
                cols, rows = dbAgnostic.execute(cmd14)

                logger.debug('Removed %d rows with NOT qualifiers' % 
                        (keeperTotal - rows[0][0]))

                dbAgnostic.commit()

        del cols, rows
        gc.collect()

        BUILT_KEEPER_TABLE = True
        return

def _buildKeepersDictionary():
        # pull into memory the annotation keys for annotations which pass the
        # ND rules

        global KEEPERS

        if KEEPERS != None:
                return

        _buildKeeperTable()

        cmd = 'select _Annot_key from %s' % KEEPER_TABLE
        cols, rows = dbAgnostic.execute(cmd)

        KEEPERS = {}
        for row in rows:
                KEEPERS[row[0]] = 1
        
        del rows
        gc.collect()

        logger.debug('Brought %d keeper annotations into memory' % \
                len(KEEPERS))
        return

def _buildByMarkerTable():
        # build the temporary table for countable annotations per marker.
        # A unique annotation is defined by the combination of marker key,
        # term, qualifier, evidence code, and inferred-from value.

        global BUILT_BY_MARKER_TABLE

        if BUILT_BY_MARKER_TABLE:
                return

        _buildKeeperTable()

        cmd0 = '''select distinct _Marker_key, _Term_key, _Qualifier_key,
                        _EvidenceTerm_key, inferredFrom
                into temporary table %s
                from %s''' % (BY_MARKER_TABLE, KEEPER_TABLE)
        dbAgnostic.execute(cmd0)

        cmd1 = 'select count(1) from %s' % BY_MARKER_TABLE
        cols, rows = dbAgnostic.execute(cmd1)

        logger.debug('Found %d distinct annotations to count for markers' % \
                rows[0][0])
        BUILT_BY_MARKER_TABLE = True
        return

def _buildByMarkerDictionary():
        # pull the marker counts into an in-memory dictionary

        global BY_MARKER

        if BY_MARKER != None:
                return

        _buildByMarkerTable()

        cmd2 = '''select _Marker_key, count(1) as annot_count
                from %s
                group by _Marker_key''' % BY_MARKER_TABLE
        cols, rows = dbAgnostic.execute(cmd2)

        keyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
        countCol = dbAgnostic.columnNumber(cols, 'annot_count')

        BY_MARKER = {}
        for row in rows:
                BY_MARKER[row[keyCol]] = row[countCol]

        del rows
        gc.collect()
        logger.debug('Got counts for %d markers' % len(BY_MARKER))
        return

def _buildByReferenceTable():
        # build the temporary table for countable annotations per reference.
        # A unique annotation is defined by the combination of marker key,
        # term, qualifier, evidence code, and inferred-from value.

        global BUILT_BY_REFERENCE_TABLE

        if BUILT_BY_REFERENCE_TABLE:
                return

        _buildKeeperTable()

        cmd0 = '''select distinct _Marker_key, _Term_key, _Qualifier_key,
                        _EvidenceTerm_key, inferredFrom, _Refs_key
                into temporary table %s
                from %s''' % (BY_REFERENCE_TABLE, KEEPER_TABLE)
        dbAgnostic.execute(cmd0)

        cmd1 = 'select count(1) from %s' % BY_REFERENCE_TABLE
        cols, rows = dbAgnostic.execute(cmd1)

        logger.debug('Found %d distinct annotations to count for references' \
                % rows[0][0])

        BUILT_BY_REFERENCE_TABLE = True
        return

def _buildByReferenceDictionary():
        global BY_REFERENCE

        if BY_REFERENCE != None:
                return

        _buildByReferenceTable()

        cmd2 = '''select _Refs_key, count(1) as annot_count
                from %s
                group by _Refs_key''' % BY_REFERENCE_TABLE
        cols, rows = dbAgnostic.execute(cmd2)

        keyCol = dbAgnostic.columnNumber(cols, '_Refs_key')
        countCol = dbAgnostic.columnNumber(cols, 'annot_count')

        BY_REFERENCE = {}
        for row in rows:
                BY_REFERENCE[row[keyCol]] = row[countCol]

        del rows
        gc.collect()
        logger.debug('Got counts for %d references' % len(BY_REFERENCE))
        return

###--- Functions ---###

def keepNots(flag = True):
        # tell this module whether to keep annotations with a NOT qualifier
        # (True) or not (False).  Must be called before this module does any
        # other work.

        global KEEP_NOTS
        if BUILT_KEEPER_TABLE:
                raise Exception('%s: Call keepNots() before other GOFilter functions.' % error)

        KEEP_NOTS = flag
        logger.debug('Set KEEP_NOTS flag to %s' % str(flag))
        return

def removeAllND(flag = True):
        # tell this module whether to remove all ND (No Data) annotations
        # (True) or to only remove those where there is a non-ND annotation
        # within the same DAG for the same marker (False).  Must be called
        # before this module does any other work.

        global REMOVE_ALL_ND
        if BUILT_KEEPER_TABLE:
                raise error('Call removeAllND() before other GOFilter functions.')

        REMOVE_ALL_ND = flag
        logger.debug('Set REMOVE_ALL_ND flag to %s' % str(flag))
        return

def getAnnotationCount (markerKey):
        # get a count of GO annotations for the given 'markerKey'

        _buildByMarkerTable()

        if markerKey in BY_MARKER:
                return BY_MARKER[markerKey]
        return 0
        
def getAnnotationCountForRef (refsKey):
        # get a count of GO annotations for the given 'refsKey'

        _buildByReferenceTable()

        if refsKey in BY_REFERENCE:
                return BY_REFERENCE[refsKey]
        return 0
        
def shouldInclude (annotKey):
        # determine if the annotation with this 'annotKey' should be included
        # in the front-end database (True) or not (False)

        _buildKeepersDictionary()

        if annotKey in KEEPERS:
                return True
        return False

def getMarkerKeys ():
        # return the list of marker keys which have GO annotations

        _buildByMarkerDictionary()

        keys = list(BY_MARKER.keys())
        keys.sort()

        return keys

def getReferenceKeys ():
        # return the list of reference keys which have GO annotations

        _buildByReferenceDictionary()

        keys = list(BY_REFERENCE.keys())
        keys.sort()

        return keys

def getKeeperAnnotationTable():
        # returns the name of a pre-computed temp table that contains
        # annotations that pass the ND test.  The temp table will contain
        # columns:  _Marker_key, _DAG_key, _Term_key, _Qualifier_key,
        #    _EvidenceTerm_key, inferredFrom

        _buildKeeperTable()
        return KEEPER_TABLE

def getCountableAnnotationTableByMarker():
        # return the name of a pre-computed temp table that contains distinct
        # annotations for each marker.  Fields in the table include:
        #    _Marker_key, _Term_key, _Qualifier_key, _EvidenceTerm_key,
        #    inferredFrom

        _buildByMarkerTable()
        return BY_MARKER_TABLE

def getCountableAnnotationTableByReference():
        # return the name of a pre-computed temp table that contains distinct
        # annotations for each marker, including the reference for each.
        # Fields in the table include:
        #    _Marker_key, _Term_key, _Qualifier_key, _EvidenceTerm_key,
        #    inferredFrom, _Refs_key

        _buildByReferenceTable()
        return BY_REFERENCE_TABLE
