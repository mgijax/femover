#!./python
# 
# gathers data for the 'markerID' table in the front-end database

import Gatherer
import logger
import gc

###--- Globals ---###

# logical database keys for IDs which should not appear in the 'Other IDs'
# section of the marker detail page
EXCLUDE_FROM_OTHER_DBS = [ 1, 8, 9, 13, 23, 27, 28, 35, 36, 41, 45,
                53, 55, 59, 60, 81, 85, 86, 88, 95, 96, 97, 98, 99,
                100, 101, 102, 103, 104, 122, 123, 131, 132, 133,
                134, 135, 136, 139, 140, 141, 144, 164 ]

LDB_ORDER = [ 85, 60, 55, 23, 35, 36, 53, 8, 45, 126 ]

###--- Functions ---###

ldbKeyCol = None
ldbNameCol = None
idCol = None

def ldbSortKey (a):
        # return a tuple for sorting logical database info in 'a', where we return
        # a tuple like (preferred ldb order, ldb name, accession ID)
        aKey = a[ldbKeyCol]

        if aKey in LDB_ORDER:
                return (LDB_ORDER.index(aKey), a[ldbNameCol], a[idCol])

        # not a preferred ID, so sort it after all the preferred ones
        return (99, a[ldbNameCol], a[idCol])

markerIdKey = 0
def nextKey():
        global markerIdKey

        markerIdKey = markerIdKey + 1
        return markerIdKey

markerIdKeys = {}       # (marker key, logical db, ID) -> marker ID key
def getMarkerIdKey (markerKey, ldbKey, accID):
        # the unique ID for this record for the marker ID table

        global markerIdKeys

        key = (markerKey, ldbKey, accID)
        if key not in markerIdKeys:
                markerIdKeys[key] = nextKey()
        return markerIdKeys[key] 

def resetMarkerIdKeys():
        # can reset the cache of keys to save memory from chunk to chunk
        global markerIdKeys

        markerIdKeys = {}
        return

###--- Classes ---###

i = 0           # global sequence num

class MarkerIDGatherer (Gatherer.CachingMultiFileGatherer):
        # Is: a data gatherer for the markerID table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for marker IDs,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # slice and dice the query results to produce our set of
                # final results

                global ldbKeyCol, ldbNameCol, idCol, i

                resetMarkerIdKeys()

                # first, gather all the IDs by marker

                ids = {}        # marker key -> ID rows
                cols, rows = self.results[0]

                ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
                ldbNameCol = Gatherer.columnNumber (cols, 'logicalDB')
                idCol = Gatherer.columnNumber (cols, 'accID')
                keyCol = Gatherer.columnNumber (cols, 'markerKey')
                preferredCol = Gatherer.columnNumber (cols, 'preferred')
                privateCol = Gatherer.columnNumber (cols, 'private')

                for row in rows:
                        key = row[keyCol]
                        if key in ids:
                                ids[key].append (row)
                        else:
                                ids[key] = [ row ]

                logger.debug ('Collected IDs for %d markers' % len(ids))

                # grab the non-mouse markers' HomoloGene IDs

                cols, rows = self.results[2]

                ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
                ldbNameCol = Gatherer.columnNumber (cols, 'logicalDB')
                idCol = Gatherer.columnNumber (cols, 'accID')
                keyCol = Gatherer.columnNumber (cols, 'markerKey')
                preferredCol = Gatherer.columnNumber (cols, 'preferred')
                privateCol = Gatherer.columnNumber (cols, 'private')

                for row in rows:
                        key = row[keyCol]
                        if key in ids:
                                ids[key].append (row)
                        else:
                                ids[key] = [ row ]

                logger.debug ('Added %d HomoloGene IDs from clusters' % \
                        len(rows))

                # proceed with processing...

                markerKeys = list(ids.keys())
                markerKeys.sort()

                logger.debug ('Sorted %d marker keys' % len(markerKeys))

                for key in markerKeys:
                        ids[key].sort(key=ldbSortKey)

                logger.debug ('Sorted IDs for %d marker keys' % len(ids))

                # now compile our IDs into our set of final results

                seenIdKeys = {}

                for key in markerKeys:
                        for r in ids[key]:
                                markerIdKey = getMarkerIdKey (key,
                                        r[ldbKeyCol], r[idCol])

                                # ensure we have no duplicate IDs (skip any
                                # duplicates)

                                if markerIdKey in seenIdKeys:
                                        continue
                                seenIdKeys[markerIdKey] = 1

                                i = i + 1
                                if r[ldbKeyCol] in EXCLUDE_FROM_OTHER_DBS:
                                        otherDB = 0
                                elif r[privateCol] == 1:
                                        otherDB = 0
                                else:
                                        otherDB = 1

                                self.addRow('marker_id', [ markerIdKey,
                                        key,
                                        r[ldbNameCol],
                                        r[idCol],
                                        r[preferredCol],
                                        r[privateCol],
                                        otherDB,
                                        i
                                        ] )
                logger.debug ('Got %d marker ID rows' % i)

                # handle query 1 -- shared gene model IDs

                cols, rows = self.results[1]

                idCol = Gatherer.columnNumber (cols, 'accID')
                ldbCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')
                baseMarkerKeyCol = Gatherer.columnNumber (cols,
                        'baseMarkerKey')
                symbolCol = Gatherer.columnNumber (cols, 'symbol')
                markerIDCol = Gatherer.columnNumber (cols, 'markerID')

                r = 0
                for row in rows:
                        markerIdKey = getMarkerIdKey (row[baseMarkerKeyCol],
                                row[ldbCol], row[idCol])

                        r = r + 1
                        self.addRow('marker_id_other_marker', [ markerIdKey,
                                row[keyCol], row[symbolCol], row[markerIDCol]
                                ] )

                logger.debug ('Got %d shared GM ID rows' % r)
                gc.collect()
                return

###--- globals ---###

cmds = [
        # 0. all IDs for each marker
        '''select a._Object_key as markerKey, a._LogicalDB_key,
                a.accID, a.preferred, a.private, ldb.name as logicalDB
        from acc_accession a, acc_logicaldb ldb
        where a._MGIType_key = 2
                and exists (select 1 from mrk_marker m
                        where a._Object_key = m._Marker_key)
                and a._Object_key >= %d
                and a._Object_key < %d
                and a._LogicalDB_key = ldb._LogicalDB_key
        order by 1, 2, 3''',

        # 1. some gene model IDs are shared by multiple markers; we need to
        # look up the markers sharing each gene model ID
        '''select a.accID, a._LogicalDB_key, m._Marker_key, m.symbol,
                c.accID as markerID, a._Object_key as baseMarkerKey
        from acc_accession a, acc_accession b, mrk_marker m, acc_accession c
        where a._LogicalDB_key in (85, 60, 55)
                and a.accID = b.accID
                and a._LogicalDB_key = b._LogicalDB_key
                and a._MGIType_key = 2
                and b._MGIType_key = 2
                and a._Object_key != b._Object_key
                and b._Object_key = m._Marker_key
                and m._Marker_key = c._Object_key
                and a._Object_key >= %d
                and a._Object_key < %d
                and exists (select 1 from mrk_marker m2
                        where m2._Marker_key = a._Object_key)
                and c._MGIType_key = 2
                and c._LogicalDB_key = 1
                and c.preferred = 1''',

        # 2. we need to bring HomoloGene IDs over for non-mouse markers,
        # getting them from the homology clusters involving those markers
        '''select mm._Marker_key as markerKey, aa._LogicalDB_key, aa.accID,
                aa.preferred, aa.private, ldb.name as logicalDB
        from voc_term vt, mrk_cluster mc, mrk_clustermember mcm,
                mrk_marker mm, acc_accession aa, acc_logicaldb ldb
        where vt.term = 'HomoloGene'
                and vt._Term_key = mc._ClusterSource_key
                and mc._Cluster_key = mcm._Cluster_key
                and mcm._Marker_key = mm._Marker_key
                and mm._Organism_key != 1
                and mc._Cluster_key = aa._Object_key
                and aa._MGIType_key = 39
                and mm._Marker_key >= %d
                and mm._Marker_key < %d
                and aa._LogicalDB_key = ldb._LogicalDB_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
files = [ ('marker_id',
                [ 'markerIdKey', 'markerKey', 'logicalDB',
                        'accID', 'preferred', 'private',
                        'isForOtherDbSection', 'sequence_num' ],
                [ 'markerIdKey', 'markerKey', 'logicalDB', 'accID',
                        'preferred', 'private', 'isForOtherDbSection',
                        'sequence_num' ] ),

        ('marker_id_other_marker',
                [ 'markerIdKey', 'markerKey', 'symbol', 'accID' ], 
                [ Gatherer.AUTO, 'markerIdKey', 'markerKey', 'symbol',
                        'accID' ] )
        ]

# global instance of a markerIDGatherer
gatherer = MarkerIDGatherer (files, cmds)
gatherer.setupChunking (
        'select min(_Marker_key) from MRK_Marker',
        'select max(_Marker_key) from MRK_Marker',
        10000
        )

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
