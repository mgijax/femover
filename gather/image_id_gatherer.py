#!./python
# 
# gathers data for the 'image_id' table in the front-end database
#

import Gatherer
import logger
import re

###--- Globals ---###

EXCLUDE_FROM_OTHER_DBS = [ 1, 19, 163 ]
LDB_ORDER = [ 159, 105 ]

###--- Functions ---###

ldbKeyCol = None
ldbNameCol = None
idCol = None

def ldbCompare (a):
        # preferred LDBs before all non-preferred ones, then by LDB name, then by ID

        prefLdb = 999
        if a[ldbKeyCol] in LDB_ORDER:
                prefLdb = LDB_ORDER.index(a[ldbKeyCol])

        return (prefLdb, a[ldbNameCol], a[idCol])

imageIdKeys = {}        # (image key, logical db, ID) -> image ID key
def getImageIdKey (imageKey, ldbKey, accID):
        # the unique ID for this record for the image ID table

        global imageIdKeys

        key = (imageKey, ldbKey, accID)
        if key not in imageIdKeys:
                imageIdKeys[key] = len(imageIdKeys) + 1
        return imageIdKeys[key] 

gpRE = re.compile('^([A-Za-z]+),-A([0-9]+),-A([A-Za-z]+),?-?A?([0-9]+)?:?$')
def tweakID (ldb, accID):
        # Some image IDs (currently GenePaint) require us to slice and dice
        # the ID to repackage it for a new link.  (These aren't actually 
        # IDs anyway, but rather URL parameters.)  Need to handle these formats:
        #       MH,-A1112,-Asetstart,-A19:              (notice trailing colon)
        #       DA,-A75,-Asetstart,-A15
        #       FG,-A46,-Asetview
        
        if (ldb == 'GenePaint'):
                match = gpRE.match(accID)
                if match:
                        (idPrefix, idSuffix, linkType, pane) = match.groups()
                        if pane != None:
                                accID = '%s%s/%s' % (idPrefix, idSuffix, int(pane) - 1)
                        else:
                                accID = '%s%s' % (idPrefix, idSuffix)

        return accID

###--- Classes ---###

class ImageIDGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the image ID table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for image IDs,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # slice and dice the query results to produce our set of
                # final results

                global ldbKeyCol, ldbNameCol, idCol

                # first, gather all the IDs by image

                ids = {}        # image key -> ID rows
                cols, rows = self.results[0]

                ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
                ldbNameCol = Gatherer.columnNumber (cols, 'logicalDB')
                idCol = Gatherer.columnNumber (cols, 'accID')
                keyCol = Gatherer.columnNumber (cols, 'imageKey')
                preferredCol = Gatherer.columnNumber (cols, 'preferred')
                privateCol = Gatherer.columnNumber (cols, 'private')

                for row in rows:
                        key = row[keyCol]
                        if key in ids:
                                ids[key].append (row)
                        else:
                                ids[key] = [ row ]

                logger.debug ('Collected IDs for %d images' % len(ids))

                imageKeys = list(ids.keys())
                imageKeys.sort()

                logger.debug ('Sorted %d image keys' % len(imageKeys))

                for key in imageKeys:
                        ids[key].sort(key=ldbCompare)

                logger.debug ('Sorted IDs for %d image keys' % len(ids))

                # now compile our IDs into our set of final results

                idResults = []
                idColumns = [ 'imageIdKey', 'imageKey', 'logicalDB',
                        'accID', 'preferred', 'private',
                        'isForOtherDbSection', 'sequence_num' ]

                i = 0
                seenIdKeys = {}

                for key in imageKeys:
                        for r in ids[key]:
                                imageIdKey = getImageIdKey (key,
                                        r[ldbKeyCol], r[idCol])

                                # ensure we have no duplicate IDs (skip any
                                # duplicates)

                                if imageIdKey in seenIdKeys:
                                        continue
                                seenIdKeys[imageIdKey] = 1

                                i = i + 1
                                if r[ldbKeyCol] in EXCLUDE_FROM_OTHER_DBS:
                                        otherDB = 0
                                elif r[privateCol] == 1:
                                        otherDB = 0
                                else:
                                        otherDB = 1

                                idResults.append ( [ imageIdKey,
                                        key,
                                        r[ldbNameCol],
                                        tweakID(r[ldbNameCol], r[idCol]),
                                        r[preferredCol],
                                        r[privateCol],
                                        otherDB,
                                        i
                                        ] )

                self.finalColumns = idColumns
                self.finalResults = idResults
                logger.debug ('Got %d image ID rows' % len(idResults))
                return

###--- globals ---###

cmds = [
        # 0. all IDs for each image
        '''select a._Object_key as imageKey, a._LogicalDB_key,
                a.accID, a.preferred, a.private, ldb.name as logicalDB
        from acc_accession a, acc_logicaldb ldb
        where a._MGIType_key = 9
                and exists (select 1 from img_image i
                        where i._Image_key = a._Object_key)
                and a._LogicalDB_key = ldb._LogicalDB_key''',
        ]

fieldOrder = [ 'imageIdKey', 'imageKey', 'logicalDB', 'accID',
                        'preferred', 'private', 'isForOtherDbSection',
                        'sequence_num' ]

filenamePrefix = 'image_id'

# global instance of a ImageIDGatherer
gatherer = ImageIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
