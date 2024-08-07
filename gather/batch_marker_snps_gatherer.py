#!./python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database.
# This table stores the SNP IDs that will be shown in the batch query output
# for each marker (when SNPs are requested).

import Gatherer
import config
import dbAgnostic
import logger

###--- Classes ---###

class BatchMarkerSnpsGatherer (Gatherer.CachingMultiFileGatherer):
        def collateResults (self):
                # produce the rows of results for the current batch of SNPs
                
                cols0, snpKeys = self.results[0]
                keyCol0 = Gatherer.columnNumber(cols0, '_ConsensusSNP_key')

                cols1, snpSingleLoc = self.results[1]
                keyCol1 = Gatherer.columnNumber(cols1, '_ConsensusSNP_key')

                cols2, snpMarkerKeys = self.results[2]
                keyCol2 = Gatherer.columnNumber(cols2, '_ConsensusSNP_key')
                mkeyCol2 = Gatherer.columnNumber(cols2, '_marker_key')

                cols3, snpRsIds = self.results[3]
                keyCol3 = Gatherer.columnNumber(cols3, '_ConsensusSNP_key')
                accCol3 = Gatherer.columnNumber(cols3, 'accid')

                rows = []
                i0 = i1 = i2 = i3 = 0
                while i0 < len(snpKeys):
                    csk = snpKeys[i0][keyCol0]
                    (i1,singleLocs) = self.findK(csk, snpSingleLoc, i1, keyCol1)
                    (i2,snpMarkers) = self.findK(csk, snpMarkerKeys, i2, keyCol2)
                    (i3,snpIds) = self.findK(csk, snpRsIds, i3, keyCol3)
                    if len(singleLocs) > 0 and len(snpMarkers) > 0 and len(snpIds) > 0:
                        snpId = snpIds[0][accCol3]
                        for r in snpMarkers:
                            rows.append([r[mkeyCol2], snpId])
                    i0 += 1
                self.addRows('batch_marker_snps', rows)
                return

        def findK (self, k, recs, i, keyCol) :
            ans = []
            while i < len(recs) and recs[i][keyCol] < k:
                i += 1
            while i < len(recs) and recs[i][keyCol] == k:
                ans.append(recs[i])
                i += 1
            return (i, ans)

###--- Setup for Gatherer ---###

#
# For performance reasons, the queries for this gatherer are unusual in that there are no joins.
# Instead, simple selection queries are done on each table, the results are ordered by key.
# Joining is done in the gatherer code which processes the sorted streams in parallel.
# The performance is dramatically better (45 min vs 14 hours) than the previous queries (or any variants
# that I could come up with) involving joins
#

cmds = [
    # query 0: consensus snp keys
    '''select _ConsensusSNP_key
        from snp.snp_consensussnp
        where _ConsensusSNP_key >= %d
        and _ConsensusSNP_key < %d
        and _VarClass_key = 1878510
        order by _ConsensusSNP_key
    ''' ,

    # query 1: keys of consensus snp with single locations
    ''' 
        select _ConsensusSNP_key
        from snp.snp_coord_cache
        where _ConsensusSNP_key >=  %d
        and _ConsensusSNP_key < %d
        and isMultiCoord = 0
        order by _ConsensusSNP_key
    ''' ,

    # query 2: snp/markers key pairs where distance < 2kb
    ''' 
        select _ConsensusSNP_key, _marker_key
        from snp.snp_consensussnp_marker
        where _ConsensusSNP_key >=  %d
        and _ConsensusSNP_key < %d
        order by _ConsensusSNP_key
    ''' ,

    # query 3: rsIDs
    ''' select _object_key as _ConsensusSNP_key, accid
        from snp.snp_accession
        where _object_key >=  %d
        and _object_key < %d
        and _mgitype_key = 30
        order by _object_key
    ''' ,

    ]

files = [
        ('batch_marker_snps',
                [ '_Marker_key', 'accID' ],
                [ Gatherer.AUTO, '_Marker_key', 'accID' ],
                ),
        ]

gatherer = BatchMarkerSnpsGatherer (files, cmds)
gatherer.setupChunking (
        'select min(_ConsensusSNP_key) from snp_consensussnp_marker',
        'select max(_ConsensusSNP_key) from snp_consensussnp_marker',
        1000000
        )

###--- main program ---###

if __name__ == '__main__':
        Gatherer.main(gatherer)
