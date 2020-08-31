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
                
                (cols, rows) = self.results[-1]
                self.addRows('batch_marker_snps', rows)
                return

###--- Setup for Gatherer ---###

tempTable = 'snp_batch'

cmds = [
        # 0 drop any existing temp table
        'drop table if exists %s' % tempTable,
        
        # 1. SNP/marker pairs for the batch that are within the appropriate distance
        '''select distinct s._ConsensusSNP_key, s._Marker_key
                into temp table %s
                from snp_consensussnp_marker s, mrk_marker m
                where s._ConsensusSNP_key >= %%d
                        and s._ConsensusSNP_key < %%d
                        and s.distance_from <= 2000
                        and s._Marker_key = m._Marker_key
        ''' % tempTable, 

        # 2. index the temp table
        'create index %s_snp_index on %s (_ConsensusSNP_key)' % (tempTable, tempTable),
        
        # 3. delete SNPs that have the wrong variation class
        '''delete from %s
                using snp_consensussnp t
                where t._VarClass_key != 1878510
                        and %s._ConsensusSNP_key = t._ConsensusSNP_key
        ''' % (tempTable, tempTable),
        
        # 4. delete SNPs that have multiple coordinates
        '''delete from %s
                using snp_coord_cache s
                where s.isMultiCoord = 1
                        and %s._ConsensusSNP_key = s._ConsensusSNP_key
                ''' % (tempTable, tempTable),

        # 5. get SNP/marker pairs
        '''select distinct t._Marker_key, a.accID
                from %s t, snp_accession a
                where t._ConsensusSNP_key = a._Object_key
                        and a._MGIType_key = 30         -- consensus SNP
                ''' % tempTable,
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
