#!./python
# 
# gathers data for the 'expression_ht_consolidated_sample_measurement' table in the front-end database

import Gatherer
import utils
import logger
import dbAgnostic
import gc

###--- Globals ---###

LEVELS = {}                     # level key : term

###--- Functions ---###

def initialize():
        # initialize this gatherer by populating global caches
        global LEVELS
        
        levelQuery = '''select distinct t._Term_key, t.term
                from voc_term t
                where t._Vocab_key = 144'''
        utils.fillDictionary('levels', levelQuery, LEVELS, '_Term_key', 'term')
        return

###--- Classes ---###

class EHCSMGatherer (Gatherer.CachingMultiFileGatherer):
        # Is: a data gatherer for the expression_ht_consolidated_sample_measurement table
        # Has: queries to execute against the source database
        # Does: queries the source database for measurements for consolidated RNA-Seq samples,
        #       collates results, writes tab-delimited text file

        def collateResults(self):
                cols, rows = self.results[0]
                uniqueKeyCol = dbAgnostic.columnNumber(cols, '_RNASeqCombined_key')
                seqSetKeyCol = dbAgnostic.columnNumber(cols, '_RNASeqSet_key')
                markerKeyCol = dbAgnostic.columnNumber(cols, '_Marker_key')
                levelKeyCol = dbAgnostic.columnNumber(cols, '_Level_key')
                replicatesCol = dbAgnostic.columnNumber(cols, 'numberOfBiologicalReplicates')
                qnTpmCol = dbAgnostic.columnNumber(cols, 'averageQuantileNormalizedTPM')
                
                for row in rows:
                        markerKey = row[markerKeyCol]
                        
                        self.addRow('expression_ht_consolidated_sample_measurement', [ row[uniqueKeyCol],
                                row[seqSetKeyCol], markerKey, LEVELS[row[levelKeyCol]], row[replicatesCol],
                                row[qnTpmCol] ] )

                logger.debug('Processed %d rows' % len(rows))
                gc.collect()
                return
        
###--- globals ---###

cmds = [
                # 0. impose ordering in the query so combined samples will ordered on disk (making joins
                # more efficient in the postprocessing script)
                '''select distinct c._RNASeqCombined_key, m._RNASeqSet_key, r._Marker_key, c._Level_key,
                                c.numberOfBiologicalReplicates, c.averageQuantileNormalizedTPM
                from gxd_htsample_rnaseqcombined c, gxd_htsample_rnaseq r, gxd_htsample_rnaseqsetmember m
                where c._RNASeqCombined_key = r._RNASeqCombined_key
                        and r._Sample_key = m._Sample_key
                        and c._RNASeqCombined_key >= %d
                        and c._RNASeqCombined_key < %d
                order by 1
                '''
        ]

# order of fields (from the query results) to be written to the
# output file
files = [
        ('expression_ht_consolidated_sample_measurement',
                [ '_RNASeqCombined_key', '_RNASeqSet_key', '_Marker_key', 
                        'level', 'numberOfBiologicalReplicates', 'averageQuantileNormalizedTPM' ],
                [ '_RNASeqCombined_key', '_RNASeqSet_key', '_Marker_key', 
                        'level', 'numberOfBiologicalReplicates', 'averageQuantileNormalizedTPM' ],
        )
]

# global instance of a TemplateGatherer
gatherer = EHCSMGatherer (files, cmds)
gatherer.setupChunking(
        'select min(_RNASeqCombined_key) from gxd_htsample_rnaseqcombined',
        'select max(_RNASeqCombined_key) from gxd_htsample_rnaseqcombined',
        3000000
        )

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        initialize()
        Gatherer.main (gatherer)
