#!./python
# 
# gathers data for the 'universal_expression_result' table in the front-end database

import Gatherer
import logger
import GXDUniUtils
import OutputFile

from expression_ht import experiments
GXDUniUtils.setExptIDList(experiments.getExperimentIDsAsList(True))

# first - build the needed keystone table
keystoneTable = GXDUniUtils.getKeystoneTable()

###--- Classes ---###

class UERGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the universal_expression_result table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for both classical
        #       and RNA-Seq expression results, collates results, writes
        #       tab-delimited text file

        def __init__ (self, filenamePrefix, fieldOrder, cmds):
                Gatherer.ChunkGatherer.__init__(self, filenamePrefix, fieldOrder, cmds)
                self.chunkSize = 500000
                return

        def getOutputFile (self):
                return OutputFile.TrustingOutputFile (self.filenamePrefix)

        def getMinKeyQuery (self):
                return 'select min(uni_key) from %s' % keystoneTable

        def getMaxKeyQuery (self):
                return 'select max(uni_key) from %s' % keystoneTable

###--- globals ---###

cmds = [
        '''select uni_key, is_classical, result_key
                from %s
                where uni_key >= %%d
                and uni_key < %%d
                order by 1''' % keystoneTable,
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        'uni_key', 'is_classical', 'result_key',
        ]

# prefix for the filename of the output file
filenamePrefix = 'universal_expression_result'

# global instance of a UERGatherer
gatherer = UERGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
