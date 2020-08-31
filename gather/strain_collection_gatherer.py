#!./python
# 
# gathers data for the 'strain_collection' table in the front-end database

import Gatherer
import StrainUtils
import FileReader
import logger

###--- Classes ---###

collections = [
        ('docc_founder_strains.txt', 'DOCCFounders'),           # filename, collection name
        ('hdp_strains.txt', 'HDP'),
        ('cc_strains.txt', 'CC'),
        ]

class StrainCollectionGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the strain_collection table
        # Has: queries to execute against the source database
        # Does: reads two data files with collections of strains,
        #       collates results, writes tab-delimited text file
        
        def collateResults(self):
                # collect the set of primary strain IDs from the database (in idCache)
                
                idCache = {}            # maps from primary strain ID to strain key
                cols, rows = self.results[0]
                keyCol = Gatherer.columnNumber(cols, '_Strain_key')
                idCol = Gatherer.columnNumber(cols, 'strain_id')
                
                for row in rows:
                        idCache[row[idCol]] = row[keyCol]
                        
                logger.debug('Cached %d strain IDs' % len(idCache))

                # process the data files to generate the set of rows to load
                self.finalColumns = [ 'strain_key', 'collection' ]
                self.finalResults = []
                
                for (filename, collection) in collections:
                        fr = FileReader.FileReader('../data/%s' % filename, hasHeaderRow = False)
                        cols, rows = fr.getData()
                        
                        for row in rows:
                                if row and (len(row) > 0):
                                        strainID = row[-1]
                                        if strainID in idCache:
                                                self.finalResults.append( (idCache[strainID], collection) )
                
                logger.info('Got %d rows for %d collections' % (len(self.finalResults), len(collections)))
                return

###--- globals ---###

cmds = [
        'select _Strain_key, strain_id from %s' % StrainUtils.getStrainIDTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'strain_key', 'collection' ]

# prefix for the filename of the output file
filenamePrefix = 'strain_collection'

# global instance of a StrainCollectionGatherer
gatherer = StrainCollectionGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
