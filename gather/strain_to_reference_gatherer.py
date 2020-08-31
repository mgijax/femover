#!./python
# 
# gathers data for the 'strain_to_reference' table in the front-end database

import Gatherer
import logger
import StrainUtils

###--- Classes ---###

class StrainToReferenceGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the strain_to_reference table
        # Has: queries to execute against the source database
        # Does: queries the source database for references for strains,
        #       collates results, writes tab-delimited text file

        def collateResults(self):
                self.finalColumns = [ '_Strain_key', '_Refs_key', 'qualifier' ]
                self.finalResults = []
                
                cols, rows = self.results[0]
                
                strainCol = Gatherer.columnNumber(cols, '_Strain_key')
                refsCol = Gatherer.columnNumber(cols, '_Refs_key')
                
                lastStrain = None
                for row in rows:
                        strain = row[strainCol]
                        if strain == lastStrain:
                                qualifier = None
                        else:
                                qualifier = 'earliest'
                                lastStrain = strain
                                
                                # flag the last reference for the previous strain as its latest
                                if self.finalResults:
                                        if self.finalResults[-1][-1] == None:
                                                self.finalResults[-1][-1] = 'latest'
                                
                        self.finalResults.append( [strain, row[refsCol], qualifier] ) 
                                
                logger.debug('Collected %d strain/refs rows' % len(self.finalResults))
                return
        
###--- globals ---###

cmds = [
        # 0. actually pull out the ordered references for each strain
        '''select distinct sr._Strain_key, r.year, c.numericPart, sr._Refs_key
                from %s sr, bib_citation_cache c, bib_refs r
                where sr._Refs_key = c._Refs_key
                        and sr._Refs_key = r._Refs_key
                order by 1, 2, 3''' % StrainUtils.getStrainReferenceTempTable()
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Strain_key', '_Refs_key', 'qualifier' ]

# prefix for the filename of the output file
filenamePrefix = 'strain_to_reference'

# global instance of a StrainToReferenceGatherer
gatherer = StrainToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
