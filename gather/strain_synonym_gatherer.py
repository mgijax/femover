#!./python
# 
# gathers data for the 'strain_synonym' table in the front-end database

import Gatherer
import symbolsort
import logger
import StrainUtils

###--- Functions ---###

def resultSortKey(a):
        # assumes strain key is column 0, synonym is column 1, synonym type is column 2, and
        # J: number is column 3
        
        return (a[0], symbolsort.splitter(a[1]), symbolsort.splitter(a[2]), symbolsort.splitter(a[3]))

###--- Classes ---###

class StrainSynonymGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the strain_synonym table
        # Has: queries to execute against the source database
        # Does: queries the source database for synonyms of strains,
        #       collates results, writes tab-delimited text file

        def collateResults(self):
                self.finalColumns, self.finalResults = self.results[0]
                self.finalResults.sort(key=resultSortKey)
                self.convertFinalResultsToList()
                self.finalColumns.append('sequence_num')
                
                i = 1
                for row in self.finalResults:
                        row.append(i)
                        i = i + 1

                logger.debug('built %d synonym rows' % len(self.finalResults))
                return
        
###--- globals ---###

cmds = [
        '''select s._Object_key, s.synonym, t.synonymType, c.jnumID
                from mgi_synonym s
                inner join mgi_synonymtype t on (s._SynonymType_key = t._SynonymType_key and t._MGIType_key = 10)
                inner join %s ps on (s._Object_key = ps._Strain_key)
                left outer join bib_citation_cache c on (s._Refs_key = c._Refs_key)
                order by 1, 2''' % StrainUtils.getStrainTempTable()
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Object_key', 'synonym', 'synonymType', 'jnumID', 'sequence_num' ]

# prefix for the filename of the output file
filenamePrefix = 'strain_synonym'

# global instance of a StrainSynonymGatherer
gatherer = StrainSynonymGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
