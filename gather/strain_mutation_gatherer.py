#!./python
# 
# gathers data for the 'strain_to_mutation' table in the front-end database

import Gatherer
import symbolsort
import logger
import StrainUtils

###--- Functions ---###

def rowSortKey(a):
        # assumes strain key is column 0, allele symbol is 5, and marker symbol is 2
        return (a[0], symbolsort.splitter(a[2]), symbolsort.splitter(a[5]))

###--- Classes ---###

class StrainMutationGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the strain_to_mutation table
        # Has: queries to execute against the source database
        # Does: queries the source database for mutations for strains,
        #       collates results, writes tab-delimited text file

        def collateResults(self):
                self.finalColumns, self.finalResults = self.results[0]
                self.finalResults.sort(key=rowSortKey)
                self.convertFinalResultsToList()
                self.finalColumns.append('sequence_num')
                
                i = 1
                for row in self.finalResults:
                        row.append(i)
                        i = i + 1
                
                logger.debug('Compiled %d rows' % len(self.finalResults))
                return
        
###--- globals ---###

cmds = [
        '''select psm._Strain_key, psm._Marker_key, m.symbol as marker_symbol, ma.accID as marker_id,
                        psm._Allele_key, a.symbol as allele_symbol, aa.accID as allele_id, m.name as marker_name
                from prb_strain_marker psm
                inner join %s t on (psm._Strain_key = t._Strain_key)
                inner join mrk_marker m on (psm._Marker_key = m._Marker_key)
                inner join acc_accession ma on (psm._Marker_key = ma._Object_key
                        and ma._MGIType_key = 2
                        and ma._LogicalDB_key = 1
                        and ma.preferred = 1)
                left outer join all_allele a on (psm._Allele_key = a._Allele_key)
                left outer join acc_accession aa on (psm._Allele_key = aa._Object_key
                        and aa._MGIType_key = 11
                        and aa._LogicalDB_key = 1
                        and aa.preferred = 1)
                order by psm._Strain_key, m.symbol, a.symbol''' % StrainUtils.getStrainTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Strain_key', '_Marker_key', 'marker_symbol', 'marker_name', 'marker_id',
        '_Allele_key', 'allele_symbol', 'allele_id', 'sequence_num',
        ]

# prefix for the filename of the output file
filenamePrefix = 'strain_mutation'

# global instance of a StrainMutationGatherer
gatherer = StrainMutationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
