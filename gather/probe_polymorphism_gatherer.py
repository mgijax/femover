#!./python
# 
# gathers data for the 'probe_polymorphism' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Globals ---###

endonucleaseCol = None
symbolCol = None
jnumCol = None

###--- Functions ---###

def comparePolymorphisms (a):
        # sort by jnum, endonuclease, and symbol
        
        endo = a[endonucleaseCol]
        if type(endo) != str:
                endo = 'zzzzzz'

        return (a[jnumCol], symbolsort.splitter(a[endonucleaseCol]), symbolsort.splitter(a[symbolCol]))

###--- Classes ---###

class ProbePolymorphismGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the probe_polymorphism table
        # Has: queries to execute against the source database
        # Does: queries the source database for polymorphism data for probes,
        #       collates results, writes tab-delimited text file

        def postprocessResults(self):
                global endonucleaseCol, symbolCol, jnumCol
                
                self.convertFinalResultsToList()
                
                endonucleaseCol = Gatherer.columnNumber(self.finalColumns, 'endonuclease')
                symbolCol = Gatherer.columnNumber(self.finalColumns, 'symbol')
                jnumCol = Gatherer.columnNumber(self.finalColumns, 'jnum')
                
                self.finalResults.sort(key=comparePolymorphisms)
                logger.debug('sorted %d rows' % len(self.finalResults))
                
                self.finalColumns.append('sequence_num')
                i = 0
                for row in self.finalResults:
                        i = i + 1
                        row.append(i)
                        
                logger.debug('added %d sequence numbers' % i)
                return
        
###--- globals ---###

cmds = [
        '''select r._RFLV_key, r._Reference_key, r.endonuclease, r._Marker_key, m.symbol, a.accID, c.numericPart as jnum
                from prb_rflv r, mrk_marker m, acc_accession a, prb_reference pr, bib_citation_cache c
                where r._Marker_key = m._Marker_key
                        and r._Marker_key = a._Object_key
                        and r._Reference_key = pr._Reference_key
                        and pr._Refs_key = c._Refs_key
                        and a._MGITYpe_key = 2
                        and a.preferred = 1
                        and a._LogicalDB_key = 1''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_RFLV_key', '_Reference_key', '_Marker_key', 'accID', 'symbol', 'endonuclease', 'sequence_num'
        ]

# prefix for the filename of the output file
filenamePrefix = 'probe_polymorphism'

# global instance of a ProbePolymorphismGatherer
gatherer = ProbePolymorphismGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
