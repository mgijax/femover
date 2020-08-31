#!./python
# 
# gathers data for the 'marker_polymorphism' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Functions ---###

def tupleCompare(a):
        # return sort key for 'a', whihc is: (marker key, probe name, jnum ID, endonuclease, rflv key)
        # sort by:  marker key (ascending), jnum ID (smart-alpha), probe name (smart-alpha),
        #       endonuclease (smart-alpha), rflv key (ascending)
        
        if a[3] == None:
                endo = 'zzzz'
        else:
                endo = a[3]

        return (a[0], symbolsort.splitter(a[2]), symbolsort.splitter(a[1]), symbolsort.splitter(endo), a[4])

###--- Classes ---###

class MarkerPolymorphismGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the marker_polymorphism table
        # Has: queries to execute against the source database
        # Does: queries the source database for polymorphism data for markers,
        #       collates results, writes tab-delimited text file

        def postprocessResults(self):
                self.convertFinalResultsToList()

                seqNum = {}                             # (marker key, probe name, rflv key) : integer sequence num
                
                self.finalColumns.append('sequence_num')
                
                markerCol = Gatherer.columnNumber(self.finalColumns, '_Marker_key')
                probeCol = Gatherer.columnNumber(self.finalColumns, 'name')
                rflvCol = Gatherer.columnNumber(self.finalColumns, '_RFLV_key')
                jnumCol = Gatherer.columnNumber(self.finalColumns, 'jnum_id')
                endonucleaseCol = Gatherer.columnNumber(self.finalColumns, 'endonuclease')
                
                for row in self.finalResults:
                        seqNum[(row[markerCol], row[probeCol], row[jnumCol], row[endonucleaseCol], row[rflvCol])] = 0
                logger.debug('Collected data for %d rows' % len(seqNum))
                
                tuples = list(seqNum.keys())
                tuples.sort(key=tupleCompare)
                logger.debug('Sorted %d rows' % len(seqNum))
                
                i = 0
                for myTuple in tuples:
                        i = i + 1
                        seqNum[myTuple] = i
                logger.debug('Computed sequence numbers')
                        
                for row in self.finalResults:
                        row.append(seqNum[(row[markerCol], row[probeCol], row[jnumCol], row[endonucleaseCol], row[rflvCol])])
                logger.debug('Appended sequence numbers')
                return
        
###--- globals ---###

cmds = [
        '''select p._Marker_key, p._RFLV_key, j.accID as jnum_id, pp.name, a.accID as probe_id, p.endonuclease,
                        case when t.term = 'primer' then 'PCR'
                                else 'RFLP'
                                end as polymorphism_type
                from prb_rflv p, prb_reference r, prb_probe pp, acc_accession j, voc_term t, acc_accession a
                where p._Reference_key = r._Reference_key
                        and r._Probe_key = pp._Probe_key
                        and r._Refs_key = j._Object_key
                        and j._LogicalDB_key = 1
                        and j._MGIType_key = 1
                        and j.preferred = 1
                        and j.prefixPart = 'J:'
                        and r._Probe_key = a._Object_key
                        and a._LogicalDB_key = 1
                        and a._MGIType_key = 3
                        and a.preferred = 1
                        and pp._SegmentType_key = t._Term_key
                order by p._Marker_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_RFLV_key', '_Marker_key', 'polymorphism_type', 'jnum_id', 'name', 'probe_id', 'endonuclease',
        'sequence_num' ]

# prefix for the filename of the output file
filenamePrefix = 'marker_polymorphism'

# global instance of a MarkerPolymorphismGatherer
gatherer = MarkerPolymorphismGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
