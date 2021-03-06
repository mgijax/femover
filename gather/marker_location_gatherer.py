#!./python
# 
# gathers data for the 'markerLocation' table in the front-end database

import Gatherer
import config

###--- Classes ---###

class MarkerLocationGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the markerLocation table
        # Has: queries to execute against the source database
        # Does: queries the source database for marker locations,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # Purpose: go through the set of query results and assemble
                #       the necessary records in self.finalResults
                
                self.finalColumns = [ 'markerKey', 'sequenceNum',
                        'chromosome', 'cmOffset', 'cytogeneticOffset',
                        'startCoordinate', 'endCoordinate', 'buildIdentifier',
                        'locationType', 'mapUnits', 'provider', 'strand' ]
                self.finalResults = []

                # first deal with markers from MRK_Location_Cache

                cols = self.results[0][0]
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')
                # genetic chromosome
                chrCol = Gatherer.columnNumber (cols, 'chromosome')
                gChrCol = Gatherer.columnNumber (cols, 'genomicChromosome')
                cmCol = Gatherer.columnNumber (cols, 'cmOffset')
                cytoCol = Gatherer.columnNumber (cols, 'cytogeneticOffset')
                startCol = Gatherer.columnNumber (cols, 'startCoordinate')
                endCol = Gatherer.columnNumber (cols, 'endCoordinate')
                buildCol = Gatherer.columnNumber (cols, 'version')
                strandCol = Gatherer.columnNumber (cols, 'strand')
                providerCol = Gatherer.columnNumber (cols, 'provider')

                for row in self.results[0][1]:
                        startCoordinate = row[startCol]
                        cm = row[cmCol]
                        cyto = row[cytoCol]
                        key = row[keyCol]
                        chrom = row[chrCol]
                        gChrom = row[gChrCol]
                        strand = row[strandCol]
                        prov = row[providerCol]

                        seqNum = 0

                        if startCoordinate:

                                seqNum = seqNum + 1
                                self.finalResults.append ( [ key, seqNum,
                                        gChrom, None, None,
                                        int(startCoordinate),
                                        int(row[endCol]), row[buildCol],
                                        'coordinates', 'bp', prov, strand ] )
                        if cm and (int(cm) != -1 or not cyto):
                                seqNum = seqNum + 1
                                self.finalResults.append ( [ key, seqNum,
                                        chrom, '%0.2f' % cm, None,
                                        None, None, None,
                                        'centimorgans', 'cM', None, None ] )
                        if cyto:
                                seqNum = seqNum + 1
                                self.finalResults.append ( [ key, seqNum,
                                        chrom, None, cyto,
                                        None, None, None,
                                        'cytogenetic', None, None, None ] )

                        # did we only have a chromosome?
                        if chrom and (seqNum == 0):
                                seqNum = seqNum + 1
                                self.finalResults.append ( [ key, seqNum,
                                        chrom, None, None,
                                        None, None, None,
                                        'cytogenetic', None, None, None ] )

                # add markers not in MRK_Location_Cache, i.e. non-mouse markers
                # non-mouse markers do not have cmOffsets (null)

                cols = self.results[-1][0]
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')
                chrCol = Gatherer.columnNumber (cols, 'chromosome')
                cytoCol = Gatherer.columnNumber (cols, 'cytogeneticOffset')

                for row in self.results[-1][1]:
                        cyto = row[cytoCol]
                        key = row[keyCol]
                        chrom = row[chrCol]

                        seqNum = 1

                        if cyto:
                                self.finalResults.append ( [ key, seqNum,
                                        chrom, None, cyto,
                                        None, None, None,
                                        'cytogenetic', None, None, None ] )

                        else:
                                self.finalResults.append ( [ key, seqNum,
                                        chrom, None, None,
                                        None, None, None,
                                        'cytogenetic', None, None, None ] )

                return

###--- globals ---###
cmds = [
        # mouse and human markers are in MRK_Location_Cache

        '''select distinct _Marker_key, chromosome, cmOffset, genomicChromosome,
                provider, cytogeneticOffset, startCoordinate, endCoordinate, 
                version, strand, mapUnits
           from mrk_location_cache
        ''',

        # pick up the non-mouse organisms that are not included in MRK_Location_Cache

        '''select m. _Marker_key, m.chromosome, m.cytogeneticOffset
           from mrk_marker m
           where m._Organism_key != 1
           and not exists (select 1 from mrk_location_cache c where m._Marker_key = c._Marker_key)
        ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, 'markerKey', 'sequenceNum', 'chromosome', 'cmOffset',
        'cytogeneticOffset', 'startCoordinate', 'endCoordinate', 
        'buildIdentifier', 'locationType', 'mapUnits', 'provider', 'strand',
        ]

# prefix for the filename of the output file
filenamePrefix = 'marker_location'

# global instance of a MarkerLocationGatherer
gatherer = MarkerLocationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
