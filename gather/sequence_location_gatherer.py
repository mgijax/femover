#!./python
# 
# gathers data for the 'sequenceLocation' table in the front-end database

import Gatherer

###--- Classes ---###

class SequenceLocationGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the sequenceLocation table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for sequence
        #       locations, collates results, writes tab-delimited text file

        def postprocessResults (self):
                # Purpose: override to provide key-based lookups

                self.convertFinalResultsToList()

                mapCol = Gatherer.columnNumber (self.finalColumns, '_Map_key')

                for r in self.finalResults:
                        self.addColumn ('buildIdentifier',
                                Gatherer.resolve (r[mapCol], 'map_coordinate',
                                '_Map_key', 'name'),
                                r, self.finalColumns)
                return

        def getMinKeyQuery (self):
                return 'select min(_Sequence_key) from seq_coord_cache'

        def getMaxKeyQuery (self):
                return 'select max(_Sequence_key) from seq_coord_cache'

###--- globals ---###

cmds = [
        '''select s._Sequence_key,
                1 as sequenceNum,
                s.chromosome,
                s.startCoordinate,
                s.endCoordinate,
                s._Map_key,
                'coordinates' as locationType,
                s.mapUnits,
                s.provider,
                s.version,
                s.strand
        from seq_coord_cache s
        where s._Sequence_key >= %d and s._Sequence_key < %d
                and exists (select 1 from seq_sequence ss
                        where s._Sequence_key = ss._Sequence_key)''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Sequence_key', 'sequenceNum', 'chromosome',
        'startCoordinate', 'endCoordinate', 'buildIdentifier',
        'locationType', 'mapUnits', 'provider', 'version', 'strand',
        ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_location'

# global instance of a SequenceLocationGatherer
gatherer = SequenceLocationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
