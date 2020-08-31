#!./python
# 
# gathers data for the 'sequenceCounts' table in the front-end database

# NOTE: To add more counts:
#       1. add a fieldname for the count as a global (like MarkerCount)
#       2. add a new query to 'cmds' in the main program
#       3. add processing for the new query to collateResults(), to tie the
#               query results to the new fieldname in each sequence's
#               dictionary
#       4. add the new fieldname to fieldOrder in the main program

import Gatherer

###--- Globals ---###

MarkerCount = 'markerCount'
ProbeCount = 'probeCount'

###--- Classes ---###

class SequenceCountsGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the sequenceCounts table
        # Has: queries to execute against the source database
        # Does: queries for primary data for sequence counts,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                # Purpose: to combine the results of the various queries into
                #       one single list of final results, with one row per
                #       sequence

                # list of count types (like field names)
                counts = []

                seqKeyCol = Gatherer.columnNumber (self.results[0][0],
                        '_Sequence_key')

                # initialize dictionary for collecting data per sequence
                #       d[sequence key] = { count type : count }
                d = {}
                for row in self.results[0][1]:
                        sequenceKey = row[seqKeyCol]
                        d[row[0]] = {}

                # count of markers per sequence
                counts.append (MarkerCount)
                mrkCol = Gatherer.columnNumber (self.results[1][0],
                        'mrkCount')
                seqKeyCol = Gatherer.columnNumber (self.results[1][0],
                        '_Sequence_key')

                for row in self.results[1][1]:
                        sequenceKey = row[seqKeyCol]
                        if sequenceKey in d:
                                d[sequenceKey][MarkerCount] = row[mrkCol]

                # count of probes per sequence
                counts.append (ProbeCount)
                prbCol = Gatherer.columnNumber (self.results[2][0],
                        'prbCount')
                seqKeyCol = Gatherer.columnNumber (self.results[2][0],
                        '_Sequence_key')

                for row in self.results[2][1]:
                        sequenceKey = row[seqKeyCol]
                        d[sequenceKey][ProbeCount] = row[prbCol]

                # add other counts here...






                # compile the list of collated counts in self.finalResults
                self.finalResults = []
                sequenceKeys = list(d.keys())
                sequenceKeys.sort()

                self.finalColumns = [ '_Sequence_key' ] + counts

                for sequenceKey in sequenceKeys:
                        row = [ sequenceKey ]
                        for count in counts:
                                if count in d[sequenceKey]:
                                        row.append (d[sequenceKey][count])
                                else:
                                        row.append (0)

                        self.finalResults.append (row)
                return

        def getMinKeyQuery (self):
                return 'select min(_Sequence_key) from seq_sequence'

        def getMaxKeyQuery (self):
                return 'select max(_Sequence_key) from seq_sequence'

###--- globals ---###

cmds = [
        # all sequences
        '''select m._Sequence_key
                from seq_sequence m
                where m._Sequence_key >= %d and m._Sequence_key < %d''',

        # count of markers for each sequence
        '''select m._Sequence_key, count(1) as mrkCount
                from seq_marker_cache m
                where m._Sequence_key >= %d and m._Sequence_key < %d
                group by m._Sequence_key''',

        # count of probes for each sequence
        '''select m._Sequence_key, count(1) as prbCount
                from seq_probe_cache m
                where m._Sequence_key >= %d and m._Sequence_key < %d
                group by m._Sequence_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Sequence_key', MarkerCount, ProbeCount ]

# prefix for the filename of the output file
filenamePrefix = 'sequence_counts'

# global instance of a SequenceCountsGatherer
gatherer = SequenceCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
