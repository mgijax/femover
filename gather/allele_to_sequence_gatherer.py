#!./python
# 
# gathers data for the 'allele_to_sequence' table in the front-end database

import Gatherer

###--- Classes ---###

class AlleleToSequenceGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the allele_to_sequence table
        # Has: queries to execute against the source database
        # Does: queries the source database for allele/sequence
        #       relationships, collates results, writes tab-delimited text
        #       file

        def postprocessResults (self):
                self.convertFinalResultsToList()

                qualCol = Gatherer.columnNumber (self.finalColumns,
                        '_Qualifier_key')

                self.finalColumns.append ('qualifier')

                for r in self.finalResults:
                        qualKey = r[qualCol]
                        if qualKey == None:
                                r.append (None)
                        else:
                                r.append (Gatherer.resolve (qualKey))
                return

###--- globals ---###

cmds = [
        '''select m._Allele_key,
                m._Sequence_key,
                m._Refs_key,
                m._Qualifier_key
        from seq_allele_assoc m
        where exists (select 1 from seq_genetrap g
                where g._Sequence_key = m._Sequence_key)
        and exists (select 1 from seq_sequence s
                where m._Sequence_key = s._Sequence_key)'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Allele_key', '_Sequence_key', '_Refs_key',
        'qualifier'
        ]

# prefix for the filename of the output file
filenamePrefix = 'allele_to_sequence'

# global instance of a AlleleToSequenceGatherer
gatherer = AlleleToSequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
