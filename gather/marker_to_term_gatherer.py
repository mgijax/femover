#!./python
# 
# gathers data for the 'marker_to_term' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerToTermGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the marker_to_term table
        # Has: queries to execute against the source database
        # Does: queries the source database for marker/term associations,
        #       collates results, writes tab-delimited text file

        def postprocessResults (self):
                self.convertFinalResultsToList()

                for r in self.finalResults:
                        self.addColumn ('_Refs_key', None, r, self.finalColumns)
                        self.addColumn ('qualifier', None, r, self.finalColumns)
                return 

###--- globals ---###

cmds = [
        # 0. All PIRSF annotations (type 1007) are to mouse markers, but we
        # will ensure that in our query.  Also the qualifier field is not
        # used in mgd for PIRSF annotations, so won't even retrieve it.
        '''select a._Object_key as _Marker_key,
                a._Term_key
        from VOC_Annot a, MRK_Marker m
        where a._AnnotType_key = 1007
                and a._Object_key = m._Marker_key
                and m._Organism_key = 1''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, '_Marker_key', '_Term_key', '_Refs_key', 'qualifier',
        ]

# prefix for the filename of the output file
filenamePrefix = 'marker_to_term'

# global instance of a MarkerToTermGatherer
gatherer = MarkerToTermGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
