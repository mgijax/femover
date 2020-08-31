#!./python
# 
# gathers data for the 'hdp_marker_to_reference' table in the front-end db.
# Pulled out of hdp_annotation_gatherer in Spring 2015 to simplify that
# gatherer and to allow counts of disease-relevant references for markers to
# be pre-computed and stored in the database.

import Gatherer
import config
import ReferenceUtils
import OutputFile
import logger
import gc

###--- Globals ---###

# rough number of output rows to cache in memory
cacheSize = 50000

# output data file
outFile = OutputFile.OutputFile('hdp_marker_to_reference')

###--- Functions ---###

def main():
        markers = ReferenceUtils.getMarkersWithDiseaseRelevantReferences()

        outCols = [ Gatherer.AUTO, '_Marker_key', '_Refs_key' ]
        cols = [ '_Marker_key', '_Refs_key' ]
        rows = []

        # We walk through the markers and collect the disease-relevant refs
        # for each, writing them out periodically to save space.

        for marker in markers:
                refs = ReferenceUtils.getDiseaseRelevantReferences(marker)

                for refsKey in refs:
                        rows.append( (marker, refsKey) )

                # Our cache size is approximate, so we check it only
                # after each marker, as this is close enough.

                if len(rows) >= cacheSize:
                        outFile.writeToFile(outCols, cols, rows)
                        rows = []
                        gc.collect()

        # If any unwritten rows, we need to write them out.

        if rows:
                outFile.writeToFile(outCols, cols, rows)
                rows = []
                gc.collect() 

        outFile.close()
        print('%s %s' % (outFile.getPath(), 'hdp_marker_to_reference'))
        return

if __name__ == '__main__':
        main()
