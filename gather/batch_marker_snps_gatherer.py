#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_snps' table in the front-end database

import Gatherer
import config
import MarkerSnpAssociations

###--- Classes ---###

BatchMarkerSnpsGatherer = Gatherer.Gatherer

###--- globals ---###

# note there should be no associated SNPs for QTL markers
cmds = [ '''
	select m._Marker_key, a.accID, row_number() over(order by a.accID) sequence_num
        from mrk_marker m, snp_consensussnp_marker s, snp_accession a
        where m._Organism_key = 1
                and m._Marker_Type_key != 6
                and m._Marker_Status_key in (1,3)
                and m._Marker_key = s._Marker_key
                and s._consensussnp_key = a._Object_key
                and a._MGIType_key = 30
	'''
	#and s._consensussnp_key >= %d and s._consensussnp_key < %d
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'accid', 'sequence_num'
	]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_snps'

# global instance of a BatchMarkerSnpsGatherer
gatherer = BatchMarkerSnpsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
