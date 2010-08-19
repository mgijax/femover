#!/usr/local/bin/python
# 
# gathers data for the 'sequenceGeneModel' table in the front-end database

import Gatherer

###--- Classes ---###

SequenceGeneModelGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the sequenceGeneModel table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for gene model sequences,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		m.name as markerType,
		s.rawBiotype,
		s.exonCount,
		s.transcriptCount
	from seq_genemodel s, mrk_types m
	where s._GMMarker_Type_key = m._Marker_Type_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'markerType', 'rawBiotype', 'exonCount',
	'transcriptCount'
	]

# prefix for the filename of the output file
filenamePrefix = 'sequence_gene_model'

# global instance of a SequenceGeneModelGatherer
gatherer = SequenceGeneModelGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
