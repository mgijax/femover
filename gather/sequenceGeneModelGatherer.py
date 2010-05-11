#!/usr/local/bin/python
# 
# gathers data for the 'sequenceGeneModel' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class SequenceGeneModelGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequenceGeneModel table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for gene model sequences,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single sequence,
		#	rather than for all sequences

		if self.keyField == 'sequenceKey':
			return 's._Sequence_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		for r in self.finalResults:
			r['markerType'] = sybaseUtil.resolve (
				r['_Marker_Type_key'], 'MRK_Types',
				'_Marker_Type_key', 'name')
		return

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		s._GMMarker_Type_key as _Marker_Type_key,
		s.rawBiotype,
		s.exonCount,
		s.transcriptCount
	from SEQ_GeneModel s %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'markerType', 'rawBiotype', 'exonCount',
	'transcriptCount'
	]

# prefix for the filename of the output file
filenamePrefix = 'sequenceGeneModel'

# global instance of a SequenceGeneModelGatherer
gatherer = SequenceGeneModelGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
