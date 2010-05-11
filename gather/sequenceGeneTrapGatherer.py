#!/usr/local/bin/python
# 
# gathers data for the 'sequenceGeneTrap' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class SequenceGeneTrapGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequenceGeneTrap table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for gene trap sequences,
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
			r['tagMethod'] = sybaseUtil.resolve (
				r['_TagMethod_key'])
			r['vectorEnd'] = sybaseUtil.resolve (
				r['_VectorEnd_key'])
			r['reverseComplement'] = sybaseUtil.resolve (
				r['_ReverseComp_key'])
		return

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		s._TagMethod_key,
		s._VectorEnd_key,
		s._ReverseComp_key,
		s.goodHitCount,
		s.pointCoordinate
	from SEQ_GeneTrap s %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'tagMethod', 'vectorEnd', 'reverseComplement',
	'goodHitCount', 'pointCoordinate',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequenceGeneTrap'

# global instance of a SequenceGeneTrapGatherer
gatherer = SequenceGeneTrapGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
