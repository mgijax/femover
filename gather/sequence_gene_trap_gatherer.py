#!/usr/local/bin/python
# 
# gathers data for the 'sequenceGeneTrap' table in the front-end database

import Gatherer

###--- Classes ---###

class SequenceGeneTrapGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequenceGeneTrap table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for gene trap sequences,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override to provide key-based lookups

		self.convertFinalResultsToList()

		tmCol = Gatherer.columnNumber (self.finalColumns,
			'_TagMethod_key')
		veCol = Gatherer.columnNumber (self.finalColumns,
			'_VectorEnd_key')
		rcCol = Gatherer.columnNumber (self.finalColumns,
			'_ReverseComp_key')

		for r in self.finalResults:
			self.addColumn ('tagMethod', Gatherer.resolve (
				r[tmCol]), r, self.finalColumns)
			self.addColumn ('vectorEnd', Gatherer.resolve (
				r[veCol]), r, self.finalColumns)
			self.addColumn ('reverseComplement',
				Gatherer.resolve (r[rcCol]),
				r, self.finalColumns)
		return 

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from seq_genetrap'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from seq_genetrap'

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		s._TagMethod_key,
		s._VectorEnd_key,
		s._ReverseComp_key,
		s.goodHitCount,
		s.pointCoordinate
	from seq_genetrap s
	where s._Sequence_key >= %d and s._Sequence_key < %d''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'tagMethod', 'vectorEnd', 'reverseComplement',
	'goodHitCount', 'pointCoordinate',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequence_gene_trap'

# global instance of a SequenceGeneTrapGatherer
gatherer = SequenceGeneTrapGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
