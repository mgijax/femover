#!/usr/local/bin/python
# 
# gathers data for the 'sequenceSource' table in the front-end database

import config
import Gatherer

###--- Classes ---###

class SequenceSourceGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the sequenceSource table
	# Has: queries to execute against the source database
	# Does: queries source database for source data for sequences,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: we override this method to provide cached key
		#	lookups, to attempt to give a performance boost

		self.convertFinalResultsToList()

		tissueCol = Gatherer.columnNumber (self.finalColumns,
			'_Tissue_key')
		sexCol = Gatherer.columnNumber (self.finalColumns,
			'_Gender_key')

		for r in self.finalResults:

			# lookups from voc_term

			self.addColumn ('tissue', Gatherer.resolve(
				r[tissueCol]), r, self.finalColumns)
			self.addColumn ('sex', Gatherer.resolve(
				r[sexCol]), r, self.finalColumns)
		return

	def getMinKeyQuery (self):
		return 'select min(_Sequence_key) from seq_source_assoc'

	def getMaxKeyQuery (self):
		return 'select max(_Sequence_key) from seq_source_assoc'

###--- globals ---###

cmds = [
	'''select ssa._Sequence_key,
		s.strain,
		ps._Tissue_key,
		ps._Gender_key,
		ps.age,
		c.cellLine
	from seq_source_assoc ssa,
		prb_source ps,
		all_cellline c,
		prb_strain s
	where ssa._Sequence_key >= %d and ssa._Sequence_key < %d
		and ssa._Source_key = ps._Source_key
		and ps._Strain_key = s._Strain_key
		and ps._CellLine_key = c._CellLine_key'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO,
	'_Sequence_key', 'strain', 'tissue', 'age', 'sex', 'cellLine',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequenceSource'

# global instance of a sequenceGatherer
gatherer = SequenceSourceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
