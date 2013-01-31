#!/usr/local/bin/python
# 
# gathers data for the 'antigen' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class AntigenGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the antigen table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for antigens,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		
		# the first query contains the bulk of the data we need, with
		# the rest to come via post-processing

		self.finalColumns = self.results[0][0]
		self.finalResults = self.results[0][1]
		return

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Antigen_key') 
		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')

		for r in self.finalResults:
			self.addColumn ('logicalDB', Gatherer.resolve (
				r[ldbCol], 'acc_logicaldb', '_LogicalDB_key',
				'name'), r, self.finalColumns)

		return

###--- globals ---###

cmds = [
	'''select p._Antigen_key,
		p.antigenName,
		p.regionCovered,
		p.antigenNote,
		a.accID as primary_id,
		a._LogicalDB_key
	from gxd_antigen p, acc_accession a
	where p._Antigen_key = a._Object_key
		and a._MGIType_key = 7
		and a.preferred = 1
		and a._LogicalDB_key = 1

        ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Antigen_key', 'antigenName', 'regionCovered', 'antigenNote', 'primary_id', 'logicalDB',
	]

# prefix for the filename of the output file
filenamePrefix = 'antigen'

# global instance of a AntigenGatherer
gatherer = AntigenGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)

