#!/usr/local/bin/python
# 
# gathers data for the 'Antibody' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class AntibodyGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the Antibody table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for Antibodys,
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

		keyCol = Gatherer.columnNumber (self.finalColumns, '_Antibody_key') 

		classCol = Gatherer.columnNumber (self.finalColumns, '_AntibodyClass_key')

		typeCol = Gatherer.columnNumber (self.finalColumns, '_AntibodyType_key')

		organismCol = Gatherer.columnNumber (self.finalColumns, '_Organism_key')

		ldbCol = Gatherer.columnNumber (self.finalColumns, '_LogicalDB_key')

		for r in self.finalResults:

			self.addColumn ('antibodyClass', Gatherer.resolve (
				r[classCol], 'gxd_antibodyclass', '_AntibodyClass_key',
				'class'), r, self.finalColumns)

			self.addColumn ('antibodyType', Gatherer.resolve (
				r[typeCol], 'gxd_antibodytype', '_AntibodyType_key',
				'antibodyType'), r, self.finalColumns)

			self.addColumn ('organism', Gatherer.resolve (
				r[organismCol], 'mgi_organism', '_Organism_key',
				'commonName'), r, self.finalColumns)

			self.addColumn ('logicalDB', Gatherer.resolve (
				r[ldbCol], 'acc_logicaldb', '_LogicalDB_key',
				'name'), r, self.finalColumns)

		return

###--- globals ---###

cmds = [
	'''select p._Antibody_key,
		p.antibodyName,
		p._AntibodyClass_key,
		p._AntibodyType_key,
		p._Organism_key,
		p.antibodyNote,
		a.accID as primary_id,
		a._LogicalDB_key
	from gxd_antibody p, acc_accession a
	where p._antibody_key = a._Object_key
		and a._MGIType_key = 6
		and a.preferred = 1
		and a._LogicalDB_key = 1

        ''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Antibody_key', 'antibodyName', 
	'antibodyClass', 'antibodyType', 'organism',
	'antibodyNote', 'primary_id', 'logicalDB',]

# prefix for the filename of the output file
filenamePrefix = 'antibody'

# global instance of a AntibodyGatherer
gatherer = AntibodyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)

