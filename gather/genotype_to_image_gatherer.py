#!/usr/local/bin/python
# 
# gathers data for the 'genotype_to_image' table in the front-end database

import Gatherer

###--- Classes ---###

class GenotypeToImageGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the genotype_to_image table
	# Has: queries to execute against the source database
	# Does: queries the source database for associations between genotypes
	#	and images, collates results, writes tab-delimited text file

	def collateResults (self):
		cols, rows = self.results[0]
		self.finalColumns = cols
		self.finalResults = rows

		primaryCol = Gatherer.columnNumber (cols, 'isPrimary')

		self.convertFinalResultsToList()
		for row in self.finalResults:
			if row[primaryCol] == 1:
				qualifier = 'primary'
			else:
				qualifier = None
			self.addColumn ('qualifier', qualifier, row, cols)
		return

###--- globals ---###

cmds = [
	'''select i._Image_key,
		i._Refs_key,
		g._Genotype_key,
		a.isPrimary
	from img_image i,
		img_imagepane p,
		img_imagepane_assoc a,
		gxd_genotype g
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._Object_key = g._Genotype_key
		and a._MGIType_key = 12''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Genotype_key', '_Image_key', '_Refs_key', 'qualifier'
	]

# prefix for the filename of the output file
filenamePrefix = 'genotype_to_image'

# global instance of a GenotypeToImageGatherer
gatherer = GenotypeToImageGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
