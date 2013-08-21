#!/usr/local/bin/python
# 
# gathers data for the 'allele_to_image' table in the front-end database

import Gatherer

###--- Classes ---###

class AlleleToImageGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the allele_to_image table
	# Has: queries to execute against the source database
	# Does: queries the source database for alleles associated with
	#	images,	collates results, writes tab-delimited text file

	def collateResults (self):
		cols, rows = self.results[0]

		imageCol = Gatherer.columnNumber (cols, '_Image_key')
		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
		isPrimaryCol = Gatherer.columnNumber (cols, 'isPrimary')

		self.finalResults = []
		self.finalColumns = [ '_Image_key', '_Allele_key',
			'qualifier' ]

		# (image key, allele key) = 1
		seen = {}

		# assumes ordering by allele key, then by isPrimary in
		# descending order

		for row in rows:
			imageKey = row[imageCol]
			alleleKey = row[alleleCol]

			t = (imageKey, alleleKey)

			if seen.has_key(t):
				continue
			seen[t] = 1

			if row[isPrimaryCol] == 1:
				qualifier = 'primary'
			else:
				qualifier = None

			self.finalResults.append ( [ imageKey, alleleKey,
				qualifier ] )
		return

###--- globals ---###

cmds = [
	'''select i._Image_key,
		a._Object_key as _Allele_key,
		a.isPrimary
	from img_image i,
		img_imagepane p,
		img_imagepane_assoc a
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._MGIType_key = 11
		and exists (select 1 from all_allele aa
			where aa._Allele_key = a._Object_key)
	union
	select i._Image_key,
		gag._Allele_key,
		0 as isPrimary
	from img_image i,
		img_imagepane p,
		img_imagepane_assoc a,
		gxd_allelegenotype gag
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._MGIType_key = 12
		and a._Object_key = gag._Genotype_key
		and exists (select 1 from all_allele aa
			where aa._Allele_key = gag._Allele_key)
	order by _Allele_key, isPrimary desc'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Allele_key', '_Image_key', 'qualifier',
	]

# prefix for the filename of the output file
filenamePrefix = 'allele_to_image'

# global instance of a AlleleToImageGatherer
gatherer = AlleleToImageGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
