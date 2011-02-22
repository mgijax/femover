#!/usr/local/bin/python
# 
# gathers data for the 'image_alleles' table in the front-end database

import Gatherer

###--- Classes ---###

class ImageAllelesGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the image_alleles table
	# Has: queries to execute against the source database
	# Does: queries the source database for alleles associated with
	#	images,	collates results, writes tab-delimited text file

	def postprocessResults (self):
		self.convertFinalResultsToList()

		i = 0
		for row in self.finalResults:
			i = i + 1
			self.addColumn ('sequenceNum', i, row,
				self.finalColumns)
		return 

###--- globals ---###

cmds = [
	'''select i._Image_key,
		a._Object_key as _Allele_key,
		aa.symbol,
		aa.name,
		acc.accID
	from img_image i,
		img_imagepane p,
		img_imagepane_assoc a,
		all_allele aa,
		acc_accession acc
	where i._Image_key = p._Image_key
		and p._ImagePane_key = a._ImagePane_key
		and a._MGIType_key = 11
		and a._Object_key = aa._Allele_key
		and aa._Allele_key = acc._Object_key
		and acc.preferred = 1
		and acc._MGIType_key = 11
	order by i._Image_key, aa.symbol, aa.name'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Image_key', '_Allele_key', 'symbol', 'name', 'accID',
	'sequenceNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'image_alleles'

# global instance of a ImageAllelesGatherer
gatherer = ImageAllelesGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
