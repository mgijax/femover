#!/usr/local/bin/python
# 
# gathers data for the 'alleleCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like ReferenceCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each allele's dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer
import logger

###--- Globals ---###

error = 'allele_counts_gatherer.error'

MarkerCount = 'markerCount'
ReferenceCount = 'referenceCount'
ExpressionCount = 'expressionAssayResultCount'
ImageCount = 'imageCount'

###--- Classes ---###

class AlleleCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleCounts table
	# Has: queries to execute against the source database
	# Does: queries the source database for allele counts,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	allele

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per allele
		#	d[allele key] = { count type : count }
		d = {}
		for row in self.results[0][1]:
			d[row[0]] = {}

		# counts to add in this order, with each tuple being:
		#	(set of results, count constant, count column)

		toAdd = [ (self.results[1], MarkerCount, 'mrkCount'),
			(self.results[2], ReferenceCount, 'refCount'),
			(self.results[3], ExpressionCount, 'expCount'),
			]

		for (r, countName, colName) in toAdd:
			logger.debug ('Processing %s, %d rows' % (countName,
				len(r[1])) )
			counts.append (countName)
			keyCol = Gatherer.columnNumber (r[0], '_Allele_key')
			countCol = Gatherer.columnNumber (r[0], colName)

			for row in r[1]:
				allele = row[keyCol]
				if d.has_key(allele):
					d[allele][countName] = row[countCol]
				else:
					logger.debug (
					'Unknown allele key: %d' % allele)

		# non-standard handling for images; we need to collect the
		# image keys for each allele, then get the counts from there

		columns, rows = self.results[4]

		logger.debug ('Processing ImageCount, %d rows' % \
			len(rows) )

		allKeyCol = Gatherer.columnNumber (columns, '_Allele_key')
		imgKeyCol = Gatherer.columnNumber (columns, '_Image_key')

		# imagesPerAllele[allele key] = [ image keys ]
		imagesPerAllele = {}

		for row in rows:
			allele = row[allKeyCol]
			image = row[imgKeyCol]

			if imagesPerAllele.has_key(allele):
				if image not in imagesPerAllele[allele]:
					imagesPerAllele[allele].append (image)
			else:
				imagesPerAllele[allele] = [ image ]

		alleleKeys = imagesPerAllele.keys()
		for allele in alleleKeys:
			if d.has_key(allele):
				d[allele][ImageCount] = \
					len(imagesPerAllele[allele]) 

		counts.append (ImageCount)
		
		# compile the list of collated counts in self.finalResults

		self.finalResults = []
		alleleKeys = d.keys()
		alleleKeys.sort()

		self.finalColumns = [ '_Allele_key' ] + counts

		for alleleKey in alleleKeys:
			row = [ alleleKey ]
			for count in counts:
				if d[alleleKey].has_key(count):
					row.append (d[alleleKey][count])
				else:
					row.append (0)

			self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	# all alleles
	'''select _Allele_key from all_allele''',

	# count of markers for each allele
	'''select m._Allele_key, count(1) as mrkCount
		from all_marker_assoc m
		group by m._Allele_key''',

	# count of references for each allele
	'''select _Object_key as _Allele_key, 
			count(distinct _Refs_key) as refCount
		from mgi_reference_assoc
		where _MGIType_key = 11
		group by _Object_key''',

	# count of expression assay results for each allele
	'''select gag._Allele_key, count(distinct _Expression_key) as expCount
		from gxd_allelegenotype gag,
			gxd_expression ge
		where gag._Genotype_key = ge._Genotype_key
			and ge.isForGXD = 1
		group by gag._Allele_key''',

	# allele images by key (we count them in Python, since I didn't see
	# an obvious way to handle the 'union' in a 'select count')
	'''select ipa._Object_key as _Allele_key,
			ip._Image_key
		from img_imagepane_assoc ipa,
			img_imagepane ip
		where ipa._MGIType_key = 11
			and ipa._ImagePane_key = ip._ImagePane_key
	   union
	   select gag._Allele_key,
	   		ip._Image_key
	   	from img_imagepane_assoc ipa,
			img_imagepane ip,
			gxd_allelegenotype gag
		where gag._Genotype_key = ipa._Object_key
			and ipa._MGIType_key = 12
			and ipa._ImagePane_key = ip._ImagePane_key''', 
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Allele_key', MarkerCount, ReferenceCount, ExpressionCount, 
		ImageCount, ]

# prefix for the filename of the output file
filenamePrefix = 'allele_counts'

# global instance of a AlleleCountsGatherer
gatherer = AlleleCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
