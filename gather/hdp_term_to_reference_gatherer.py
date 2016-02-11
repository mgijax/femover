#!/usr/local/bin/python
# 
# gathers data for the 'hdp_term_to_reference' table in the front-end database

import Gatherer
import logger
import DiseasePortalUtils

###--- Classes ---###

class TermToReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the hdp_term_to_reference table
	# Has: queries to execute against the source database
	# Does: queries the source database for data about terms and references
	#	for the HMDC, collates results, writes tab-delimited text file

	def collateResults (self):
		termRefDict = {}
		(cols, rows) = self.results[0]
		termKeyCol = Gatherer.columnNumber(cols, '_Term_key')
		refKeyCol = Gatherer.columnNumber(cols, '_Refs_key')

		for row in rows:
			termRefDict.setdefault(row[termKeyCol],[]).append(
				row[refKeyCol])

		logger.debug('Found refs for %d terms' % len(termRefDict))

		self.finalColumns = [ '_Term_key', '_Refs_key' ]
		self.finalResults = []

		# filter down the rows returned to only be those from
		# annotations that rolled up to markers.  Why?  We need also
		# include the disease-to-allele references, but only when the
		# term also appears in a rolled-up disease-to-genotype
		# annotation.

		annotCols, annotRows = DiseasePortalUtils.getAnnotations(
			's._AnnotType_key = %d' % \
				DiseasePortalUtils.OMIM_MARKER)

		termKeyCol = Gatherer.columnNumber(annotCols, '_Term_key')

		for row in annotRows:
			termKey = row[termKeyCol]

			if termRefDict.has_key(termKey):
				for refKey in termRefDict[termKey]:
					t = [ termKey, refKey ]
					if t not in self.finalResults:
						self.finalResults.append(t)

		logger.debug('Filtered %d rows down to %d' % (len(rows),
			len(self.finalResults)) )
		return 


###--- globals ---###

cmds = [
	# 0. distinct set of references for positive annotations from a
	# genotype to a disease term and from an allele directly to a disease
	# term
	'''select distinct v._Term_key, e._Refs_key
		from VOC_Annot v, VOC_Evidence e
		where (
		    (v._AnnotType_key = %d and v._Qualifier_key != %d)
		    or v._AnnotType_key = %d
		    )
		and v._Annot_key = e._Annot_key''' % (
			DiseasePortalUtils.OMIM_GENOTYPE,
			DiseasePortalUtils.NOT_QUALIFIER,
			DiseasePortalUtils.OMIM_ALLELE)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Term_key', '_Refs_key' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_term_to_reference'

# global instance of a TermToReferenceGatherer
gatherer = TermToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
