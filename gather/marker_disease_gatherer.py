#!/usr/local/bin/python
# 
# gathers data for the 'marker_disease' table in the front-end database

import Gatherer
import logger
import GenotypeClassifier

###--- Constants ---###

OMIM_GENOTYPE = 1005		# from VOC_AnnotType
OMIM_HUMAN_MARKER = 1006	# from VOC_AnnotType
NOT_QUALIFIER = 1614157		# from VOC_Term
TERM_MGITYPE = 13		# from ACC_MGIType
DRIVER_NOTE = 1034		# from MGI_NoteType
GT_ROSA = 37270			# MRK_Marker for 'Gt(ROSA)26Sor'

###--- Classes ---###

class MarkerDiseaseGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_disease table
	# Has: queries to execute against the source database
	# Does: queries the source database for disease data for mouse and
	#	human markers, collates results, writes tab-delimited text
	#	file

	def collateResults (self):
		seqNum = 0
		outputCols = [ '_Marker_key', 'term', 'accID', 'name',
			'sequenceNum' ]
		outputRows = []

		# first do annotations to mouse markers' alleles' genotypes

		cols, rows = self.results[0]

		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbCol = Gatherer.columnNumber (cols, 'name')
		termCol = Gatherer.columnNumber (cols, 'term')

		thisMarker = None
		thisMarkersDiseases = []

		for row in rows:
			designation = GenotypeClassifier.getClass (
				row[genotypeCol])

			# skip complex genotypes that are not conditional
			if designation == 'cx':
				continue

			marker = row[markerCol]
			omimID = row[idCol]

			if marker != thisMarker:
				thisMarker = marker
				thisMarkersDiseases = [ omimID ]

			# if we've already added this disease for this marker,
			# skip it
			elif omimID in thisMarkersDiseases:
				continue

			else:
				thisMarkersDiseases.append(omimID)

			seqNum = seqNum + 1

			outputRows.append ( [ marker, row[termCol], omimID,
				row[ldbCol], seqNum ] )

		# second do annotations to human markers

		cols, rows = self.results[1]

		markerCol = Gatherer.columnNumber (cols, '_Marker_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		ldbCol = Gatherer.columnNumber (cols, 'name')
		termCol = Gatherer.columnNumber (cols, 'term')

		for row in rows:
			seqNum = seqNum + 1
			outputRows.append ( [ row[markerCol], row[termCol],
				row[idCol], row[ldbCol], seqNum ] )

		# remember the results
		self.finalColumns = outputCols
		self.finalResults = outputRows
		return

###--- globals ---###

cmds = [
	# 0. pull OMIM annotations up from genotypes to mouse markers.
	#    Exclude paths from markers to genotypes which involve:
	#	a. recombinase alleles (ones with driver notes)
	#	b. wild-type alleles
	#	c. complex, not conditional genotypes
	#	d. complex, not conditional genotypes with transgenes
	#	e. marker Gt(ROSA)
	'''select distinct gag._Marker_key,
		gag._Genotype_key,
		vt.term,
		aa.accID, 
		ldb.name
	from gxd_genotype gg,
		gxd_allelegenotype gag,
		voc_annot va,
		voc_term vt,
		acc_accession aa,
		acc_logicaldb ldb,
		all_allele a
	where gg._Genotype_key = gag._Genotype_key
		and gg._Genotype_key = va._Object_key
		and va._AnnotType_key = %d
		and va._Qualifier_key != %d
		and va._Term_key = vt._Term_key
		and vt._Term_key = aa._Object_key
		and aa._MGIType_key = %d
		and aa.private = 0
		and aa.preferred = 1
		and gag._Allele_key = a._Allele_key
		and a.isWildType = 0
		and not exists (select 1 from MGI_Note mn
			where mn._NoteType_key = %d
			and mn._Object_key = gag._Allele_key)
		and aa._LogicalDB_key = ldb._LogicalDB_key
		and gag._Marker_key != %d
	order by gag._Marker_key, vt.term''' % (
		OMIM_GENOTYPE, NOT_QUALIFIER, TERM_MGITYPE, DRIVER_NOTE,
		GT_ROSA),	

	# 1. get OMIM annotations to human markers
	'''select distinct va._Object_key as _Marker_key,
		vt.term,
		aa.accID,
		ldb.name
	from voc_annot va,
		voc_term vt,
		acc_accession aa,
		acc_logicaldb ldb
	where va._AnnotType_key = 1006
		and va._Qualifier_key != %d
		and va._Term_key = vt._Term_key
		and vt._Term_key = aa._Object_key
		and aa._MGIType_key = 13
		and aa.private = 0
		and aa.preferred = 1
		and aa._LogicalDB_key = ldb._LogicalDB_key
	order by va._Object_key, vt.term''' % NOT_QUALIFIER, 
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, '_Marker_key', 'term', 'accID', 'name', 'sequenceNum'
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_disease'

# global instance of a MarkerDiseaseGatherer
gatherer = MarkerDiseaseGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
