#!/usr/local/bin/python
# 
# gathers data for the 'markerAnnotation' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerAnnotation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker annotations,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		self.convertFinalResultsToList()

		vocCol = Gatherer.columnNumber (self.finalColumns,
			'_Vocab_key')
		etCol = Gatherer.columnNumber (self.finalColumns,
			'_EvidenceTerm_key')
		aqCol = Gatherer.columnNumber (self.finalColumns,
			'_Qualifier_key')

		for r in self.finalResults:
			self.addColumn ('vocab', Gatherer.resolve (
				r[vocCol], 'voc_vocab', '_Vocab_key', 'name'),
				r, self.finalColumns)
			self.addColumn ('evidenceTerm', Gatherer.resolve (
				r[etCol]), r, self.finalColumns)
			self.addColumn ('annotQualifier', Gatherer.resolve (
				r[aqCol]), r, self.finalColumns)
		return

###--- globals ---###

cmds = [ '''select distinct va._Object_key as _Marker_key,
			  vat.name as annotType,
			  vt._Vocab_key,
			  vt.term as term,
			  aa.accID as termID,
			  va._Annot_key,
			  ve._EvidenceTerm_key,
			  va._Qualifier_key,
			  ve._Refs_key,
			  bc.jnumID
			from voc_annottype vat,
			  voc_annot va,
			  voc_evidence ve,
			  voc_term vt,
			  acc_accession aa,
			  bib_citation_cache bc
			where vat._MGIType_key = 2
			  and vat._AnnotType_key = va._AnnotType_key
			  and va._Term_key = vt._Term_key
			  and vt._Term_key = aa._Object_key
			  and aa._MGIType_key = 13
			  and aa.preferred = 1
			  and va._Annot_key = ve._Annot_key
			  and ve._Refs_key = bc._Refs_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', 'annotType', 'vocab', 'term',
	'termID', '_Annot_key', 'annotQualifier', 'evidenceTerm', '_Refs_key',
	'jnumID',
	]

# prefix for the filename of the output file
filenamePrefix = 'markerAnnotation'

# global instance of a MarkerAnnotationGatherer
gatherer = MarkerAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
