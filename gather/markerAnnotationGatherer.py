#!/usr/local/bin/python
# 
# gathers data for the 'markerAnnotation' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerAnnotation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker annotations,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker,
		#	rather than for all markers

		if self.keyField == 'markerKey':
			return 'va._Object_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		for r in self.finalResults:
			r['vocab'] = sybaseUtil.resolve (r['_Vocab_key'],
				'VOC_Vocab', '_Vocab_key', 'name')
			r['evidenceTerm'] = sybaseUtil.resolve (
				r['_EvidenceTerm_key'])
			r['annotQualifier'] = sybaseUtil.resolve (
				r['_Qualifier_key'])
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
			from VOC_AnnotType vat,
			  VOC_Annot va,
			  VOC_Evidence ve,
			  VOC_Term vt,
			  ACC_Accession aa,
			  BIB_Citation_Cache bc
			where vat._MGIType_key = 2
			  and vat._AnnotType_key = va._AnnotType_key
			  and va._Term_key = vt._Term_key
			  and vt._Term_key = aa._Object_key
			  and aa._MGIType_key = 13
			  and aa.preferred = 1
			  and va._Annot_key = ve._Annot_key
			  and ve._Refs_key = bc._Refs_key %s''',
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
