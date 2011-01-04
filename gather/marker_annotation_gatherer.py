#!/usr/local/bin/python
# 
# gathers data for the 'markerAnnotation' table in the front-end database

import Gatherer
import VocabSorter

###--- Classes ---###

class MarkerAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the markerAnnotation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for marker annotations,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# join the two lists of results together, assuming that they
		# have the same column definitions

		combined = []
		for set in [ self.results[0][1], self.results[1][1],
				self.results[2][1] ]:
			for row in set:
				combined.append(row)

		self.results.append ( [ self.results[0][0], combined ] )
		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]
		return

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		self.convertFinalResultsToList()

		vocCol = Gatherer.columnNumber (self.finalColumns,
			'_Vocab_key')
		etCol = Gatherer.columnNumber (self.finalColumns,
			'_EvidenceTerm_key')
		aqCol = Gatherer.columnNumber (self.finalColumns,
			'_Qualifier_key')
		termCol = Gatherer.columnNumber (self.finalColumns,
			'_Term_key')

		for r in self.finalResults:
			self.addColumn ('vocab', Gatherer.resolve (
				r[vocCol], 'voc_vocab', '_Vocab_key', 'name'),
				r, self.finalColumns)
			if r[etCol]:
				self.addColumn ('evidenceTerm',
					Gatherer.resolve (
					r[etCol]), r, self.finalColumns)
			else:
				self.addColumn ('evidenceTerm', None,
					r, self.finalColumns)

			if r[aqCol]:
				self.addColumn ('annotQualifier',
					Gatherer.resolve (
					r[aqCol]), r, self.finalColumns)
			else:
				self.addColumn ('annotQualifier', None,
					r, self.finalColumns)

			self.addColumn ('sequenceNum',
				VocabSorter.getSequenceNum (r[termCol]),
				r, self.finalColumns)
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
			  bc.jnumID,
			  vt._Term_key
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

	# Protein Ontology terms
	'''select distinct m._Object_key as _Marker_key,
			'Protein Ontology' as annotType,
			vt._Vocab_key,
			vt.term as term,
			p.accID as termID,
			null as _Annot_key,
			null as _EvidenceTerm_key,
			null as _Qualifier_key,
			null as _Refs_key,
			null as jnumID,
			vt._Term_key
		from acc_accession m,
			acc_accession p,
			voc_term vt,
			acc_actualdb adb
		where m._LogicalDB_key = 135
			and m._MGIType_key = 2
			and m.accID = p.accID
			and p._LogicalDB_key = 135
			and p._MGIType_key = 13
			and p._Object_key = vt._Term_key
			and p._LogicalDB_key = adb._LogicalDB_key''',

	# bring in and cache MP and OMIM annotations for each marker's alleles
	'''select distinct a._Marker_key,
			  vat.name as annotType,
			  vt._Vocab_key,
			  vt.term as term,
			  aa.accID as termID,
			  va._Annot_key,
			  ve._EvidenceTerm_key,
			  va._Qualifier_key,
			  ve._Refs_key,
			  bc.jnumID,
			  vt._Term_key
			from all_allele a,
			  gxd_allelegenotype g,
			  voc_annottype vat,
			  voc_annot va,
			  voc_evidence ve,
			  voc_term vt,
			  acc_accession aa,
			  bib_citation_cache bc
			where vat._MGIType_key = 12
			  and vat._AnnotType_key = va._AnnotType_key
			  and va._Object_key = g._Genotype_key
			  and g._Allele_key = a._Allele_key
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
	'jnumID', 'sequenceNum',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_annotation'

# global instance of a MarkerAnnotationGatherer
gatherer = MarkerAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
