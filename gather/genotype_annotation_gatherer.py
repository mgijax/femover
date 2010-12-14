#!/usr/local/bin/python
# 
# gathers data for the 'genotype_annotation' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class GenotypeAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the genotype_annotation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for annotations to genotypes,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		
		# produce a cache with the header term for each MP term

		columns, rows = self.results[0]

		termCol = Gatherer.columnNumber (columns, 'term')
		descCol = Gatherer.columnNumber (columns,
			'_DescendentObject_key')

		headers = {}
		for r in rows:
			headers[r[descCol]] = r[termCol]

		logger.debug('Cached headers for %d terms' % len(headers))

		# remember the cache for post-processing

		self.headers = headers

		# the second query contains the bulk of the data we need, with
		# the rest to come via post-processing

		self.finalColumns = self.results[1][0]
		self.finalResults = self.results[1][1]
		return

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Term_key') 
		vocCol = Gatherer.columnNumber (self.finalColumns,
			'_Vocab_key')
		evCol = Gatherer.columnNumber (self.finalColumns,
			'_EvidenceTerm_key')
		aqCol = Gatherer.columnNumber (self.finalColumns,
			'_Qualifier_key')
		refCol = Gatherer.columnNumber (self.finalColumns,
			'_Refs_key')

		for r in self.finalResults:
			self.addColumn ('vocab', Gatherer.resolve (
				r[vocCol], 'voc_vocab', '_Vocab_key', 'name'),
				r, self.finalColumns)
			self.addColumn ('evidenceTerm', Gatherer.resolve (
				r[evCol]), r, self.finalColumns)
			self.addColumn ('annotQualifier',
				Gatherer.resolve (aqCol), r,
				self.finalColumns)
			self.addColumn ('jnumID', Gatherer.resolve (
				r[refCol], 'BIB_Citation_Cache', '_Refs_key',
				'jnumID'), r, self.finalColumns)
			if self.headers.has_key(r[keyCol]):
				self.addColumn ('headerTerm',
					self.headers[r[keyCol]], r,
					self.finalColumns)
			else:
				self.addColumn ('headerTerm',
					None, r, self.finalColumns)
		return

###--- globals ---###

cmds = [
	'''select distinct d._Object_key, tt.term, dc._DescendentObject_key
		from dag_node d, dag_edge e, dag_node a, voc_term tt,
			dag_closure dc
		where d._Object_key = tt._Term_key
			and a._Object_key = 49772
			and e._Parent_key = a._Node_key
			and e._Child_key = d._Node_key
			and d._Object_key = dc._AncestorObject_key''',
			
	'''select distinct va._Object_key as _Genotype_key,
			  vat.name as annotType,
			  vt._Vocab_key,
			  vt.term as term,
			  vt._Term_key,
			  aa.accID as termID,
			  va._Annot_key,
			  ve._EvidenceTerm_key,
			  va._Qualifier_key,
			  ve._Refs_key
			from voc_annottype vat,
			  voc_annot va,
			  voc_evidence ve,
			  voc_term vt,
			  acc_accession aa
			where vat._MGIType_key = 12
			  and vat._AnnotType_key = va._AnnotType_key
			  and va._Term_key = vt._Term_key
			  and vt._Term_key = aa._Object_key
			  and aa._MGIType_key = 13
			  and aa.preferred = 1
			  and va._Annot_key = ve._Annot_key''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Genotype_key', 'annotType', 'vocab', 'term',
	'termID', '_Annot_key', 'annotQualifier', 'evidenceTerm', '_Refs_key',
	'jnumID', 'headerTerm',
	]

# prefix for the filename of the output file
filenamePrefix = 'genotype_annotation'

# global instance of a GenotypeAnnotationGatherer
gatherer = GenotypeAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
