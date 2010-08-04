#!/usr/local/bin/python
# 
# gathers data for the 'alleleAnnotation' table in the front-end database

import Gatherer

###--- Classes ---###

class AlleleAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the alleleAnnotation table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for allele annotations,
	#	collates results, writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		for r in self.finalResults:
			r['vocab'] = Gatherer.resolve (r['_Vocab_key'],
				'VOC_Vocab', '_Vocab_key', 'name')
			r['evidenceTerm'] = Gatherer.resolve (
				r['_EvidenceTerm_key'])
			r['annotQualifier'] = Gatherer.resolve (
				r['_Qualifier_key'])
			r['jnumID'] = Gatherer.resolve (r['_Refs_key'],
				'BIB_Citation_Cache', '_Refs_key', 'jnumID')
			r['headerTerm'] = Gatherer.resolve (
				r['_AncestorObject_key'])
		return

###--- globals ---###

cmds = [
	'''select distinct d._Object_key, tt.term
		into #mp_headers
		from DAG_Node d, DAG_Edge e, DAG_Node a, VOC_Term tt
		where d._Object_key = tt._Term_key
			and a._Object_key = 49772
			and e._Parent_key = a._Node_key
			and e._Child_key = d._Node_key''',
			
	'create unique index #tmp_index2 on #mp_headers (_Object_key)',

	# maps from MP header terms (ancestors) to their descendents
	'''select dc._AncestorObject_key, dc._DescendentObject_key
		into #tmp_headers
		from #mp_headers t, DAG_Closure dc
		where t._Object_key = dc._AncestorObject_key''',

	# index for quick lookup of ancestor by descendent
	'''create index #tmp_index1 on #tmp_headers (_DescendentObject_key,
		_AncestorObject_key)''',

	'''select distinct gap._Allele_key,
			  vat.name as annotType,
			  vt._Vocab_key,
			  vt.term as term,
			  aa.accID as termID,
			  va._Annot_key,
			  ve._EvidenceTerm_key,
			  va._Qualifier_key,
			  ve._Refs_key,
			  t._AncestorObject_key
			from VOC_AnnotType vat,
			  VOC_Annot va,
			  VOC_Evidence ve,
			  VOC_Term vt,
			  ACC_Accession aa,
			  GXD_AlleleGenotype gap,
			  #tmp_headers t
			where vat._MGIType_key = 12
			  and va._Object_key = gap._Genotype_key
			  and vt._Term_key = t._DescendentObject_key
			  and vat._AnnotType_key = va._AnnotType_key
			  and va._Term_key = vt._Term_key
			  and vt._Term_key = aa._Object_key
			  and aa._MGIType_key = 13
			  and aa.preferred = 1
			  and va._Annot_key = ve._Annot_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Allele_key', 'annotType', 'vocab', 'term',
	'termID', '_Annot_key', 'annotQualifier', 'evidenceTerm', '_Refs_key',
	'jnumID', 'headerTerm',
	]

# prefix for the filename of the output file
filenamePrefix = 'alleleAnnotation'

# global instance of a AlleleAnnotationGatherer
gatherer = AlleleAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
