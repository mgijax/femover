#!/usr/local/bin/python
# 
# gathers data for the 'hdp_annotation' table in the front-end database
#
# 07/19/2013	lec
#	- TR11423/Human Disease Portal
#

import Gatherer
import logger

###
GT_ROSA = 37270

###--- Classes ---###

#HDPAnnotationGatherer = Gatherer.Gatherer
class HDPAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the actual_database table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for actual dbs,
	#	collates results, writes tab-delimited text file

        def collateResults(self):

		#
		# process final SQL (0)
		#
		self.finalResults = []
		self.finalColumns = ['_Marker_key', 
				'_Term_key', 
				'_AnnotType_key', 
				'_Object_key', 
				'_Allele_key',
				'isConditional',
				'term',
				'accID',
				'name'
			]

		(cols, rows) = self.results[0]

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
		alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
		isConditionalCol = Gatherer.columnNumber (cols, 'isConditional')
		termCol = Gatherer.columnNumber (cols, 'term')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')

		#
		# dictionary of distinct genotype/marker keys
		#
	
		genomarkerDict = {}
		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]

			# only interested in markers with genotypes
			if genotypeKey == None:
				continue
			
			if genomarkerDict.has_key(genotypeKey):	
				if not markerKey in genomarkerDict[genotypeKey]:
					genomarkerDict[genotypeKey].append(markerKey)
			else:
				genomarkerDict[genotypeKey] = []
				genomarkerDict[genotypeKey].append(markerKey)

		#
		# if simple genotype (at most one marker) and NOT GT_ROSE,
		# then it is OK to load this row into the table
		#
		# then it is OK to load this row into the table
		# if no genotype (human/omim annotations)
		#

		for row in rows:

			ok = 1

			if genomarkerDict.has_key(row[genotypeKeyCol]):
				if len(genomarkerDict[row[genotypeKeyCol]]) == 1 and \
			   		row[markerKeyCol] == GT_ROSA:

					ok = 0

			if ok:
				self.finalResults.append ( [ row[markerKeyCol],
					     row[termKeyCol],
					     row[vocabKeyCol],
					     row[genotypeKeyCol],
					     row[alleleKeyCol],
					     row[isConditionalCol],
					     row[termCol],
					     row[termIDCol],
					     row[vocabNameCol]
					])

		return

        def postprocessResults(self):
                return

###--- globals ---###

cmds = [
	#
        # mouse/genotypes annotated to OMIM terms (1005)
        # human/genes annotated to OMIM terms (1006)
        # make human/genotype key = null
	#
	# exclude: NOT annotations genotypes
	# exclude: 'Transgenic (Reporter)' alleles (847129)
	# exclude: wild type alleles
	# exclude: genotype conditional = yes and allele driver note exists
	#
        '''
        select distinct gg._Marker_key, v._Term_key, v._AnnotType_key, 
		v._Object_key, gg._Allele_key, g.isConditional, t.term, a.accID, vv.name
        from VOC_Annot v, VOC_Term t, VOC_Vocab vv, 
		GXD_AlleleGenotype gg, GXD_Genotype g,
		ALL_Allele ag, ACC_Accession a
        where v._AnnotType_key = 1005
        and v._Qualifier_key != 1614157
        and v._Term_key = t._Term_key
        and v._Object_key = gg._Genotype_key
        and gg._Allele_key = ag._Allele_key
	and ag._Allele_Type_key != 847129
	and ag.isWildType = 0
	and gg._Genotype_key = g._Genotype_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
	and not exists (select 1 from MGI_Note n
		where g.isConditional = 1
		and gg._Allele_key = n._Object_key
		and n._MGIType_key = 11 and n._NoteType_key = 1034)
        union
        select distinct v._Object_key, v._Term_key, v._AnnotType_key, 
		null::integer, null::integer, null::integer, t.term, a.accID, vv.name
        from VOC_Annot v , VOC_Term t, VOC_Vocab vv, ACC_Accession a
        where v._AnnotType_key in (1006)
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        ''',

	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Term_key', '_AnnotType_key', 
		'_Object_key', '_Allele_key', 'isConditional', 'term', 'accID', 'name' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_annotation'

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
