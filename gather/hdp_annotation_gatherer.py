#!/usr/local/bin/python
# 
# gathers data for the 'hdp_annotation' table in the front-end database
#
# Purpose:
#
# To load all mouse/genotypes annotated to OMIM terms (_AnnotType_key = 1005)
# and human/genes annotated to OMIM terms (_AnnotType_key = 1006)
# into the HDP table.
#
# 1.  select from non-fewi database:
#
#	human:
# 	select all human/gene/OMIM annotations
#
# 	mouse:
#	select all mouse genotype/OMIM annotations
# 	exclude: 'NOT' annotations
# 	exclude: allele type = 'Transgenic (Reporter)'
# 	exclude: allele = wild type
# 	exclude: genotype conditional = yes and allele driver note exists
#	selecting by genotype/marker/allele
#
#	these exclusions are used to filter/delete out as many complex genotypes
#	as possible, and leave only the set of potential simple genotypes
#	that we are interested in loaded into the HDP table.
#
# 2.  collation of results:
#
#	a) all human/gene/OMIM annotations are loaded
#
#	for mouse:
#	a) generate dictionary of distinct genotype/marker counts
#
#	b) add genotype/marker/allele to the HTDP table if:
# 		. genotype_key is null (human annotation) 
#		AND
#		. and marker is NOT Gt(ROSA)26Sor'
#	    else
#		the genotype is complex and we don't want to load it
#
# 07/19/2013	lec
#	- TR11423/Human Disease Portal
#

import Gatherer
import logger

###--- Constents ---###
GT_ROSA = 37270

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
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
				'accID',
				'term',
				'name'
			]

		# human gene/OMIM annotations
		(cols, rows) = self.results[0]

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')

		for row in rows:
			self.finalResults.append ( [ row[markerKeyCol],
				     row[termKeyCol],
				     row[vocabKeyCol],
				     '',
				     '',
				     row[termIDCol],
				     row[termCol],
				     row[vocabNameCol]
				])

		# mouse genotype/OMIM annotations
		(cols, rows) = self.results[1]

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
		alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')

		#
		# dictionary of distinct genotype/marker keys and their counts
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
		# when to set OK to true
		#
		# if genotype_key is null (human annotation)
		# if genotype/marker count == 1 and marker is NOT Gt(ROSA)26Sor'
		#

		for row in rows:

			ok = 0

			if len(genomarkerDict[row[genotypeKeyCol]]) == 1 and \
		   		row[markerKeyCol] != GT_ROSA:

				ok = 1

			if ok == 1:
				self.finalResults.append ( [ row[markerKeyCol],
					     row[termKeyCol],
					     row[vocabKeyCol],
					     row[genotypeKeyCol],
					     row[alleleKeyCol],
					     row[termIDCol],
					     row[termCol],
					     row[vocabNameCol]
					])

		return

        def postprocessResults(self):
                return

###--- globals ---###

cmds = [
	# human gene/OMIM annotations
	'''
        select distinct v._Object_key as _Marker_key, v._Term_key, v._AnnotType_key, 
		a.accID, t.term, vv.name
        from VOC_Annot v , VOC_Term t, VOC_Vocab vv, ACC_Accession a
        where v._AnnotType_key in (1006)
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        ''',

	# mouse genotype/OMIM annotations
	'''
        select distinct gg._Marker_key, v._Term_key, v._AnnotType_key, 
		v._Object_key, gg._Allele_key, a.accID, t.term, vv.name
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
        ''',

	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Term_key', '_AnnotType_key', 
		'_Object_key', '_Allele_key', 'accID', 'term', 'name' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_annotation'

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
