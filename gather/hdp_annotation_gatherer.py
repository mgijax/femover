#!/usr/local/bin/python
# 
# gathers data for the 'hdp_annotation' table in the front-end database
#
# Purpose:
#
# To load all:
#	mouse/genotypes annotated to OMIM terms (_AnnotType_key = 1005)
# 	mouse/genotypes annotationed to MP Phenotype terms (_AnnotType_key = 1002)
# 	human/genes annotated to OMIM terms (_AnnotType_key = 1006)
# into the HDP table.
#
# 1.  select from non-fewi database:
#
#	human:
# 	select all human/gene/OMIM annotations (_AnnotType_key = 1006)
#
# 	mouse:
#	select all mouse genotype/OMIM annotations (_AnnotType_key = 1005)
#	select all mouse genotype/MP annotations (_AnnotType_key = 1002)
# 	exclude: 'NOT' annotations (OMIM)
# 	exclude: 'normal' annotations (MP)
# 	exclude: allele type = 'Transgenic (Reporter)'
# 	exclude: allele = wild type
# 	exclude: genotype conditional = yes and allele driver note exists
#	selecting by genotype/marker
#
#	selecting by genotype/marker
#	these exclusions are used to filter/delete out as many complex genotypes
#	as possible, and leave only the set of potential simple genotypes
#	that we are interested in loading into the HDP table.
#
#	mouse:
#	select all allele/OMIM annotations (_AnnotType_key = 1012)
#
# 2.  collation of results:
#
#	a) all human/gene/OMIM annotations are loaded
#
#	for mouse:
#	a) generate dictionary of distinct genotype/marker counts
#
#	b) add genotype/marker/allele to the HTDP table if:
# 		. genotype contains at most one gene
#		AND
#		. marker is NOT 'Gt(ROSA)26Sor'
#
#	c) all gene (via allele)/OMIM annotations are loaded
#	
# 07/19/2013	lec
#	- TR11423/Human Disease Portal
#

import Gatherer
import logger

###--- Constents ---###
GT_ROSA = 37270
SIMPLE_TYPE = 'simple'
COMPLEX_TYPE = 'complex'

simpleGenotype = []

###--- Functions ---###

def getGeneCount (rows, genotypeKeyCol, markerKeyCol):
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

	return genomarkerDict

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
	#	collates results, writes tab-delimited text file

        def collateResults(self):

		global simpleGenotype

		#
		# process final SQL
		#
		self.finalResults = []
		self.finalColumns = ['_Marker_key', 
				'_Organism_key', 
				'_Term_key', 
				'_AnnotType_key', 
				'_Object_key', 
				'genotype_type',
				'accID',
				'term',
				'name',
			]

		# sql (0)
		# mouse genotype/OMIM annotations
		# mouse genotype/MP annotations
		(cols, rows) = self.results[0]

		# set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')

		# dictionary of distinct genotype/marker keys and their counts
		genomarkerDict = getGeneCount(rows, genotypeKeyCol, markerKeyCol)

		#
		# if the genotype is simple (at most one marker)
		# and the marker is NOT ''Gt(ROSA)26Sor'
		#

		for row in rows:
			if len(genomarkerDict[row[genotypeKeyCol]]) == 1 and \
	   			row[markerKeyCol] != GT_ROSA:

				# save simple genotype list
				simpleGenotype.append(row[genotypeKeyCol])

				self.finalResults.append ( [ 
					row[markerKeyCol],
					row[organismKeyCol],
					row[termKeyCol],
					row[annotTypeKeyCol],
					row[genotypeKeyCol],
					SIMPLE_TYPE,
					row[termIDCol],
					row[termCol],
					row[vocabNameCol],
					])

                # sql (1)
		# mouse genotype/OMIM annotations : complex
		# mouse genotype/MP annotations : complex
                (cols, rows) = self.results[1]

                # set of columns for common sql fields
                genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
                organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
                termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
                annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
                termIDCol = Gatherer.columnNumber (cols, 'accID')
                termCol = Gatherer.columnNumber (cols, 'term')
                vocabNameCol = Gatherer.columnNumber (cols, 'name')

                for row in rows:

			# skip those listed as 'simple'
                        if row[genotypeKeyCol] not in simpleGenotype:

                        	self.finalResults.append ( [
                                     	row[markerKeyCol],
                                     	row[organismKeyCol],
                                     	row[termKeyCol],
                                     	row[annotTypeKeyCol],
                                     	row[genotypeKeyCol],
                                     	COMPLEX_TYPE,
                                     	row[termIDCol],
                                     	row[termCol],
                                     	row[vocabNameCol],
                                	])

		# sql (2)
		# allele/OMIM annotations
		(cols, rows) = self.results[2]

                # set of columns for common sql fields
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
                organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
                termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
                annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
                termIDCol = Gatherer.columnNumber (cols, 'accID')
                termCol = Gatherer.columnNumber (cols, 'term')
                vocabNameCol = Gatherer.columnNumber (cols, 'name')

                for row in rows:

			# no genotypes
                        self.finalResults.append ( [
                                     row[markerKeyCol],
                                     row[organismKeyCol],
                                     row[termKeyCol],
                                     row[annotTypeKeyCol],
                                     None,
				     None,
                                     row[termIDCol],
                                     row[termCol],
                                     row[vocabNameCol],
                                ])

		# sql (3)
		# human gene/OMIM annotations
		(cols, rows) = self.results[3]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')

		for row in rows:

			# no genotypes
			self.finalResults.append ( [ 
                                     row[markerKeyCol],
				     row[organismKeyCol],
				     row[termKeyCol],
				     row[annotTypeKeyCol],
				     None,
				     None,
				     row[termIDCol],
				     row[termCol],
				     row[vocabNameCol],
				])

		return

        def postprocessResults(self):
                return

###--- globals ---###

cmds = [
        # sql (0)
        # mouse genotype/OMIM annotations : simple
        # mouse genotype/MP annotations : simple
        '''
        select distinct gg._Marker_key, 
                m._Organism_key,
                v._Term_key, v._AnnotType_key, 
                v._Object_key, 
                a.accID, t.term, vv.name
        from VOC_Annot v, VOC_Term t, VOC_Vocab vv, 
                GXD_AlleleGenotype gg, GXD_Genotype g,
                ALL_Allele ag, ACC_Accession a, MRK_Marker m
        where (
               (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)
                or
               (v._AnnotType_key = 1002 and v._Qualifier_key != 2181424)
               )
        and v._Term_key = t._Term_key
        and v._Object_key = gg._Genotype_key
        and gg._Allele_key = ag._Allele_key
        and gg._Genotype_key = g._Genotype_key
        and ag._Allele_Type_key != 847129
        and ag.isWildType = 0
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        and not exists (select 1 from MGI_Note n
                where g.isConditional = 1
                and gg._Allele_key = n._Object_key
                and n._MGIType_key = 11 and n._NoteType_key = 1034)
        and gg._Marker_key = m._Marker_key
        ''',

        # sql (1)
	# mouse genotype/OMIM annotations : complex
	# mouse genotype/MP annotations : complex
        '''
        select distinct gg._Marker_key,
                m._Organism_key,
                v._Term_key, v._AnnotType_key,
                v._Object_key,
                a.accID, t.term, vv.name
        from VOC_Annot v, VOC_Term t, VOC_Vocab vv,
                GXD_AlleleGenotype gg,
                ACC_Accession a, MRK_Marker m
        where ((v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)
                or
               (v._AnnotType_key = 1002 and v._Qualifier_key != 2181424)
               )
        and v._Term_key = t._Term_key
        and v._Object_key = gg._Genotype_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        and gg._Marker_key = m._Marker_key
        ''',

        # sql (2)
        # allele/OMIM annotations
        '''
        select distinct m._Marker_key,
                m._Organism_key,
                v._Term_key, v._AnnotType_key, 
                a.accID, t.term, vv.name
        from VOC_Annot v , VOC_Term t, VOC_Vocab vv, ACC_Accession a, 
	     ALL_Allele al, MRK_Marker m
        where v._AnnotType_key = 1012
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        and v._Object_key = al._Allele_key
	and al._Marker_key = m._Marker_key
        ''',

	# sql (3)
	# human gene/OMIM annotations
	'''
        select distinct v._Object_key as _Marker_key, 
		m._Organism_key,
		v._Term_key, v._AnnotType_key, 
		a.accID, t.term, vv.name
        from VOC_Annot v , VOC_Term t, VOC_Vocab vv, ACC_Accession a, MRK_Marker m
        where v._AnnotType_key = 1006
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
	and v._Object_key = m._Marker_key
        ''',

	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Organism_key', 
		'_Term_key', '_AnnotType_key', 
		'_Object_key', 'genotype_type', 'accID', 'term', 'name', ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_annotation'

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
