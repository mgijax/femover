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
#	select all genotype (via allele) genotype/OMIM annotations (_AnnotType_key = 1012)
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

mouseLocation = {}
simpleGenotype = []

#coordinateDisplay = 'Chr%s:%0.2f-%0.2f (%s) %s' 
coordinateDisplay = 'Chr%s:%s-%s (%s) %s' 
locationDisplay1 = 'Chr%s %s cM'
locationDisplay2 = 'Chr%s %s'

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

def getLocationDisplay (marker, organism):
	#
	# returns coordinate display
	# returns location display
	#

	gchromosome = mouseLocation[marker][1]
	chromosome = mouseLocation[marker][2]
	startCoordinate = mouseLocation[marker][3]
	endCoordinate = mouseLocation[marker][4]
	strand = mouseLocation[marker][5]
	cmoffset = mouseLocation[marker][6]
	cytooffset = mouseLocation[marker][7]
	version = mouseLocation[marker][8]

	if startCoordinate:
		coordinate = coordinateDisplay \
			% (gchromosome, startCoordinate, endCoordinate,
		   	   strand, version)
	else:
		coordinate = ''

	if organism == 1:
		location = locationDisplay1 \
			% (chromosome, cmoffset)
	else:
		location = locationDisplay2 \
			% (chromosome, cytooffset)

	return coordinate, location

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
	#	collates results, writes tab-delimited text file

        def collateResults(self):

		global mouseLocation
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
				'location_display',
				'coordinate_display'
			]

		# sql (11)
		# mouse coordinates
		(cols, rows) = self.results[11]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')

		for row in rows:
			mouseLocation[row[markerKeyCol]] = row

		# sql (12)
		# mouse genotype/OMIM annotations
		# mouse genotype/MP annotations
		(cols, rows) = self.results[12]

		# set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
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

				coordinate, location = getLocationDisplay(\
					row[markerKeyCol],
					row[organismKeyCol])

				self.finalResults.append ( [ 
					row[markerKeyCol],
					row[organismKeyCol],
					row[termKeyCol],
					row[vocabKeyCol],
					row[genotypeKeyCol],
					SIMPLE_TYPE,
					row[termIDCol],
					row[termCol],
					row[vocabNameCol],
					location,
					coordinate
					])

                # sql (13)
		# mouse genotype/OMIM annotations : complex
		# mouse genotype/MP annotations : complex
                (cols, rows) = self.results[13]

                # set of columns for common sql fields
                genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
                organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
                termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
                vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
                termIDCol = Gatherer.columnNumber (cols, 'accID')
                termCol = Gatherer.columnNumber (cols, 'term')
                vocabNameCol = Gatherer.columnNumber (cols, 'name')

                for row in rows:

			# skip those listed as 'simple'
                        if row[genotypeKeyCol] not in simpleGenotype:

				coordinate, location = getLocationDisplay(\
					row[markerKeyCol],
					row[organismKeyCol])

                        	self.finalResults.append ( [
                                     	row[markerKeyCol],
                                     	row[organismKeyCol],
                                     	row[termKeyCol],
                                     	row[vocabKeyCol],
                                     	row[genotypeKeyCol],
                                     	COMPLEX_TYPE,
                                     	row[termIDCol],
                                     	row[termCol],
                                     	row[vocabNameCol],
				     	location,
				     	coordinate
                                	])

		# sql (14)
		# allele/genotype/OMIM annotations : simple
		(cols, rows) = self.results[14]

                # set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
                organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
                termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
                vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
                termIDCol = Gatherer.columnNumber (cols, 'accID')
                termCol = Gatherer.columnNumber (cols, 'term')
                vocabNameCol = Gatherer.columnNumber (cols, 'name')

                for row in rows:

			coordinate, location = getLocationDisplay(\
				row[markerKeyCol],
				row[organismKeyCol])

                        self.finalResults.append ( [
                                     row[markerKeyCol],
                                     row[organismKeyCol],
                                     row[termKeyCol],
                                     row[vocabKeyCol],
                                     row[genotypeKeyCol],
				     SIMPLE_TYPE,
                                     row[termIDCol],
                                     row[termCol],
                                     row[vocabNameCol],
				     location,
				     coordinate
                                ])

		# sql (15)
		# human gene/OMIM annotations
		(cols, rows) = self.results[15]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		vocabKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')

		for row in rows:

			coordinate, location = getLocationDisplay(\
				row[markerKeyCol],
				row[organismKeyCol])

			self.finalResults.append ( [ 
                                     row[markerKeyCol],
				     row[organismKeyCol],
				     row[termKeyCol],
				     row[vocabKeyCol],
				     None,
				     None,
				     row[termIDCol],
				     row[termCol],
				     row[vocabNameCol],
				     location,
				     coordinate
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
	into temporary table simpleGenotype
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
	into temporary table allGenotype
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
        # allele/genotype/OMIM annotations : simple
        '''
        select distinct gg._Marker_key, 
                m._Organism_key,
                v._Term_key, v._AnnotType_key, 
                v._Object_key,
                a.accID, t.term, vv.name
	into temporary table alleleGenotype
        from VOC_Annot v, VOC_Term t, VOC_Vocab vv, 
                GXD_AlleleGenotype gg,
                ACC_Accession a, MRK_Marker m
        where v._AnnotType_key = 1012
        and v._Term_key = t._Term_key
        and v._Object_key = gg._Genotype_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        and gg._Marker_key = m._Marker_key
        ''',

	# sql (3)
	# human gene/OMIM annotations
	'''
        select distinct v._Object_key as _Marker_key, 
		m._Organism_key,
		v._Term_key, v._AnnotType_key, 
		a.accID, t.term, vv.name
	into temporary table humanGene
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

	#
	# indexes (4-10)
	# remove genotype keys if they are not to be used
	#
	'''
	create index idx_marker_simple on simpleGenotype (_Marker_key)
	''',
	'''
	create index idx_genotype_simple on simpleGenotype (_Object_key)
	''',
	'''
	create index idx_marker_all on allGenotype (_Marker_key)
	''',
	'''
	create index idx_genotype_all on allGenotype (_Object_key)
	''',
	'''
	create index idx_marker_allele on alleleGenotype (_Marker_key)
	''',
	'''
	create index idx_genotype_allele on alleleGenotype (_Object_key)
	''',
	'''
	create index idx_marker_human on humanGene (_Marker_key)
	''',

	#
	# sql (11)
	# all coordinates
	'''
	select distinct l._Marker_key, l.genomicchromosome, l.chromosome,
		l.startcoordinate, l.endcoordinate,
		l.strand, l.cmoffset, l.cytogeneticoffset, l.version
	from MRK_Location_Cache l
	where exists (select 1 from simpleGenotype g where l._Marker_key = g._Marker_key)
	or exists (select 1 from allGenotype g where l._Marker_key = g._Marker_key)
	or exists (select 1 from alleleGenotype g where l._Marker_key = g._Marker_key)
	or exists (select 1 from humanGene g where l._Marker_key = g._Marker_key)
	''',
	
	# sql (12)
	# mouse genotype/OMIM annotations : simple
	# mouse genotype/MP annotations : simple
	'''
	select * from simpleGenotype
        ''',

        # sql (13)
	# mouse genotype/OMIM annotations : complex
	# mouse genotype/MP annotations : complex
        '''
	select * from allGenotype
        ''',

        # sql (14)
	# allele/genotype/OMIM annotations : simple
        '''
	select * from alleleGenotype
        ''',

	# sql (15)
	# human gene/OMIM annotations
	'''
	select * from humanGene
        ''',

	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Organism_key', 
		'_Term_key', '_AnnotType_key', 
		'_Object_key', 'genotype_type', 'accID', 'term', 'name',
		'location_display', 'coordinate_display' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_annotation'

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
