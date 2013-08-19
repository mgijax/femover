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

###--- Constents ---###
GT_ROSA = 37270
SIMPLE_TYPE = 'simple'
COMPLEX_TYPE = 'complex'

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

class HDPAnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
	#	collates results, writes tab-delimited text file

        def collateResults(self):

		simpleGenotype = set([])

		#
		# hdp_annotation
		#
		annotResults = []
		annotCols = ['_Marker_key', 
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
				simpleGenotype.add(row[genotypeKeyCol])

				annotResults.append ( [ 
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

                        	annotResults.append ( [
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
                        annotResults.append ( [
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
			annotResults.append ( [ 
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

		#
		# hdp_gridcluster/hdp_gridcluster_marker
		#
		clusterResults = []
		clusterCols = ['_Cluster_key',
			]

		markerResults = []
		markerCols = ['_Cluster_key',
				'_Marker_key', 
				'_Organism_key', 
				'symbol',
			]

		# sql (6) : annotations that contain homologene clusters
		(cols, rows) = self.results[6]

		# set of columns for common sql fields
		clusterKeyCol = Gatherer.columnNumber (cols, '_Cluster_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

		clusterKey = 1
		clusterLookup = set([])

                # at most one marker should exist in a gridCluster
                markerLookup = set([])

		for row in rows:

			clusterKey = row[clusterKeyCol]
			if clusterKey not in clusterLookup:
				clusterResults.append ( [ 
                                     	clusterKey,
					])
				clusterLookup.add(clusterKey)

			markerKey = row[markerKeyCol]
			if markerKey not in markerLookup:
				markerResults.append ( [ 
				    	row[clusterKeyCol],
				    	markerKey,
				    	row[organismKeyCol],
				    	row[symbolCol],
					])
				markerLookup.add(markerKey)

		# sql (7) : mouse/human markers with annotations that do NOT contain homologene clusters
		(cols, rows) = self.results[7]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

		for row in rows:

			clusterKey = clusterKey + 1
			clusterResults.append ( [ 
                                clusterKey,
				])

			markerKey = row[markerKeyCol]
			if markerKey not in markerLookup:
				markerResults.append ( [ 
                                    		clusterKey,
						markerKey,
			     			row[organismKeyCol],
			     			row[symbolCol],
					])
				markerLookup.add(markerKey)

		# push data to output files
		self.output.append((annotCols, annotResults))
		self.output.append((clusterCols, clusterResults))
		self.output.append((markerCols, markerResults))

		return

        def postprocessResults(self):
                return

###--- globals ---###

cmds = [
	#
	# hdp_annotation table
	#

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
	# exclude Gt(ROSA)
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
	and m._Marker_key != %s
        ''' % (GT_ROSA),

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

        #
        # hdp_gridcluster/hdp_gridcluster_marker tables
        #

	# sql (4-5) : super-simple genotypes
	'''
	select _Genotype_key 
	into temporary table temp_genotype
	from GXD_AlleleGenotype 
	group by _Genotype_key having count(*) = 1
	''',

	'''
	create index idx_genotype on temp_genotype (_Genotype_key)
	''',

        # sql (6) : markers that contain homologene clusters
        #       by mouse marker/OMIM (1005) for super-simple genotypes
        #       by mouse marker/MP (1002) for super-simple genotypes
        #       by mouse marker (via allele)/OMIM (1012)
        #       by human marker/OMIM (1006)
        #
        # only include if the genotype is a super-simple genotype
        # that is, the genotype has only one marker
        # and merker is NOT Gt(ROSA)
        #
        # select all *distinct* homologene clusters that contain a mouse/human HPD
        # annotation.  then select all of the mouse/human markers that are contained
        # within each of those clusters
        #
        '''
	WITH
	temp_homologene AS
	(select distinct c._Cluster_key
        	from MRK_ClusterMember c
        	where exists (select 1 from VOC_Annot v, GXD_AlleleGenotype g, temp_genotype t
			where t._Genotype_key = g._Genotype_key
                	and g._Marker_key = c._Marker_key
                	and g._Marker_key != 37270
                	and g._Genotype_key = v._Object_key
                	and v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)
        	or exists (select 1 from VOC_Annot v, GXD_AlleleGenotype g, temp_genotype t
			where t._Genotype_key = g._Genotype_key
                	and g._Marker_key = c._Marker_key
                	and g._Marker_key != 37270
                	and g._Genotype_key = v._Object_key
                	and v._AnnotType_key = 1002 and v._Qualifier_key != 2181424)
        	or exists (select 1 from VOC_Annot v, ALL_Allele a
                	where c._Marker_key = a._Marker_key
                	and a._Allele_key = v._Object_key
                	and v._AnnotType_key = 1012)
        	or exists (select 1 from VOC_Annot v
                	where c._Marker_key = v._Object_key
                	and v._AnnotType_key = 1006)
	)
	select c._Cluster_key, c._Marker_key, m._Organism_key, m.symbol
	from temp_homologene h, MRK_ClusterMember c, MRK_Marker m
	where h._Cluster_key = c._Cluster_key
	and c._Marker_key = m._Marker_key
	and m._Organism_key in (1,2)
	order by c._Cluster_key
	''',

        # sql (7) : mouse with OMIM annotations where mouse does NOT contain homologene clusters
        #           mouse with MP annotations where mouse does NOT contain homologene clusters
        #           mouse (by allele) with MP annotations where mouse does NOT contain homologene clusters
        #           human with OMIM annotations where mouse does NOT contain homologene clusters
        # include only super-simple genotypes
        # exclude Gt(ROSA)
	'''
	select m._Marker_key, m._Organism_key, m.symbol
	from MRK_Marker m
	where exists (select 1 from VOC_Annot v, GXD_AlleleGenotype g, temp_genotype t
			where t._Genotype_key = g._Genotype_key
                        and g._Marker_key = m._Marker_key
                        and g._Genotype_key = v._Object_key
                        and g._Marker_key != 37270
               		and v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)

	union

	select m._Marker_key, m._Organism_key, m.symbol
	from MRK_Marker m
	where exists (select 1 from VOC_Annot v, GXD_AlleleGenotype g, temp_genotype t
			where t._Genotype_key = g._Genotype_key
                        and g._Marker_key = m._Marker_key
                        and g._Genotype_key = v._Object_key
                        and g._Marker_key != 37270
                        and v._AnnotType_key = 1002 and v._Qualifier_key != 2181424)

	union

        select m._Marker_key, m._Organism_key, m.symbol
	from VOC_Annot v, ALL_Allele a, MRK_Marker m
        where v._AnnotType_key = 1012
        and v._Object_key = a._Allele_key 
	and a._Marker_key = m._Marker_key
        and m._Marker_key != 37270
	and not exists (select 1 from MRK_ClusterMember c where m._Marker_key = c._Marker_key)

	union

	select v._Object_key as _Marker_key, m._Organism_key, m.symbol
	from VOC_Annot v, MRK_Marker m
        where v._AnnotType_key = 1006
        and v._Object_key = m._Marker_key
	and not exists (select 1 from MRK_ClusterMember c where m._Marker_key = c._Marker_key)
	''',
	]

# prefix for the filename of the output file
files = [
	('hdp_annotation',
		[ Gatherer.AUTO, '_Marker_key', '_Organism_key', 
			'_Term_key', '_AnnotType_key', 
			'_Object_key', 'genotype_type', 'accID', 'term', 'name', ],
          'hdp_annotation'),

	('hdp_gridcluster',
		[ '_Cluster_key' ],
          'hdp_gridcluster'),

	('hdp_gridcluster_marker',
		[ Gatherer.AUTO, '_Cluster_key', '_Marker_key',
		  '_Organism_key', 'symbol' ],
          'hdp_gridcluster_marker'),
	]

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
