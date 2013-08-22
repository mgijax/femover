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
#	hdp_annotation:
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
#	hdp_gridcluster/hdp_gridcluster_marker
#
#	a) markers that contain/don't contain homologene clusters
#       	by mouse marker/OMIM (1005) for super-simple genotypes
#       	by mouse marker/MP (1002) for super-simple genotypes
#       	by mouse marker (via allele)/OMIM (1012)
#       	by human marker/OMIM (1006)
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

	logger.debug ('processed mouse gene counts')

	return genomarkerDict

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
	#	collates results, writes tab-delimited text file

        def collateResults(self):

		#
		# sql (0)
		# mp term -> mp header term
		mpHeaderDict = {}
		(cols, rows) = self.results[0]
		key = Gatherer.columnNumber (cols, '_Object_key')
		value = Gatherer.columnNumber (cols, 'synonym')
		for row in rows:
			mpHeaderDict[row[key]] = []
			mpHeaderDict[row[key]].append(row[value])
		#logger.debug (mpHeaderDict)

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
				'mp_header'
			]

		# sql (1)
		# mouse genotype/OMIM annotations
		# mouse genotype/MP annotations
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

		# dictionary of distinct genotype/marker keys and their counts
		genomarkerDict = getGeneCount(rows, genotypeKeyCol, markerKeyCol)

		# list of simple genotypes
		simpleGenotypeList = set([])

		#
		# if the genotype is simple (at most one marker)
		# and the marker is NOT 'Gt(ROSA)26Sor'
		#

		for row in rows:
			if len(genomarkerDict[row[genotypeKeyCol]]) == 1 \
				and row[markerKeyCol] != GT_ROSA:

				# save simple genotype list
				simpleGenotypeList.add(row[genotypeKeyCol])

				termKey = row[termKeyCol]
				if mpHeaderDict.has_key(termKey):
					mpHeader = mpHeaderDict[termKey][0]
				else:
					mpHeader = None

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
					mpHeader,
					])

		logger.debug ('processed simple mouse annotations')

                # sql (2)
		# mouse genotype/OMIM annotations : complex
		# mouse genotype/MP annotations : complex
                (cols, rows) = self.results[2]

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
                        if row[genotypeKeyCol] not in simpleGenotypeList:

				termKey = row[termKeyCol]
				if mpHeaderDict.has_key(termKey):
					mpHeader = mpHeaderDict[termKey][0]
				else:
					mpHeader = None

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
					mpHeader,
                                	])
		logger.debug ('processed complex mouse annotations')

		# sql (3)
		# allele/OMIM annotations
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

			termKey = row[termKeyCol]
			if mpHeaderDict.has_key(termKey):
				mpHeader = mpHeaderDict[termKey][0]
			else:
				mpHeader = None

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
				     mpHeader,
                                ])
		logger.debug ('processed allele/OMIM annotatins')

		# sql (4)
		# human gene/OMIM annotations
		(cols, rows) = self.results[4]

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

			termKey = row[termKeyCol]
			if mpHeaderDict.has_key(termKey):
				mpHeader = mpHeaderDict[termKey][0]
			else:
				mpHeader = None

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
				     mpHeader,
				])
		logger.debug ('processed human OMIM annotations')

		#
		# hdp_gridcluster
		# hdp_gridcluster_marker
		# hdp_gridcluster_disease
		#
		clusterResults = []
		clusterCols = ['_Cluster_key',
				'homologene_id',
			]

		markerResults = []
		markerCols = ['_Cluster_key',
				'_Marker_key', 
				'_Organism_key', 
				'symbol',
			]

		diseaseResults = []
		diseaseCols = ['_Cluster_key',
				'_Term_key', 
				'_AnnotType_key',
				'accID', 
				'term',
			]

		# sql (12)
		# homologene clusters
		diseaseDict1 = {}
		(cols, rows) = self.results[12]
		clusterKey = Gatherer.columnNumber (cols, '_Cluster_key')
		for row in rows:
			key = row[clusterKey]
			value = row
			if not diseaseDict1.has_key(key):
				diseaseDict1[key] = []
			diseaseDict1[key].append(row)
		#logger.debug (diseaseDict1)

		# sql (13)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		diseaseDict2 = {}
		(cols, rows) = self.results[13]
		markerKey = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			key = row[markerKey]
			value = row
			if not diseaseDict2.has_key(key):
				diseaseDict2[key] = []
			diseaseDict2[key].append(row)
			key = key + 1
		logger.debug (diseaseDict2)

		# sql (14) : annotations that contain homologene clusters
		(cols, rows) = self.results[14]

		# set of columns for common sql fields
		clusterKeyCol = Gatherer.columnNumber (cols, '_Cluster_key')
		homologeneIDCol = Gatherer.columnNumber (cols, 'homologene_id')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

		# set of distinct clusters
		clusterKey = 1
		clusterList = set([])

                # at most one marker in a gridCluster_marker
                markerList = set([])

                # at most one cluster/term in a gridCluster_disease
                diseaseList = set([])

		for row in rows:

			clusterKey = row[clusterKeyCol]
			homologeneID = row[homologeneIDCol]
			markerKey = row[markerKeyCol]

			#
			# clusterResults
			#
			if clusterKey not in clusterList:
				clusterResults.append( [ 
                                     	clusterKey,
					homologeneID,
					])
				clusterList.add(clusterKey)

			#
			# markerResults
			#
			if markerKey not in markerList:
				markerResults.append ( [ 
			    		row[clusterKeyCol],
			    		markerKey,
			    		row[organismKeyCol],
			    		row[symbolCol],
					])
				markerList.add(markerKey)

			#
			# diseaseResults : include unique instances only
			#
			if diseaseDict1.has_key(clusterKey):
				for d in diseaseDict1[clusterKey]:
					termKey = d[2]
					annotationKey = d[3]
					termName = d[4]
					termId = d[5]
					if (clusterKey, termKey) not in diseaseList:
						diseaseResults.append( [ 
			    				row[clusterKeyCol],
							termKey,
							annotationKey,
							termId,
							termName
							])
						diseaseList.add((clusterKey, termKey))

		logger.debug ('processed mouse/human genes with homolgene clusters')

		# sql (15) : mouse/human markers with annotations that do NOT contain homologene clusters
		(cols, rows) = self.results[15]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

                # at most one marker/term in a gridCluster_disease
                diseaseList = set([])

		for row in rows:

			# fake cluster key
			clusterKey = clusterKey + 1
			markerKey = row[markerKeyCol]

			#
			# clusterResults
			#
			clusterResults.append ( [ 
                               	clusterKey,
				None,
				])

			#
			# markerResults
			#
			if markerKey not in markerList:
				markerResults.append ( [ 
                               		clusterKey,
					markerKey,
			     		row[organismKeyCol],
			     		row[symbolCol],
				])
				markerList.add(markerKey)

			#
			# diseaseResults : include unique instances only
			#
			if diseaseDict2.has_key(markerKey):
				for d in diseaseDict2[markerKey]:
					termKey = d[1]
					annotationKey = d[2]
					termName = d[3]
					termId = d[4]
					if (markerKey, termKey) not in diseaseList:
						diseaseResults.append( [ 
			    				clusterKey,
							termKey,
							annotationKey,
							termId,
							termName
							])
						diseaseList.add((markerKey, termKey))
		logger.debug ('processed mouse/human genes without homolgene clusters')

		# push data to output files
		self.output.append((annotCols, annotResults))
		self.output.append((clusterCols, clusterResults))
		self.output.append((markerCols, markerResults))
		self.output.append((diseaseCols, diseaseResults))

		return

        def postprocessResults(self):
                return

###--- globals ---###

cmds = [

	#
	# sql (0)
	# mp term -> mp header term
	#
	'''
	select distinct d._Object_key, h.synonym
        from DAG_Node d, DAG_Closure dc, DAG_Node dh, MGI_Synonym h
        where d._DAG_key = 4
        and d._Node_key = dc._Descendent_key
        and dc._Ancestor_key = dh._Node_key
        and dh._Label_key = 3
        and dh._Object_key = h._Object_key
        and h._SynonymType_key = 1021

        union 

        select distinct d._Object_key, h.synonym
        from DAG_Node d, DAG_Closure dc, DAG_Node dh, MGI_Synonym h
        where d._DAG_key = 4
        and d._Node_key = dc._Descendent_key
        and dc._Descendent_key = dh._Node_key
        and dh._Label_key = 3
        and dh._Object_key = h._Object_key
        and h._SynonymType_key = 1021
	''',

	#
	# hdp_annotation table
	#

        # sql (1)
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

        # sql (2)
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

        # sql (3)
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

	# sql (4)
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
        # hdp_gridcluster
	# hdp_gridcluster_marker
	# hdp_gridcluster_disease
        #

	# sql (5-6) : super-simple genotypes
	'''
	select g._Genotype_key 
	into temporary table tmp_genotype
	from GXD_AlleleGenotype g
	where exists (select 1 from ALL_Allele a
		 where g._Allele_key = a._Allele_key
			and a.isWildType = 0)
	group by g._Genotype_key having count(*) = 1
	''',

	'''
	create index idx_genotype on tmp_genotype (_Genotype_key)
	''',

        # sql (7)
        #       by mouse marker/MP (1002) for super-simple genotypes
        #       by mouse marker/OMIM (1005) for super-simple genotypes
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
	select distinct c._Cluster_key, c._Marker_key, 
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
	into temporary table tmp_cluster
        from MRK_ClusterMember c, VOC_Annot v, GXD_AlleleGenotype g, tmp_genotype tg,
			VOC_Term t, ACC_Accession a
	where tg._Genotype_key = g._Genotype_key
        and g._Marker_key = c._Marker_key
        and g._Marker_key != 37270
        and g._Genotype_key = v._Object_key
        and v._AnnotType_key = 1005 and v._Qualifier_key != 1614157
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1

	union

	select distinct c._Cluster_key, c._Marker_key, 
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
        from MRK_ClusterMember c, VOC_Annot v, GXD_AlleleGenotype g, tmp_genotype tg,
			VOC_Term t, ACC_Accession a
	where tg._Genotype_key = g._Genotype_key
        and g._Marker_key = c._Marker_key
        and g._Marker_key != 37270
        and g._Genotype_key = v._Object_key
        and v._AnnotType_key = 1002 and v._Qualifier_key != 2181424
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1

	union

	select distinct c._Cluster_key, c._Marker_key, 
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
        from MRK_ClusterMember c, VOC_Annot v, ALL_Allele al, VOC_Term t, ACC_Accession a
	where c._Marker_key = al._Marker_key
	and al._Allele_key = v._Object_key
	and v._AnnotType_key = 1012
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1

	union

	select distinct c._Cluster_key, c._Marker_key, 
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
        from MRK_ClusterMember c, VOC_Annot v, VOC_Term t, ACC_Accession a
        where c._Marker_key = v._Object_key
        and v._AnnotType_key = 1006
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
	''',

        # sql (8) : 
        #   mouse with MP annotations where mouse does NOT contain homologene clusters
	#   mouse with OMIM annotations where mouse does NOT contain homologene clusters
        #   mouse (by allele) with MP annotations where mouse does NOT contain homologene clusters
        #   human with OMIM annotations where mouse does NOT contain homologene clusters
        # include only super-simple genotypes
        # exclude Gt(ROSA)
	#
	# exclude the tmp_cluster-ed data
	'''
	select distinct c._Marker_key,
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
	into temporary table tmp_nocluster
        from MRK_Marker  c, VOC_Annot v, GXD_AlleleGenotype g, tmp_genotype tg,
			VOC_Term t, ACC_Accession a
	where tg._Genotype_key = g._Genotype_key
        and g._Marker_key = c._Marker_key
        and g._Marker_key != 37270
        and g._Genotype_key = v._Object_key
        and v._AnnotType_key = 1005 and v._Qualifier_key != 1614157
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
	and not exists (select 1 from tmp_cluster tc where c._Marker_key = tc._Marker_key)

	union

	select distinct c._Marker_key,
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
        from MRK_Marker  c, VOC_Annot v, GXD_AlleleGenotype g, tmp_genotype tg,
			VOC_Term t, ACC_Accession a
	where tg._Genotype_key = g._Genotype_key
        and g._Marker_key = c._Marker_key
        and g._Marker_key != 37270
        and g._Genotype_key = v._Object_key
        and v._AnnotType_key = 1002 and v._Qualifier_key != 2181424
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
	and not exists (select 1 from tmp_cluster tc where c._Marker_key = tc._Marker_key)

	union

        select distinct c._Marker_key,
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
        from MRK_Marker c, VOC_Annot v, ALL_Allele al, VOC_Term t, ACC_Accession a
	where c._Marker_key = al._Marker_key
	and al._Allele_key = v._Object_key
	and v._AnnotType_key = 1012
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
	and not exists (select 1 from tmp_cluster tc where c._Marker_key = tc._Marker_key)

	union

	select distinct c._Marker_key,
		v._Term_key, v._AnnotType_key, 
		t.term, a.accID
        from MRK_Marker c, VOC_Annot v, VOC_Term t, ACC_Accession a
        where c._Marker_key = v._Object_key
        and v._AnnotType_key = 1006
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
	and not exists (select 1 from tmp_cluster tc where c._Marker_key = tc._Marker_key)
	''',

	# sql (9)
	'''
	create index idx_cluster on tmp_cluster (_Cluster_key)
	''',
	# sql (10)
	'''
	create index idx_cluster_marker on tmp_cluster (_Marker_key)
	''',

	# sql (11)
	'''
	create index idx_nocluster_marker on tmp_nocluster (_Marker_key)
	''',

	# sql (12)
	'''
	select * from tmp_cluster
	''',

	# sql (13)
	'''
	select * from tmp_nocluster
	''',

	# sql (14)
	# additional info for tmp_cluster-ed data
        '''
	select distinct c._Cluster_key, c._Marker_key, m._Organism_key, m.symbol, a.accID as homologene_id
	from MRK_ClusterMember c, MRK_Marker m, ACC_Accession a
	where c._Marker_key = m._Marker_key
	and m._Organism_key in (1,2)
	and c._Cluster_key = a._Object_key
        and a._LogicalDB_key = 81
	and exists (select 1 from tmp_cluster tc where tc._Cluster_key = c._Cluster_key)
	order by c._Cluster_key
	''',

	# sql (15)
	# additional info for tmp_nocluster-ed data
        '''
	select distinct c._Marker_key, c._Organism_key, c.symbol
	from MRK_Marker c
	where c._Organism_key in (1,2)
	and exists (select 1 from tmp_nocluster tc where tc._Marker_key = c._Marker_key)
	order by c._Marker_key
	''',

	]

# prefix for the filename of the output file
files = [
	('hdp_annotation',
		[ Gatherer.AUTO, '_Marker_key', '_Organism_key', 
			'_Term_key', '_AnnotType_key', 
			'_Object_key', 'genotype_type', 'accID', 'term', 
			'name', 'mp_header' ],
          'hdp_annotation'),

	('hdp_gridcluster',
		[ '_Cluster_key', 'homologene_id' ],
          'hdp_gridcluster'),

	('hdp_gridcluster_marker',
		[ Gatherer.AUTO, '_Cluster_key', '_Marker_key',
		  '_Organism_key', 'symbol' ],
          'hdp_gridcluster_marker'),

	('hdp_gridcluster_disease',
		[ Gatherer.AUTO, '_Cluster_key', '_Term_key',
		  '_AnnotType_key', 'accID', 'term' ],
          'hdp_gridcluster_disease'),
	]

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
