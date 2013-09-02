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
# 07/19/2013	lec
#	- TR11423/Human Disease Portal
#

import Gatherer
import logger

###--- Constents ---###
SUPER_TYPE = 'super-simple'
SIMPLE_TYPE = 'simple'
COMPLEX_TYPE = 'complex'
TERM_TYPE = 'term'
HEADER_TYPE = 'header'

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

		#
		# sql (14)
		# super simple genotypes
		superSimpleList = set([])
		(cols, rows) = self.results[14]
		key = Gatherer.columnNumber (cols, '_Genotype_key')
		for row in rows:
			superSimpleList.add(row[key])
		#logger.debug(superSimpleList)

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

		# sql (5)
		# mouse genotype/OMIM annotations
		# mouse genotype/MP annotations
		(cols, rows) = self.results[5]

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

		# list of super-simple + simple genotypes
		simpleList = set([])

		#
		# if the genotype is super-simple or simple
		# and the marker is NOT 'Gt(ROSA)26Sor'
		#

		for row in rows:
			if len(genomarkerDict[row[genotypeKeyCol]]) == 1 \
				and row[markerKeyCol] != 37270:

				if row[genotypeKeyCol] in superSimpleList:
					genotype_type = SUPER_TYPE
				else:
					genotype_type = SIMPLE_TYPE

				# save simple genotype list for complex checks
				# this list should include both super-simple + simple
				simpleList.add(row[genotypeKeyCol])

				termKey = row[termKeyCol]
				if mpHeaderDict.has_key(termKey):
					for mpHeader in mpHeaderDict[termKey]:
						annotResults.append ( [ 
							row[markerKeyCol],
							row[organismKeyCol],
							row[termKeyCol],
							row[annotTypeKeyCol],
							row[genotypeKeyCol],
							genotype_type,
							row[termIDCol],
							row[termCol],
							row[vocabNameCol],
							mpHeader,
							])
				else:
					annotResults.append ( [ 
						row[markerKeyCol],
						row[organismKeyCol],
						row[termKeyCol],
						row[annotTypeKeyCol],
						row[genotypeKeyCol],
						genotype_type,
						row[termIDCol],
						row[termCol],
						row[vocabNameCol],
						None,
						])
		logger.debug ('processed simple mouse annotations')

                # sql (6)
		# mouse genotype/OMIM annotations : complex
		# mouse genotype/MP annotations : complex
                (cols, rows) = self.results[6]

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
                        if row[genotypeKeyCol] not in simpleList:

				termKey = row[termKeyCol]
				if mpHeaderDict.has_key(termKey):
					for mpHeader in mpHeaderDict[termKey]:
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
				else:
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
						None,
                                		])
		logger.debug ('processed complex mouse annotations')

		# sql (7)
		# allele/OMIM annotations
		(cols, rows) = self.results[7]

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
				for mpHeader in mpHeaderDict[termKey]:
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
			else:
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
				     	None,
                                	])
		logger.debug ('processed allele/OMIM annotatins')

		# sql (8)
		# human gene/OMIM annotations
		(cols, rows) = self.results[8]

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
				for mpHeader in mpHeaderDict[termKey]:
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
			else:
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
				     	None,
					])
		logger.debug ('processed human OMIM annotations')

		#
		# hdp_gridcluster
		# hdp_gridcluster_marker
		# hdp_gridcluster_annotation
		#
		clusterResults = []
		clusterCols = ['_Cluster_key',
				'homologene_id',
			]

		cmarkerResults = []
		cmarkerCols = ['_Cluster_key',
				'_Marker_key', 
				'_Organism_key', 
				'symbol',
			]

		cannotResults = []
		cannotCols = ['_Cluster_key',
				'_Term_key', 
				'_AnnotType_key',
				'term_type',
				'accID', 
				'term',
			]

		# sql (15)
		# homologene clusters
		clusterDict1 = {}
		(cols, rows) = self.results[15]
		clusterKey = Gatherer.columnNumber (cols, '_Cluster_key')
		for row in rows:
			key = row[clusterKey]
			value = row
			if not clusterDict1.has_key(key):
				clusterDict1[key] = []
			clusterDict1[key].append(row)
		#logger.debug (clusterDict1)

		# sql (16)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		clusterDict2 = {}
		(cols, rows) = self.results[16]
		markerKey = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			key = row[markerKey]
			value = row
			if not clusterDict2.has_key(key):
				clusterDict2[key] = []
			clusterDict2[key].append(row)
			key = key + 1
		#logger.debug (clusterDict2)

		# sql (17) : annotations that contain homologene clusters
		(cols, rows) = self.results[17]

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

                # at most one cluster/term in a gridCluster_annotation
                cannotList = set([])

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
			# cmarkerResults
			#
			if markerKey not in markerList:
				cmarkerResults.append ( [ 
			    		row[clusterKeyCol],
			    		markerKey,
			    		row[organismKeyCol],
			    		row[symbolCol],
					])
				markerList.add(markerKey)

			#
			# cannotResults : include unique instances only
			#
			if clusterDict1.has_key(clusterKey):
				for c in clusterDict1[clusterKey]:
					termKey = c[2]
					annotationKey = c[3]
					termName = c[4]
					termId = c[5]
					if (clusterKey, termKey) not in cannotList:
						cannotResults.append( [ 
			    				row[clusterKeyCol],
							termKey,
							annotationKey,
							TERM_TYPE,
							termId,
							termName
							])

						# find each mpheader for the term
						if mpHeaderDict.has_key(termKey):
							for mpHeader in mpHeaderDict[termKey]:
								cannotResults.append( [ 
			    						clusterKey,
									termKey,
									annotationKey,
									HEADER_TYPE,
									termId,
									mpHeader
									])

						cannotList.add((clusterKey, termKey))
		logger.debug ('processed mouse/human genes with homolgene clusters')

		# sql (18) : mouse/human markers with annotations that do NOT contain homologene clusters
		(cols, rows) = self.results[18]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

                # at most one marker/term in a gridCluster_annotation
                cannotList = set([])

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
			# cmarkerResults
			#
			if markerKey not in markerList:
				cmarkerResults.append ( [ 
                               		clusterKey,
					markerKey,
			     		row[organismKeyCol],
			     		row[symbolCol],
				])
				markerList.add(markerKey)

			#
			# cannotResults
			#
			if clusterDict2.has_key(markerKey):
				for c in clusterDict2[markerKey]:
					termKey = c[1]
					annotationKey = c[2]
					termName = c[3]
					termId = c[4]

					if (markerKey, termKey) not in cannotList:

						cannotResults.append( [ 
			    				clusterKey,
							termKey,
							annotationKey,
							TERM_TYPE,
							termId,
							termName
							])

						# find each mpheader for the term
						if mpHeaderDict.has_key(termKey):
							for mpHeader in mpHeaderDict[termKey]:
								cannotResults.append( [ 
			    						clusterKey,
									termKey,
									annotationKey,
									HEADER_TYPE,
									termId,
									mpHeader
									])

						cannotList.add((markerKey, termKey))
		logger.debug ('processed mouse/human genes without homolgene clusters')

		# sql (19) : genotype-cluster
		(cols, rows) = self.results[19]

		# set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		allele1KeyCol = Gatherer.columnNumber (cols, '_Allele_key_1')
		allele2KeyCol = Gatherer.columnNumber (cols, '_Allele_key_2')
		pairStateKeyCol = Gatherer.columnNumber (cols, '_PairState_key')
		isConditionalKeyCol = Gatherer.columnNumber (cols, 'isConditional')
		existsAsKeyCol = Gatherer.columnNumber (cols, '_ExistsAs_key')

		genoClusterResults = []
		genoClusterCols = [ 'hdp_genocluster_key', '_Marker_key',
				'_Allele_key_1', '_Allele_key_2',
		  		'_PairState_key', 'isConditional', '_ExistsAs_key' 
			]

		genoResults = []
		genoCols = ['hdp_genocluster_key', 
				'_Genotype_key',
			]

		compressSet = {}

        	for row in rows:

                	gSet = row[genotypeKeyCol]
                	cSet = (row[markerKeyCol], \
                        	row[allele1KeyCol], \
                        	row[allele2KeyCol], \
                        	row[pairStateKeyCol], \
                        	row[isConditionalKeyCol],
                        	row[existsAsKeyCol])

                	if not compressSet.has_key(cSet):
                        	compressSet[cSet] = []
                	compressSet[cSet].append(gSet)
		#logger.debug (compressSet)

		clusterKey = 1
		for r in compressSet:
			genoClusterResults.append( [
				clusterKey, r[0], r[1], r[2], r[3], r[4], r[5],
				])

			for gKey in compressSet[r]:
				genoResults.append( [
					clusterKey,
					gKey,
					])
				
			clusterKey = clusterKey + 1
		logger.debug ('processed genotype cluster')

		# push data to output files
		self.output.append((annotCols, annotResults))

		self.output.append((clusterCols, clusterResults))
		self.output.append((cmarkerCols, cmarkerResults))
		self.output.append((cannotCols, cannotResults))

		self.output.append((genoClusterCols, genoClusterResults))
		self.output.append((genoCols, genoResults))

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
        from
                DAG_Node d, 
                DAG_Closure dc, 
                DAG_Node dh, 
                MGI_Synonym h
        where
                d._DAG_key = 4
                and d._Node_key = dc._Descendent_key
                and dc._Ancestor_key = dh._Node_key
                and dh._Label_key = 3
                and dh._Object_key = h._object_key
                and h._synonymtype_key = 1021

        union

        select distinct d._Object_key, h.synonym
        from 
                DAG_Node d, 
                DAG_Closure dc, 
                DAG_Node dh, 
                MGI_Synonym h
        where 
                d._DAG_key = 4
                and d._Node_key = dc._Descendent_key
                and dc._Descendent_key = dh._Node_key
                and dh._Label_key = 3
                and dh._Object_key = h._object_key
                and h._synonymtype_key = 1021
	''',

	# sql (1-2) : super-simple genotypes
	'''
	select g._Genotype_key 
	into temporary table tmp_supersimple
	from GXD_AlleleGenotype g
	where exists (select 1 from ALL_Allele a
		 where g._Allele_key = a._Allele_key
			and a.isWildType = 0)
	group by g._Genotype_key having count(*) = 1
	''',

	'''
	create index idx_genotype on tmp_supersimple (_Genotype_key)
	''',

	#
	# sql (3-4)
	# super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
	# does NOT contain allele/OMIM annotations (1012)
	#
	'''
	select distinct tg._Genotype_key, m._Marker_key, 
		v._AnnotType_key, v._Term_key, t.term, a.accID
	into temporary table tmp_genotype
        from tmp_supersimple tg, GXD_AlleleGenotype g, MRK_Marker m, 
		VOC_Annot v, VOC_Term t, ACC_Accession a
	where tg._Genotype_key = g._Genotype_key
        and g._Genotype_key = v._Object_key
        and ((v._AnnotType_key = 1002 and v._Qualifier_key != 2181424)
		or
             (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)
	     )
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and g._Marker_key = m._Marker_key
        and g._Marker_key != 37270
	''',

	'''
	create index idx1_genotype on tmp_genotype (_Genotype_key)
	''',

	#
	# hdp_annotation table
	#

        # sql (5)
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

        # sql (6)
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

        # sql (7)
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
	and m._Marker_key != 37270
        ''',

	# sql (8)
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
	# hdp_gridcluster_annotation (includes OMIM, MP)
        #

        # sql (9)
        #       by mouse marker/MP (1002) for super-simple genotypes
        #       by mouse marker/OMIM (1005) for super-simple genotypes
        #       by human marker/OMIM (1006)
	# note that the allele/omim annotations (1012) are not included
        #
        # only include if the genotype is a super-simple genotype
        # that is, the genotype has only one marker
        # and marker is NOT Gt(ROSA)
        #
        # select all *distinct* homologene clusters that contain a mouse/human HPD
        # annotation.  then select all of the mouse/human markers that are contained
        # within each of those clusters
        #
	#
	'''
	select distinct c._Cluster_key, gg._Marker_key, 
		gg._Term_key, gg._AnnotType_key, gg.term, gg.accID
	into temporary table tmp_cluster
	from tmp_supersimple tg, tmp_genotype gg, MRK_ClusterMember c
	where tg._Genotype_key = gg._Genotype_key
	and gg._Marker_key = c._Marker_key

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

	# sql (10)
	'''
	create index idx_cluster on tmp_cluster (_Cluster_key)
	''',
	# sql (11)
	'''
	create index idx_cluster_marker on tmp_cluster (_Marker_key)
	''',

        # sql (12) : 
	#
	# super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
	# that do NOT contain homolgene clusters
        #   human with OMIM annotations where mouse does NOT contain homologene clusters
	#
	# note that the allele/omim annotations (1012) are not included
	#
	'''
	select distinct gg._Marker_key, gg._Term_key, gg._AnnotType_key, gg.term, gg.accID
	into temporary table tmp_nocluster
	from tmp_supersimple tg, tmp_genotype gg
	where tg._Genotype_key = gg._Genotype_key
	and not exists (select 1 from tmp_cluster tc where gg._Marker_key = tc._Marker_key)

	union

	select distinct c._Marker_key, v._Term_key, v._AnnotType_key, t.term, a.accID
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

	# sql (13)
	'''
	create index idx_nocluster_marker on tmp_nocluster (_Marker_key)
	''',

	# sql (14)
	'''
	select * from tmp_supersimple
	''',

	# sql (15)
	'''
	select * from tmp_cluster
	''',

	# sql (16)
	'''
	select * from tmp_nocluster
	''',

	# sql (17)
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

	# sql (18)
	# additional info for tmp_nocluster-ed data
        '''
	select distinct c._Marker_key, c._Organism_key, c.symbol
	from MRK_Marker c
	where c._Organism_key in (1,2)
	and exists (select 1 from tmp_nocluster tc where tc._Marker_key = c._Marker_key)
	order by c._Marker_key
	''',

	# sql (19)
	# allele pair information in order to generate the genotype-cluster
	# only includes super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
	'''
 	select p._Genotype_key, p._Marker_key,
               p._Allele_key_1, p._Allele_key_2, p._PairState_key,
               g.isConditional, g._ExistsAs_key
        from GXD_Genotype g, GXD_AllelePair p
        where g._Genotype_key = p._Genotype_key
        and exists (select 1 from tmp_supersimple c where c._Genotype_key = p._Genotype_key)
        and exists (select 1 from tmp_genotype c where c._Genotype_key = p._Genotype_key)
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

	('hdp_gridcluster_annotation',
		[ Gatherer.AUTO, '_Cluster_key', '_Term_key',
		  '_AnnotType_key', 'term_type', 'accID', 'term' ],
          'hdp_gridcluster_annotation'),

	('hdp_genocluster',
		[ 'hdp_genocluster_key', '_Marker_key',
		  '_Allele_key_1', '_Allele_key_2',
		  '_PairState_key', 'isConditional', '_ExistsAs_key' ],
          'hdp_genocluster'),

	('hdp_genocluster_genotype',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Genotype_key' ],
          'hdp_genocluster_genotype'),
	]

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
