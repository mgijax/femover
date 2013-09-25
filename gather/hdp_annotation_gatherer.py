#!/usr/local/bin/python
# 
# gathers data for the 'hdp_annotation' table in the front-end database
#
# Purpose:
#
# To load all:
# 	mouse/genotypes annotationed to MP Phenotype terms (_AnnotType_key = 1002)
#	mouse/genotypes annotated to OMIM terms (_AnnotType_key = 1005)
#	mouse/alleles annotatied to OMIM terms (_AnnotType_key = 1012)
# 	human/genes annotated to OMIM terms (_AnnotType_key = 1006, 1013)
# into the HDP table.
#
# IMPORTANT:
#	genotype data only exists for mouse/MP (1002) and mouse/OMIM (1005)
#	mouse/allele
#
# some exclusion rules:
# 	exclude: genotypes that contain 'slash' alleles and are *not* transgenes
#		(super-simple only)
# 	exclude: markers where there exists a double-wild-type allele pair
#		(complex, gridcluster, genocluster)
#
# hdp_annotation
#	genotypes: super-simple, simple, complex
#	includes annotationed terms
#	includes the header terms for each annotation MP-term
#	super-simple (OMG):
#	simple : not super-simple
#	complex : neither super-simple nor simple
#
# hdp_marker_to_reference
#	markers annotated to OMIM disease and their references
#	includes only super-simple, simple genotypes (_AnnotType_key = 1005)
#	includes all allele/OMIM (_AnnotType_key = 1012)
#
# hdp_term_to_reference
#	disease terms annotated to mouse 
#	includes all genotypes (_AnnotType_key = 1005)
#	includes all allele/OMIM (_AnnotType_key = 1012)
#
# hdp_gridcluster
# hdp_gridcluster_marker
# hdp_gridcluster_annotation
#
# 	super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
# 	that contain homolgene clusters
# 	plus
# 	human with OMIM annotations where mouse contain homologene clusters
#	+++++
# 	super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
# 	that do *not* contain homolgene clusters
# 	plus
# 	human with OMIM annotations where mouse does *not* contain homologene clusters
#
#	'marker': markers associated with a grid-cluster (both homologene + non-homolgene clusters)
#
#	'annotaton' : annotations associated with a grid-cluster (both homologene + non-homolgene clusters)
#		includes annotation type, term info
#		include header terms
#
# hdp_genocluster
# hdp_genocluster_genotype
# hdp_genocluster_annotation
#
# 	super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
#
#	create a genotype-cluster by aggregating:
#		marker, allele1, allele2, pair state, is-conditional, genotype-exists
#
#	'genotype' : genotypes that belong to a specific geno-cluster
#
#	'annotation' : annotations associated with a geno-cluster
#		includes annotation type, term info
#		include header terms
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
		# includes parent + children
		mpHeaderDict = {}
		(cols, rows) = self.results[0]
		termKey = Gatherer.columnNumber (cols, '_Object_key')
		value = Gatherer.columnNumber (cols, 'synonym')
		for row in rows:
			if not mpHeaderDict.has_key(row[termKey]):
				mpHeaderDict[row[termKey]] = []
			mpHeaderDict[row[termKey]].append(row[value])
		#logger.debug (mpHeaderDict)

		#
		# sql (17)
		# marker -> mp header term/accession id
		markerHeaderDict = {}
		(cols, rows) = self.results[17]
		key = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			if not markerHeaderDict.has_key(row[key]):
				markerHeaderDict[row[key]] = []
			markerHeaderDict[row[key]].append(row)

		# hdp_annotation
		# hdp_marker_to_reference
		# hdp_term_to_reference
		#
		annotResults = []
		annotCols = ['_Marker_key', 
				'_Organism_key', 
				'_Term_key', 
				'_AnnotType_key', 
				'_Object_key', 
				'genotype_type',
				'qualifier_type',
				'accID',
				'term',
				'name',
				'mp_header'
			]

                diseaseMarkerRefResults = []
                diseaseMarkerRefCols = ['_Marker_key', 
                                '_Refs_key', 
                        ]

                diseaseTermRefResults = []
                diseaseTermRefCols = ['_Term_key',
                                '_Refs_key',
                        ]

		# sql (22)
		# disease-references by marker
		# to store distinct marker/reference used for mouse/OMIM
                diseaseMarkerRef1Dict = {}
		(cols, rows) = self.results[22]
                key1 = Gatherer.columnNumber (cols, '_Genotype_key')
                key2 = Gatherer.columnNumber (cols, '_Marker_key')
                for row in rows:
			key = (row[key1], row[key2])
                        if not diseaseMarkerRef1Dict.has_key(key):
                                diseaseMarkerRef1Dict[key] = []
                        diseaseMarkerRef1Dict[key].append(row)
		#logger.debug (diseaseMarkerRef1Dict)

                # sql (23)
                # disease-term-referencees
                diseaseTermRefDict = {}
                (cols, rows) = self.results[23]
                key = Gatherer.columnNumber (cols, '_Term_key')
                for row in rows:
                        if not diseaseTermRefDict.has_key(row[key]):
                                diseaseTermRefDict[row[key]] = []
                        diseaseTermRefDict[row[key]].append(row)
		#logger.debug (diseaseTermRefDict[847181])

		# sql (24)
		# super-simple genotypes
		logger.debug ('start : processed super-simple mouse annotations')
		(cols, rows) = self.results[24]

		# set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier_type')

		genotype_type = SUPER_TYPE
		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]
			annotType = row[annotTypeKeyCol]
			termKey = row[termKeyCol]

			if mpHeaderDict.has_key(termKey):
				for mpHeader in mpHeaderDict[termKey]:
					annotResults.append ( [ 
						markerKey,
						row[organismKeyCol],
						termKey,
						annotType,
						genotypeKey,
						genotype_type,
						row[qualifierCol],
						row[termIDCol],
						row[termCol],
						row[vocabNameCol],
						mpHeader,
						])
			else:
				annotResults.append ( [ 
					markerKey,
					row[organismKeyCol],
					termKey,
					annotType,
					genotypeKey,
					genotype_type,
					row[qualifierCol],
					row[termIDCol],
					row[termCol],
					row[vocabNameCol],
					None,
					])

			# if this super-simple or simple genotype contains
			# 	annotation type mouse marker/omim (1005)
			# then store the *unique* marker/reference association
			key = (genotypeKey, markerKey)
                	if annotType in [1005] and diseaseMarkerRef1Dict.has_key(key):
                        	for m in diseaseMarkerRef1Dict[key]:
					if [markerKey, m[2]] not in diseaseMarkerRefResults:
                                      		diseaseMarkerRefResults.append ( [ markerKey, m[2], ])

                        # then store the *unique* term/reference association
                        if annotType in [1005] and diseaseTermRefDict.has_key(termKey):
                                for m in diseaseTermRefDict[termKey]:
                                	if [termKey, m[1]] not in diseaseTermRefResults:
                                        	diseaseTermRefResults.append ( [ termKey, m[1], ])
						if termKey == 847181:
							logger.debug (m[1])

		logger.debug ('end : processed super-simple mouse annotations')

		# sql (25)
		# simple genotypes
		logger.debug ('start : processed simple mouse annotations')
		(cols, rows) = self.results[25]

		# set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier_type')

		genotype_type = SIMPLE_TYPE
		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]
			annotType = row[annotTypeKeyCol]
			termKey = row[termKeyCol]

			if mpHeaderDict.has_key(termKey):
				for mpHeader in mpHeaderDict[termKey]:
					annotResults.append ( [ 
						markerKey,
						row[organismKeyCol],
						termKey,
						annotType,
						genotypeKey,
						genotype_type,
						row[qualifierCol],
						row[termIDCol],
						row[termCol],
						row[vocabNameCol],
						mpHeader,
						])
			else:
				annotResults.append ( [ 
					markerKey,
					row[organismKeyCol],
					termKey,
					annotType,
					genotypeKey,
					genotype_type,
					row[qualifierCol],
					row[termIDCol],
					row[termCol],
					row[vocabNameCol],
					None,
					])

			# if this super-simple or simple genotype contains
			# 	annotation type mouse marker/omim (1005)
			# then store the *unique* marker/reference association
			key = (genotypeKey, markerKey)
                	if annotType in [1005] and diseaseMarkerRef1Dict.has_key(key):
                        	for m in diseaseMarkerRef1Dict[key]:
					if [markerKey, m[2]] not in diseaseMarkerRefResults:
                                      		diseaseMarkerRefResults.append ( [ markerKey, m[2], ])

                        # then store the *unique* term/reference association
                        if annotType in [1005] and diseaseTermRefDict.has_key(termKey):
                                for m in diseaseTermRefDict[termKey]:
                                	if [termKey, m[1]] not in diseaseTermRefResults:
                                        	diseaseTermRefResults.append ( [ termKey, m[1], ])
						if termKey == 847181:
							logger.debug (m[1])

		logger.debug ('end : processed super-simple/simple mouse annotations')

                # sql (26)
		# complex genotypes
		logger.debug ('start : processed complex mouse annotations')
                (cols, rows) = self.results[26]

                # set of columns for common sql fields
                genotypeKeyCol = Gatherer.columnNumber (cols, '_Object_key')
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
                organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
                annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
                termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
                termIDCol = Gatherer.columnNumber (cols, 'accID')
                termCol = Gatherer.columnNumber (cols, 'term')
                vocabNameCol = Gatherer.columnNumber (cols, 'name')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier_type')

		genotype_type = COMPLEX_TYPE
                for row in rows:
			termKey = row[termKeyCol]
			if mpHeaderDict.has_key(termKey):
				for mpHeader in mpHeaderDict[termKey]:
					annotResults.append ( [ 
						row[markerKeyCol],
						row[organismKeyCol],
						termKey,
						row[annotTypeKeyCol],
						row[genotypeKeyCol],
                                    		genotype_type,
						row[qualifierCol],
						row[termIDCol],
						row[termCol],
						row[vocabNameCol],
						mpHeader,
						])
			else:
                       		annotResults.append ( [
                                   	row[markerKeyCol],
                                    	row[organismKeyCol],
                                     	termKey,
                                     	row[annotTypeKeyCol],
                                     	row[genotypeKeyCol],
                                    	genotype_type,
					row[qualifierCol],
                                     	row[termIDCol],
                                     	row[termCol],
                                     	row[vocabNameCol],
					None,
                                	])

                        # then store the *unique* term/reference association
                        if annotType in [1005] and diseaseTermRefDict.has_key(termKey):
                                for m in diseaseTermRefDict[termKey]:
                                	if [termKey, m[1]] not in diseaseTermRefResults:
                                        	diseaseTermRefResults.append ( [ termKey, m[1], ])
						if termKey == 847181:
							logger.debug (m[1])

		logger.debug ('end : processed complex mouse annotations')

		# sql (27)
		# disease-marker-referencees
		# to store distinct marker/reference used for allelle/OMIM annotations
                diseaseMarkerRef2Dict = {}
		(cols, rows) = self.results[27]
                key = Gatherer.columnNumber (cols, '_Marker_key')
                for row in rows:
                        if not diseaseMarkerRef2Dict.has_key(row[key]):
                                diseaseMarkerRef2Dict[row[key]] = []
                        diseaseMarkerRef2Dict[row[key]].append(row)
		#logger.debug (diseaseMarkerRef2Dict)

		# sql (28)
		# allele/OMIM annotations
		logger.debug ('start : processed allele/OMIM annotatins')
		(cols, rows) = self.results[28]

                # set of columns for common sql fields
                markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
                organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
                annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
                termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
                termIDCol = Gatherer.columnNumber (cols, 'accID')
                termCol = Gatherer.columnNumber (cols, 'term')
                vocabNameCol = Gatherer.columnNumber (cols, 'name')

                for row in rows:
			# no genotypes

			markerKey = row[markerKeyCol]
			termKey = row[termKeyCol]

			if mpHeaderDict.has_key(termKey):
				for mpHeader in mpHeaderDict[termKey]:
                        		annotResults.append ( [
                                		markerKey,
                                		row[organismKeyCol],
                                		termKey,
                                		row[annotTypeKeyCol],
                                		None,
						None,
						None,
                        			row[termIDCol],
                        			row[termCol],
                        			row[vocabNameCol],
						mpHeader,
                        			])
			else:
                        		annotResults.append ( [
                                		markerKey,
                                		row[organismKeyCol],
                                		termKey,
                                		row[annotTypeKeyCol],
                                		None,
						None,
						None,
                        			row[termIDCol],
                        			row[termCol],
                        			row[vocabNameCol],
						None,
                        			])

			# store the *unique* marker/reference association
                	if diseaseMarkerRef2Dict.has_key(markerKey):
				for m in diseaseMarkerRef2Dict[markerKey]:
					if [markerKey, m[1]] not in diseaseMarkerRefResults:
                                       			diseaseMarkerRefResults.append ( [ markerKey, m[1], ])

                        # then store the *unique* term/reference association
                        if diseaseTermRefDict.has_key(termKey):
                                for m in diseaseTermRefDict[termKey]:
                                	if [termKey, m[1]] not in diseaseTermRefResults:
                                        	diseaseTermRefResults.append ( [ termKey, m[1], ])
						if termKey == 847181:
							logger.debug (m[1])

		logger.debug ('end : processed allele/OMIM annotatins')

		# sql (29)
		# human gene/OMIM annotations
		logger.debug ('start : processed human OMIM annotations')
		(cols, rows) = self.results[29]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
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
						termKey,
						row[annotTypeKeyCol],
						None,
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
					termKey,
					row[annotTypeKeyCol],
					None,
					None,
					None,
					row[termIDCol],
					row[termCol],
					row[vocabNameCol],
					None,
					])

		logger.debug ('end : processed human OMIM annotations')

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

		# sql (35)
		# homologene clusters
		clusterDict1 = {}
		(cols, rows) = self.results[35]
		clusterKey = Gatherer.columnNumber (cols, '_Cluster_key')
		for row in rows:
			key = row[clusterKey]
			value = row
			if not clusterDict1.has_key(key):
				clusterDict1[key] = []
			clusterDict1[key].append(row)
		#logger.debug (clusterDict1)

		# sql (36)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		clusterDict2 = {}
		(cols, rows) = self.results[36]
		markerKey = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			key = row[markerKey]
			value = row
			if not clusterDict2.has_key(key):
				clusterDict2[key] = []
			clusterDict2[key].append(row)
		#logger.debug (clusterDict2)

		# sql (37) : annotations that contain homologene clusters
		logger.debug ('start : processed mouse/human genes with homolgene clusters')
		(cols, rows) = self.results[37]

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
                                        annotationKey = c[2]
                                        termKey = c[3]
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

                                                cannotList.add((clusterKey, termKey))

                        if markerHeaderDict.has_key(markerKey):
                                for markerHeader in markerHeaderDict[markerKey]:
                                        annotationKey = markerHeader[1]
                                        termKey = markerHeader[2]
                                        termName = markerHeader[3]
                                        termId = markerHeader[4]
                                        cannotResults.append( [
                                                clusterKey,
                                                termKey,
                                                annotationKey,
                                                HEADER_TYPE,
                                                termId,
                                                termName
                                                ])

		logger.debug ('end : processed mouse/human genes with homolgene clusters')

		# sql (38) : mouse/human markers with annotations that do NOT contain homologene clusters
		logger.debug ('start : processed mouse/human genes without homolgene clusters')
		(cols, rows) = self.results[38]

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
					annotationKey = c[1]
					termKey = c[2]
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

						cannotList.add((markerKey, termKey))

			if markerHeaderDict.has_key(markerKey):
				for markerHeader in markerHeaderDict[markerKey]:
					annotationKey = markerHeader[1]
					termKey = markerHeader[2]
					termName = markerHeader[3]
					termId = markerHeader[4]
					cannotResults.append( [ 
		  				clusterKey,
						termKey,
						annotationKey,
						HEADER_TYPE,
						termId,
						termName
						])

		logger.debug ('end : processed mouse/human genes without homolgene clusters')

		#
		# hdp_genocluster
		# hpd_genocluster_genotype
		# hpd_genocluster_annotation
		#

		gClusterResults = []
		gClusterCols = [ 'hdp_genocluster_key', '_Marker_key',
				'_Allele_key_1', '_Allele_key_2',
		  		'_PairState_key', 'isConditional', '_ExistsAs_key' 
			]

		gResults = []
		gCols = ['hdp_genocluster_key', 
				'_Genotype_key',
			]

		gannotResults = []
		gannotCols = ['hdp_genocluster_key',
				'_Term_key', 
				'_AnnotType_key',
				'qualifier_type',
				'term_type',
				'accID', 
				'term',
			]

		# sql (39) : genotype-cluster by annotation
		clusterDict3 = {}
		(cols, rows) = self.results[39]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		for row in rows:
			key = row[genotypeKeyCol]
			value = row
			if not clusterDict3.has_key(key):
				clusterDict3[key] = []
			clusterDict3[key].append(row)
		#logger.debug (clusterDict3)

		# sql (40) : genotype-cluster
		logger.debug ('start : processed genotype cluster')
		(cols, rows) = self.results[40]

		# set of columns for common sql fields
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		allele1KeyCol = Gatherer.columnNumber (cols, '_Allele_key_1')
		allele2KeyCol = Gatherer.columnNumber (cols, '_Allele_key_2')
		pairStateKeyCol = Gatherer.columnNumber (cols, '_PairState_key')
		isConditionalKeyCol = Gatherer.columnNumber (cols, 'isConditional')
		existsAsKeyCol = Gatherer.columnNumber (cols, '_ExistsAs_key')

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

                	# at most one cluster/header-term in a genoCluster_annotation
                	gannotList = set([])

			markerKey = r[0]

			gClusterResults.append( [
				clusterKey, r[0], r[1], r[2], r[3], r[4], r[5],
				])

			for gKey in compressSet[r]:

				gResults.append( [
					clusterKey,
					gKey,
					])
				
				if clusterDict3.has_key(gKey):
					for c in clusterDict3[gKey]:
						annotationKey = c[1]
						termKey = c[2]
						termName = c[3]
						termId = c[4]
						qualifier = c[5]

						gannotResults.append( [ 
			    				clusterKey,
							termKey,
							annotationKey,
							qualifier,
							TERM_TYPE,
							termId,
							termName
							])

						# one header per cluster
						if mpHeaderDict.has_key(termKey):
                                                        for mpHeader in mpHeaderDict[termKey]:
                                        			if (clusterKey, mpHeader) not in gannotList:
                                                                	gannotResults.append( [
                                                                        	clusterKey,
                                                                        	None,
                                                                        	annotationKey,
										qualifier,
                                                                        	HEADER_TYPE,
                                                                        	None,
                                                                        	mpHeader,
                                                                        	])
                                                			gannotList.add((clusterKey, mpHeader))

			clusterKey = clusterKey + 1

		logger.debug ('end : processed genotype cluster')

		# push data to output files

		self.output.append((annotCols, annotResults))
		self.output.append((diseaseMarkerRefCols, diseaseMarkerRefResults))
                self.output.append((diseaseTermRefCols, diseaseTermRefResults))

		self.output.append((clusterCols, clusterResults))
		self.output.append((cmarkerCols, cmarkerResults))
		self.output.append((cannotCols, cannotResults))

		self.output.append((gClusterCols, gClusterResults))
		self.output.append((gCols, gResults))
		self.output.append((gannotCols, gannotResults))

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

	# sql (1-2) 
	# exclude: markers where there exists a double-wild-type allele pair
	'''
        select distinct ap._Genotype_key, ap._Marker_key
	into temporary table tmp_exclude
	from GXD_AllelePair ap, ALL_Allele a1, ALL_Allele a2
        where ap._Allele_key_1 = a1._Allele_key
        and ap._Allele_key_2 = a2._Allele_key
        and a1.isWildType = 1
        and a2.isWildType = 1
	''',

	'''
	create index idx_ignore on tmp_exclude (_Genotype_key, _Marker_key)
	''',

        # sql (3-8)
	# super-simple genotypes
	#
	# include : non-wild type alleles
	# include : genotypes that contains mouse/MP or mouse/OMIM annotations
	# exclude : genotypes that contain 'slash' alleles and are not transgenes
        '''
        select distinct g._Genotype_key, g._Marker_key
        into temporary tmp_supersimple
        from GXD_AlleleGenotype g
        where exists (select 1 from ALL_Allele a
                 where g._Allele_key = a._Allele_key
                        and a.isWildType = 0)
        and exists (select 1 from VOC_Annot v
                where g._Genotype_key = v._Object_key
                and (v._AnnotType_key in (1002)
			or
		    (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)))
        ''',

        '''
        create index idx_super_genotype1 on tmp_supersimple (_Genotype_key)
        ''',

        '''
        select s._Genotype_key
        into temporary table tmp_supersimple_genotype
        from tmp_supersimple s
        group by s._Genotype_key having count(*) = 1
        ''',

        '''
        create index idx_super_genotype2 on tmp_supersimple_genotype (_Genotype_key)
        ''',

        # delete genotypes that contain 'slash' alleles and are *not* transgenes
        '''
        delete from tmp_supersimple_genotype s
        where exists (select 1 from GXD_AlleleGenotype g, ALL_Allele a, MRK_Marker m
                where s._Genotype_key = g._Genotype_key
                and g._Allele_key = a._Allele_key
                and a.symbol like '%/%<%>%'
                and g._Marker_key = m._Marker_key
                and m._Marker_Type_key not in (12))
        ''',

        '''
        delete from tmp_supersimple s
        where not exists (select 1 from tmp_supersimple_genotype g where s._Genotype_key = g._Genotype_key)
        ''',

        # sql (9-16)
	# simple genotypes
	#
	# include : wild type alleles where allele type is *not* 'Trangenic (Reporter)'
	# include : genotypes that contains mouse/MP or mouse/OMIM annotations
	# exclude : genotypes that contain driver notes (cre)
	# exclude : genotypes that contain 'slash' alleles and are not transgenes
	# exclude : genotypes that contain 'Gt(ROSA)' marker
	# exclude : genotypes already designated as super-simple
        '''
        select distinct g._Genotype_key, g._Marker_key
        into temporary table tmp_simple
        from GXD_AlleleGenotype g
        where exists (select 1 from ALL_Allele a
                where g._Allele_key = a._Allele_key
                and a.isWildType = 0
                and a._Allele_Type_key != 847129)
        and exists (select 1 from VOC_Annot v
                where g._Genotype_key = v._Object_key
                and (v._AnnotType_key in (1002)
			or
		    (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)))
        and not exists (select 1 from GXD_Genotype gg, MGI_Note n
                where g._Genotype_key = gg._Genotype_key
                and gg.isConditional = 1
                and g._Allele_key = n._Object_key
                and n._MGIType_key = 11 and n._NoteType_key = 1034)
        ''',

        '''
        create index idx_simple_genotype1 on tmp_simple (_Genotype_key)
        ''',

        '''
        select s._Genotype_key
        into temporary table tmp_simple_genotype
        from tmp_simple s
        group by s._Genotype_key having count(*) = 1
        ''',

        '''
        create index idx_simple_genotype2 on tmp_simple_genotype (_Genotype_key)
        ''',

        # delete genotypes that contain 'slash' alleles and are *not* transgenes
        '''
        delete from tmp_simple_genotype s
        where exists (select 1 from GXD_AlleleGenotype g, ALL_Allele a, MRK_Marker m
                where s._Genotype_key = g._Genotype_key
                and g._Allele_key = a._Allele_key
                and a.symbol like '%/%<%>%'
                and g._Marker_key = m._Marker_key
                and m._Marker_Type_key not in (12))
        ''',

        '''
        delete from tmp_simple s
        where not exists (select 1 from tmp_simple_genotype g where s._Genotype_key = g._Genotype_key)
	''',

	# delete genotypes that contain 'Gt(ROSA)' marker
        '''
        delete from tmp_simple s
        where s._Marker_key = 37270
        ''',

	# delete genotypes that are already designated as super-simple
	'''
        delete from tmp_simple s
        where exists (select 1 from tmp_supersimple g where s._Genotype_key = g._Genotype_key)
        ''',

	# sql (17)
	# super-simple
	# marker -> mp header term
	'''
	select distinct gg._Marker_key, v._AnnotType_key, v._Term_key, s.synonym, a.accID
	from tmp_supersimple g, VOC_AnnotHeader v, MGI_Synonym s, 
		GXD_AllelePair gg, ACC_Accession a
	where g._Genotype_key = v._Object_key
	and v._AnnotType_key = 1002
	and v._Term_key = s._Object_key
	and s._MGIType_key = 13
        and s._synonymtype_key = 1021
	and g._Genotype_key = gg._Genotype_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.preferred = 1
	''',

	#
	# sql (18-19)
	# build a temporary table of mouse genotype/annotation data
	#
	'''
        select distinct s._Marker_key, 
                m._Organism_key,
                v._AnnotType_key, 
                v._Term_key,
                v._Object_key as _Genotype_key, 
                a.accID, 
		t.term, 
		vv.name,
		t2.term as qualifier_type
	into temporary table tmp_mouse
        from tmp_supersimple s, MRK_Marker m,
                VOC_Annot v, VOC_Term t, VOC_Term t2, VOC_Vocab vv, 
                ACC_Accession a
        where s._Genotype_key = v._Object_key
        and s._Marker_key = m._Marker_key
        and (v._AnnotType_key in (1002)
		or
	     (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157))
        and v._Term_key = t._Term_key
	and v._Qualifier_key = t2._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
	''',

	'''
	create index tmp_mouse_genotype on tmp_mouse (_Genotype_key)
	''',

	# sql (20-21)
	# build a temporary table of human annotation data
	#
	'''
        select distinct v._Object_key as _Marker_key, 
		m._Organism_key,
		v._AnnotType_key, 
		v._Term_key, 
		a.accID, 
		t.term, 
		vv.name
	into temporary table tmp_human
        from VOC_Annot v , VOC_Term t, VOC_Vocab vv, ACC_Accession a, MRK_Marker m
        where v._AnnotType_key in (1006, 1013)
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
	and v._Object_key = m._Marker_key
        ''',

	'''
	create index tmp_human_marker on tmp_human (_Marker_key)
	''',

	#
	# sql (22)
	# disease-references by marker
	# to store distinct marker/reference used for mouse/OMIM (1005 only)
	#
	'''
        select distinct gg._Genotype_key, gg._Marker_key, e._Refs_key
        from VOC_Annot v, VOC_Evidence e, GXD_AlleleGenotype gg
	where gg._Genotype_key = v._Object_key
	and v._AnnotType_key = 1005 and v._Qualifier_key != 1614157
        and v._Annot_key = e._Annot_key
	''',

        #
        # sql (23)
        # disease-references by term
        # to store distinct term/reference used for mouse/OMIM (1005, 1012)
        #
        '''
        select distinct v._Term_key, e._Refs_key
        from VOC_Annot v, VOC_Evidence e
        where ((v._AnnotType_key = 1005 and v._Qualifier_key != 1614157)
		or
		v._AnnotType_key = 1012
		)
        and v._Annot_key = e._Annot_key
        ''',

	#
	# hdp_annotation table
	#

        # sql (24)
        # super-simple mouse genotypes (using temp table)
        '''
        select * from tmp_mouse
	''',

        # sql (25)
        # simple genotypes
        '''
        select distinct s._Marker_key, 
                m._Organism_key,
                v._AnnotType_key, 
                v._Term_key,
                v._Object_key, 
                a.accID, 
		t.term, 
		vv.name,
		t2.term as qualifier_type
        from tmp_simple s, MRK_Marker m,
                VOC_Annot v, VOC_Term t, VOC_Term t2, VOC_Vocab vv, 
                ACC_Accession a
        where s._Genotype_key = v._Object_key
        and s._Marker_key = m._Marker_key
        and (v._AnnotType_key in (1002)
		or
	     (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157))
        and v._Term_key = t._Term_key
	and v._Qualifier_key = t2._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        ''',

        # sql (26)
	# complex genotype
	# exclude: super-simple genotypes
	# exclude: simple genotypes
	# exclude: markers where there exists a double-wild-type allele pair
        '''
        select distinct g._Marker_key,
                m._Organism_key,
		v._AnnotType_key,
                v._Term_key, 
                v._Object_key,
                a.accID, 
		t.term, 
		vv.name,
		t2.term as qualifier_type
        from VOC_Annot v, VOC_Term t, VOC_Term t2, VOC_Vocab vv,
                GXD_AlleleGenotype g,
                ACC_Accession a, MRK_Marker m
        where (v._AnnotType_key in (1002)
		or
	     (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157))
        and v._Object_key = g._Genotype_key
        and g._Marker_key = m._Marker_key
        and v._Term_key = t._Term_key
	and v._Qualifier_key = t2._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
	and not exists (select 1 from tmp_supersimple s where s._Genotype_key = v._Object_key)
	and not exists (select 1 from tmp_simple s where s._Genotype_key = v._Object_key)
        and not exists (select 1 from tmp_exclude tx
                where g._Genotype_key = tx._Genotype_key
                and g._Marker_key = tx._Marker_key
                )
        ''',

	#
	# sql (27)
	# disease-references
	# to store distinct marker/reference used for allelle/OMIM annotations (1012)
	#
	'''
        select distinct al._Marker_key, e._Refs_key
        from VOC_Annot v, VOC_Evidence e, ALL_Allele al
	where al._Allele_key = v._Object_key
	and v._AnnotType_key = 1012
        and v._Annot_key = e._Annot_key
	''',

	#
        # sql (28)
        # allele/OMIM annotation data
	# exclude: Gt(ROSA)
	#
        '''
        select distinct m._Marker_key,
                m._Organism_key,
		v._AnnotType_key, 
                v._Term_key, 
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

	# sql (29)
	# human gene/OMIM annotation data (using temp table)
	'''
        select * from tmp_human
        ''',

        #
        # hdp_gridcluster
	# hdp_gridcluster_marker
	# hdp_gridcluster_annotation (includes OMIM, MP)
        #

        # sql (30)
	# super-simple genotypes
	# that contain homolgene clusters
	# plus
        # human with OMIM annotations where mouse contain homologene clusters
	#
	'''
	select distinct c._Cluster_key, 
		s._Marker_key, s._AnnotType_key, s._Term_key, s.term, s.accID
	into temporary table tmp_cluster
	from tmp_mouse s, MRK_ClusterMember c
	where s._Marker_key = c._Marker_key
        and not exists (select 1 from tmp_exclude tx
                where s._Genotype_key = tx._Genotype_key
                and s._Marker_key = tx._Marker_key
                )

	union

	select distinct c._Cluster_key, c._Marker_key,
		v._AnnotType_key, v._Term_key, v.term, v.accID
        from MRK_ClusterMember c, tmp_human v
        where c._Marker_key = v._Marker_key
	''',

	# sql (31)
	'''
	create index idx_cluster on tmp_cluster (_Cluster_key)
	''',
	# sql (32)
	'''
	create index idx_cluster_marker on tmp_cluster (_Marker_key)
	''',

        # sql (33) : 
	#
	# super-simple genotypes
	# that do NOT contain homolgene clusters
	# plus
        # human with OMIM annotations where mouse does NOT contain homologene clusters
	#
	# exclude: markers where there exists a double-wild-type allele pair
	#
	'''
	select distinct s._Marker_key, s._AnnotType_key, s._Term_key, s.term, s.accID
	into temporary table tmp_nocluster
	from tmp_mouse s
	where not exists (select 1 from tmp_cluster tc where s._Marker_key = tc._Marker_key)
        and not exists (select 1 from tmp_exclude tx
                where s._Genotype_key = tx._Genotype_key
                and s._Marker_key = tx._Marker_key
                )

	union

	select distinct c._Marker_key, v._AnnotType_key, v._Term_key, v.term, v.accID
        from MRK_Marker c, tmp_human v
        where c._Marker_key = v._Marker_key
	and not exists (select 1 from tmp_cluster tc where c._Marker_key = tc._Marker_key)
	''',

	# sql (34)
	'''
	create index idx_nocluster_marker on tmp_nocluster (_Marker_key)
	''',

	# sql (35)
	'''
	select * from tmp_cluster
	''',

	# sql (36)
	'''
	select * from tmp_nocluster
	''',

	# sql (37)
	# additional info for tmp_cluster-ed data
        '''
	select distinct c._Cluster_key, c._Marker_key, m._Organism_key, m.symbol, a.accID as homologene_id
	from MRK_ClusterMember c, MRK_Marker m, ACC_Accession a
	where c._Marker_key = m._Marker_key
	and m._Organism_key in (1,2)
	and c._Cluster_key = a._Object_key
        and a._LogicalDB_key = 81
	and exists (select 1 from tmp_cluster tc 
		where tc._Cluster_key = c._Cluster_key)
	order by c._Cluster_key
	''',

	# sql (38)
	# additional info for tmp_nocluster-ed data
        '''
	select distinct c._Marker_key, c._Organism_key, c.symbol
	from MRK_Marker c
	where c._Organism_key in (1,2)
	and exists (select 1 from tmp_nocluster tc where tc._Marker_key = c._Marker_key)
	order by c._Marker_key
	''',

	# sql (39)
	# mouse annotations by genotype
	'''
	select distinct _Genotype_key, _AnnotType_key, _Term_key, term, accID, qualifier_type from tmp_mouse
	''',

	# sql (40)
	# allele pair information in order to generate the genotype-cluster
	# only includes super-simple genotypes that contain mouse/MP or mouse/OMIM annotations
	# exclude: markers where there exists a double-wild-type allele pair
	'''
 	select p._Genotype_key, p._Marker_key,
               p._Allele_key_1, p._Allele_key_2, p._PairState_key,
               g.isConditional, g._ExistsAs_key
        from GXD_Genotype g, GXD_AllelePair p
        where g._Genotype_key = p._Genotype_key
        and exists (select 1 from tmp_supersimple c where c._Genotype_key = p._Genotype_key)
        and exists (select 1 from tmp_mouse c where c._Genotype_key = p._Genotype_key)
        and not exists (select 1 from tmp_exclude tx
                where p._Genotype_key = tx._Genotype_key
                and p._Marker_key = tx._Marker_key
                )
	''',

	]

# prefix for the filename of the output file
files = [
	('hdp_annotation',
		[ Gatherer.AUTO, '_Marker_key', '_Organism_key', 
			'_Term_key', '_AnnotType_key', 
			'_Object_key', 'genotype_type', 'qualifier_type',
			'accID', 'term', 'name', 'mp_header' ],
          'hdp_annotation'),

        ('hdp_marker_to_reference',
                [ Gatherer.AUTO, '_Marker_key', '_Refs_key', ],
          'hdp_marker_to_reference'),

        ('hdp_term_to_reference',
                [ Gatherer.AUTO, '_Term_key', '_Refs_key', ],
          'hdp_term_to_reference'),

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

	('hdp_genocluster_annotation',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Term_key',
		  '_AnnotType_key', 'qualifier_type', 'term_type', 'accID', 'term' ],
          'hdp_genocluster_annotation'),

	]

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
