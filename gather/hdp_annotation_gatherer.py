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
#	includes the header terms for each MP-term and disease/OMIM-term
#
#	super-simple (OMG): genotypes that contain only one marker
# 		include : non-wild type alleles
# 		include : genotypes that contains mouse/MP or mouse/OMIM annotations
# 		exclude : genotypes that contain 'slash' alleles and are not transgenes
#
#	simple : genotypes that have been "made" super-simple by excluding certain things
# 		include : wild type alleles where allele type is *not* 'Trangenic (Reporter)'
# 		include : genotypes that contains mouse/MP or mouse/OMIM annotations
# 		exclude : genotypes that contain driver notes (cre) AND is-conditional
# 		exclude : genotypes that contain 'slash' alleles and are not transgenes
# 		exclude : genotypes that contain 'Gt(ROSA)' marker
# 		exclude : genotypes already designated as super-simple
#
#	complex : neither super-simple nor simple
# 		exclude: super-simple genotypes
# 		exclude: simple genotypes
# 		exclude: markers where there exists a double-wild-type allele pair
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
# 	super-simple + simple genotypes that contain mouse/MP or mouse/OMIM annotations
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
#		includes the header terms for MP-term and disease/OMIM-term
#
# hdp_genocluster
# hdp_genocluster_genotype
# hdp_genocluster_annotation
#
# 	super-simple + simple genotypes that contain mouse/MP or mouse/OMIM annotations
#
#	create a genotype-cluster by aggregating:
#		marker, allele1, allele2, pair state, is-conditional, genotype-exists
#
#	'genotype' : genotypes that belong to a specific geno-cluster
#
#	'annotation' : annotations associated with a geno-cluster
#		includes annotation type, term info
#		includes the header terms for MP-term and disease/OMIM-term
#
# 07/19/2013	lec
#	- TR11423/Human Disease Portal
#

import Gatherer
import MPSorter
import logger

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
	#	collates results, writes tab-delimited text file

	# post-process all the hdp_annotation results and add the term_seq and term_depth columns
	def calculateTermSeqs(self,annotResults):
		# sql (43) : term sequencenum values for MP
		logger.info("preparing to calculate term sequences for hdp_annotation results")
		# get term sequencenum from voc_term
		origTermSeqDict = {}
		(cols, rows) = self.results[43]
		for row in rows:
			origTermSeqDict[row[0]] = row[1]
		
		# colums of an annotResult
		#		0'_Marker_key', 
		#		1'_Organism_key', 
		#		2'_Term_key', 
		#		3'_AnnotType_key', 
		#		4'_Object_key', 
		#		5'genotype_type',
		#		6'qualifier_type',
		#		7'accID',
		#		8'term',
		#		9'name',
		#		10'header',
		#		11'term_seq',
		#		12'term_depth'
		# loop through once in order to get the terms for each gridcluster/header group
		clusterHeaderTermDict = {}
		logger.debug("looping annotResults 1st time to group the terms")
		for annotResult in annotResults:
			markerKey = annotResult[0]
			termKey = annotResult[2]
			genotype_type = annotResult[5]
			vocab = annotResult[9]
			if markerKey \
				and genotype_type in ('super-simple', 'simple') \
				and markerKey in self.markerClusterKeyDict \
				and vocab == 'Mammalian Phenotype':
				header = annotResult[10]
				if header in self.mpHeaderKeyDict:
					headerKey = self.mpHeaderKeyDict[header]
					gridClusterKey = self.markerClusterKeyDict[markerKey] 
					mapKey = (gridClusterKey,headerKey)
					origTermSeq = origTermSeqDict[termKey]
					clusterHeaderTermDict.setdefault(mapKey,set([])).add((termKey,origTermSeq))

		logger.debug("calculating sequences for the term groups")
		# now go through the groups to calculate sorts
		mpSorter = MPSorter.MPSorter()
		clusterHeaderTermSeqDict = {}
		for mapKey,terms in clusterHeaderTermDict.items():
			gridClusterKey,headerKey = mapKey
			termSeqMap = mpSorter.calculateSortAndDepths(terms,headerKey)
			clusterHeaderTermSeqDict[mapKey] = termSeqMap	

		# this can be cleared
		clusterHeaderTermDict = {}

		# loop through a second time to append the calculated sequence values
		logger.debug("looping through a second time to append the relative term sequences to the annotResults")
		for annotResult in annotResults:
			markerKey = annotResult[0]
			termKey = annotResult[2]
			genotype_type = annotResult[5]
			vocab = annotResult[9]
			termSeq = None
			termDepth = None
			if markerKey \
				and genotype_type in ('super-simple', 'simple') \
				and markerKey in self.markerClusterKeyDict \
				and vocab == 'Mammalian Phenotype':
				header = annotResult[10]
				if header in self.mpHeaderKeyDict:
					headerKey = self.mpHeaderKeyDict[header]
					gridClusterKey = self.markerClusterKeyDict[markerKey] 
					mapKey = (gridClusterKey,headerKey)
					if mapKey in clusterHeaderTermSeqDict \
						and termKey in clusterHeaderTermSeqDict[mapKey]:
						termSeqMap = clusterHeaderTermSeqDict[mapKey][termKey]
						termSeq = termSeqMap["seq"]
						termDepth = termSeqMap["depth"]
			# add term_seq
			annotResult.append(termSeq)
			# add term_depth
			annotResult.append(termDepth)

		logger.info("done calculating term sequences for hdp_annotation results")
		return annotResults


	def processAnnotations(self, annotResults, diseaseMarkerRefResults, diseaseTermRefResults):
		#
		# hdp_annotation
		# hdp_marker_to_reference
		# hdp_term_to_reference
		#
		# sql (23)
		# disease-references by marker
		#
                # sql (24)
                # disease-term-referencees
		#
		# sql (25)
		# super-simple + simple genotypes
		#
                # sql (26)
		# complex genotypes
		#
		# sql (28)
		# allele/OMIM annotations
		#
		# sql (29)
		# human gene/OMIM annotations
		#

		# sql (23)
		# disease-references by marker
		# to store distinct marker/reference used for mouse/OMIM
                diseaseMarkerRef1Dict = {}
		(cols, rows) = self.results[23]
                key1 = Gatherer.columnNumber (cols, '_Genotype_key')
                key2 = Gatherer.columnNumber (cols, '_Marker_key')
                for row in rows:
			diseaseMarkerRef1Dict.setdefault((row[key1], row[key2]),[]).append(row)
		#logger.debug (diseaseMarkerRef1Dict)

                # sql (24)
                # disease-term-referencees
                diseaseTermRefDict = {}
                (cols, rows) = self.results[24]
                key = Gatherer.columnNumber (cols, '_Term_key')
                for row in rows:
			diseaseTermRefDict.setdefault(row[key],[]).append(row)
		#logger.debug (diseaseTermRefDict[847181])

		# sql (25)
		# super-simple + simple genotypes
		# contains MP + OMIM annotations
		logger.debug ('start : processed super-simple + simple mouse annotations')
		(cols, rows) = self.results[25]

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
		genotypeTypeCol = Gatherer.columnNumber (cols, 'genotype_type')

		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]
			annotType = row[annotTypeKeyCol]
			termKey = row[termKeyCol]
			genotype_type = row[genotypeTypeCol]

			# header = mp header term
			if self.mpHeaderDict.has_key(termKey):
				for header in self.mpHeaderDict[termKey]:
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
						header,
						])

			# header = disease header term
			elif self.diseaseHeaderDict.has_key(termKey):
				for header in self.diseaseHeaderDict[termKey]:
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
						header,
						])

			# header = disease-term
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
					row[termCol],
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

		logger.debug ('end : processed super-simple + simple mouse annotations')

                # sql (26)
		# complex genotypes
		# contains MP + OMIM annotations
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
		genotypeTypeCol = Gatherer.columnNumber (cols, 'genotype_type')

                for row in rows:

			termKey = row[termKeyCol]
			genotype_type = row[genotypeTypeCol]

			# header = mp header term
			if self.mpHeaderDict.has_key(termKey):
				for header in self.mpHeaderDict[termKey]:
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
						header,
						])

			# header = disease header term
			elif self.diseaseHeaderDict.has_key(termKey):
				for header in self.diseaseHeaderDict[termKey]:
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
						header,
						])

			# header = disease-term
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
					row[termCol],
					])

                        # then store the *unique* term/reference association
                        if annotType in [1005] and diseaseTermRefDict.has_key(termKey):
                                for m in diseaseTermRefDict[termKey]:
                                	if [termKey, m[1]] not in diseaseTermRefResults:
                                        	diseaseTermRefResults.append ( [ termKey, m[1], ])

		logger.debug ('end : processed complex mouse annotations')

		# sql (27)
		# disease-marker-referencees
		# to store distinct marker/reference used for allelle/OMIM annotations
                diseaseMarkerRef2Dict = {}
		(cols, rows) = self.results[27]
                key = Gatherer.columnNumber (cols, '_Marker_key')
                for row in rows:
			diseaseMarkerRef2Dict.setdefault(row[key],[]).append(row)
		#logger.debug (diseaseMarkerRef2Dict)

		# sql (28)
		# allele/OMIM annotations
		# contains OMIM annotations only
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

			# header = disease header term
			if self.diseaseHeaderDict.has_key(termKey):
				for header in self.diseaseHeaderDict[termKey]:
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
						header,
                        			])

			# header = disease-term
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
					row[termCol],
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

		logger.debug ('end : processed allele/OMIM annotatins')

		# sql (29)
		# human gene/OMIM annotations
		# contains OMIM annotations only
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
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier_type')

		for row in rows:
			# no genotypes

			termKey = row[termKeyCol]

			# header = disease header term
			if self.diseaseHeaderDict.has_key(termKey):
				for header in self.diseaseHeaderDict[termKey]:
					annotResults.append ( [ 
                                		row[markerKeyCol],
						row[organismKeyCol],
						termKey,
						row[annotTypeKeyCol],
						None,
						None,
						row[qualifierCol],
						row[termIDCol],
						row[termCol],
						row[vocabNameCol],
						header,
						])

			# header = disease-term
			else:
				annotResults.append ( [ 
                                	row[markerKeyCol],
					row[organismKeyCol],
					termKey,
					row[annotTypeKeyCol],
					None,
					None,
					row[qualifierCol],
					row[termIDCol],
					row[termCol],
					row[vocabNameCol],
					row[termCol],
					])

		logger.debug ('end : processed human OMIM annotations')

		return annotResults, diseaseMarkerRefResults, diseaseTermRefResults

	def processGridCluster(self, clusterResults, cmarkerResults, cannotResults):

		#
		# hdp_gridcluster
		# hdp_gridcluster_marker
		# hdp_gridcluster_annotation
		#
		# sql (22)
		# marker -> mp header term/accession id
		#
		# sql (35)
		# homologene clusters
		#
		# sql (36)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		#
                # sql (37) : annotations that contain homologene clusters
		#
                # sql (38) : annotations that do-not contain homologene clusters
		#

		logger.debug ('start : processed mouse/human genes with homolgene clusters')

		#
		# sql (22)
		# marker -> mp header term/accession id
		markerHeaderDict = {}
		(cols, rows) = self.results[22]
		key = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			markerHeaderDict.setdefault(row[key],[]).append(row)

		# sql (35)
		# homologene clusters
		clusterDict1 = {}
		(cols, rows) = self.results[35]
		clusterKey = Gatherer.columnNumber (cols, '_Cluster_key')
		for row in rows:
			clusterDict1.setdefault(row[clusterKey],[]).append(row)
		#logger.debug (clusterDict1)

		# sql (36)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		clusterDict2 = {}
		(cols, rows) = self.results[36]
		markerKey = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			clusterDict2.setdefault(row[markerKey],[]).append(row)
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

		self.markerClusterKeyDict = {}
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
				self.markerClusterKeyDict[markerKey] = clusterKey
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
                                                        'term',
                                                        termId,
                                                        termName
                                                        ])

                                                cannotList.add((clusterKey, termKey))

                        if markerHeaderDict.has_key(markerKey):
                                for markerHeader in markerHeaderDict[markerKey]:
                                        cannotResults.append( [
                                                clusterKey,
                                                markerHeader[2],
                                                markerHeader[1],
                                                'header',
                                                markerHeader[4],
                                                markerHeader[3],
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
							'term',
							termId,
							termName
							])

						cannotList.add((markerKey, termKey))

			if markerHeaderDict.has_key(markerKey):
				for markerHeader in markerHeaderDict[markerKey]:
                                        cannotResults.append( [
                                                clusterKey,
                                                markerHeader[2],
                                                markerHeader[1],
                                                'header',
                                                markerHeader[4],
                                                markerHeader[3],
                                                ])

		logger.debug ('end : processed mouse/human genes without homolgene clusters')

		return clusterResults, cmarkerResults, cannotResults

	def processGenotypeCluster(self, gClusterResults, gResults, gannotResults):

		#
		# hdp_genocluster
		# hpd_genocluster_genotype
		# hpd_genocluster_annotation
		#
		# sql (39) : genotype-cluster : by annotation
                # sql (40) : genotype-cluster : counts
		# sql (41) : genotype-cluster : genotype/marker
		# sql (42) : genotype-cluster : allele pairs
		#

		logger.debug ('start : processed genotype cluster')

		# sql (39) : genotype-cluster : by annotation
		clusterAnnotDict = {}
		(cols, rows) = self.results[39]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		for row in rows:
			clusterAnnotDict.setdefault(row[genotypeKeyCol],[]).append(row)

                # sql (40) : genotype-cluster : counts
                logger.debug ('start : processed genotype cluster counts')
                genoTermRefDict = {}
                (cols, rows) = self.results[40]
                key1 = Gatherer.columnNumber (cols, '_Genotype_key')
                key2 = Gatherer.columnNumber (cols, '_Term_key')
                key3 =  Gatherer.columnNumber (cols, '_Qualifier_key')
                key4 =  Gatherer.columnNumber (cols, 'refCount')
                for row in rows:
			genoTermRefDict.setdefault((row[key1], row[key2], row[key3]),[]).append(row[key4])

		# sql (41) : genotype-cluster : genotype/marker
		genoMarkerList = set([])
		(cols, rows) = self.results[41]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			genoMarkerList.add((row[genotypeKeyCol], row[markerKeyCol]))

		# sql (42) : genotype-cluster : allele pairs
		(cols, rows) = self.results[42]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		allele1KeyCol = Gatherer.columnNumber (cols, '_Allele_key_1')
		allele2KeyCol = Gatherer.columnNumber (cols, '_Allele_key_2')
		pairStateKeyCol = Gatherer.columnNumber (cols, '_PairState_key')
		isConditionalKeyCol = Gatherer.columnNumber (cols, 'isConditional')
		existsAsKeyCol = Gatherer.columnNumber (cols, '_ExistsAs_key')

		#
		# store the genotype/allele-pair info
		#

		byID = {}
        	for row in rows:
                	gSet = row[genotypeKeyCol]
                	cSet = (row[markerKeyCol], \
                        	row[allele1KeyCol], \
                        	row[allele2KeyCol], \
                        	row[pairStateKeyCol], \
                        	row[isConditionalKeyCol],
                        	row[existsAsKeyCol])
			byID.setdefault(gSet,[]).append(cSet)

		#
		# build dictionary of all genotypes 
		# organize by the number of allele-pairs in the genotype
		#

        	byPairCount = {}
        	for r in byID:
                	key = len(byID[r])
                	byPairCount.setdefault(key,{}).setdefault(r,[]).append(byID[r])
		#logger.debug (byPairCount)

		#
		# cluster genotypes by comparing #-of-allele-pairs + allele-pair-info
		#

		compressSet = {}
		clusterKey = 1

        	for r in byPairCount:

                	# r is a count of allele pairs within a genotype
                	# diff1 is a dictionary where each genotype key refers to a list of allele pairs

                	diff1 = byPairCount[r]

			# flip the data around and cluster by allele pair => genotype
                	# cluster[str(allele pairs)] = [ genotype keys ]
                	cluster = {}

                	for d1 in diff1:

                        	pairs = str(diff1[d1])
	
                        	if cluster.has_key(pairs):
                                	cluster[pairs].append(d1)
                        	else:
                                	cluster[pairs] = [d1]

                	# assign a cluster-key to each group of allele pairs

                	for pairs in cluster.keys():
                        	compressSet[clusterKey] = {}

                        	for genotypeKey in cluster[pairs]:
                                	compressSet[clusterKey][genotypeKey] = diff1[genotypeKey]
                        	clusterKey = clusterKey + 1

		#logger.debug (compressSet)

	        # at most one cluster/term/qualifier per geno-cluster
		gannotTermList = set([])

		for clusterKey in compressSet:

                	# at most one cluster/header-term in a genoCluster_annotation
			# contains both MP and Disease (OMIM) headers
                	gannotHeader = {}

                        # for each genotype in the cluster
                        # track the genotype-term-reference-count for this cluster
                        clusterAnnotCount = {}
                        for gKey in compressSet[clusterKey]:
                                if clusterAnnotDict.has_key(gKey):
                                        for c in clusterAnnotDict[gKey]:
                                                termKey = c[2]
						qualifierKey = c[3]
                                                tkey = (gKey, termKey, qualifierKey)
                                                if genoTermRefDict.has_key(tkey):
                                                        newCount = genoTermRefDict[tkey][0]
							ckey = (termKey, qualifierKey)
                                                        if not clusterAnnotCount.has_key(ckey):
                                                                clusterAnnotCount[ckey] = newCount
                                                        else:
                                                                clusterAnnotCount[ckey] += newCount
			#logger.debug (clusterAnnotCount)


                        # for each genotype in the cluster
			# track the has-background-note (1 or 0) for each cluster/term/qualifier
			# 1 trumps 0
			backgroundDict = {}
			for gKey in compressSet[clusterKey]:
				for c in clusterAnnotDict[gKey]:
					termKey = c[2]
					qualifierKey = c[3]
                                       	hasBackgroundNote = c[7]

					key = (termKey, qualifierKey)
					if backgroundDict.has_key(key):
						if backgroundDict[key] == 0:
							backgroundDict[key] = hasBackgroundNote
					else:
						backgroundDict[key] = hasBackgroundNote
			#logger.debug (backgroundDict)

                        # for each genotype in the cluster
                        # add the cluster-genotypes
                        # add the cluster-annotations
			for gKey in compressSet[clusterKey]:

				gResults.append( [
					clusterKey,
					gKey,
					])
				
				if clusterAnnotDict.has_key(gKey):

					for c in clusterAnnotDict[gKey]:

						annotationKey = c[1]
						termKey = c[2]
						qualifierKey = c[3]
						termName = c[4]
						termId = c[5]
						qualifier = c[6]
                                                hasBackgroundNote = backgroundDict[(termKey, qualifierKey)]

                                                # at most one cluster/term/qualifier per geno-cluster
                                                if (clusterKey, termKey, qualifier) in gannotTermList:
                                                        continue

                                                # get the cluster-annotation-count
                                                geno_count = 0
                                                if clusterAnnotCount.has_key((termKey, qualifierKey)):
                                                        geno_count = clusterAnnotCount[(termKey, qualifierKey)]

						gannotResults.append( [ 
			    				clusterKey,
							termKey,
							annotationKey,
							qualifier,
							'term',
							termId,
							termName,
							hasBackgroundNote,
							geno_count
							])

						gannotTermList.add((clusterKey, termKey, qualifier))

						# header = mp header term
						if self.mpHeaderDict.has_key(termKey):
                                                        for header in self.mpHeaderDict[termKey]:
                                        			if (annotationKey, qualifier, header) not in gannotHeader:
                                                			gannotHeader[annotationKey, qualifier, header] = geno_count
								else:
                                                			gannotHeader[annotationKey, qualifier, header] += geno_count

						# header = disease header term
						elif self.diseaseHeaderDict.has_key(termKey):
                                                        for header in self.diseaseHeaderDict[termKey]:
                                        			if (annotationKey, qualifier, header) not in gannotHeader:
                                                			gannotHeader[annotationKey, qualifier, header] = geno_count
								else:
                                                			gannotHeader[annotationKey, qualifier, header] += geno_count

						# header = disease-term
						else:
							header = termName
                                        		if (annotationKey, qualifier, header) not in gannotHeader:
                                                		gannotHeader[annotationKey, qualifier, header] = geno_count
							else:
                                                		gannotHeader[annotationKey, qualifier, header] += geno_count

			#
			# within each cluster
			# determine the header/qualifier for each term
			# all normals trumps non-normals (null) qualifier
			#
			#logger.debug (gannotHeader)

			for gheader in gannotHeader:

				annotationKey = gheader[0]
				qualifier = gheader[1]
				header = gheader[2]
				geno_count = gannotHeader[gheader]

				# for the given annotation-key and header-term

				# if both normal-qualifier and null-qualifier exist, use null-qualifier
				# or
				# if only null-qualifier exists, use it

				if ((qualifier == 'normal' and (annotationKey, None, header) in gannotHeader) \
					or \
				   (qualifier == None and (annotationKey, 'normal', header) not in gannotHeader)):
					gannotResults.append([clusterKey, None, annotationKey,
						None, 'header', None, header, 0, geno_count])

				# else if only normal-qualifier exists, then use it

				elif (qualifier == 'normal' and (annotationKey, None, header) not in gannotHeader):
					gannotResults.append([clusterKey, None, annotationKey,
						qualifier, 'header', None, header, 0, geno_count])

				# do nothing...as this would create a duplicate row in gannotResults
				#elif (qualifier == None and (annotationKey, 'normal', header) in gannotHeader)

		#
		# ready to push the compressedSet into the gClusterResults
		#

		for clusterKey in compressSet:
			# the cluster-result (cluster key, allele-pair info)
			for gKey in compressSet[clusterKey]:
				for ap1 in compressSet[clusterKey][gKey]:
					for ap2 in ap1:
						if (gKey, ap2[0]) in genoMarkerList \
							and ([clusterKey, ap2[0]]) not in gClusterResults:
							gClusterResults.append([clusterKey, ap2[0]])

		logger.debug ('end : processed genotype cluster')

		return gClusterResults, gResults, gannotResults

        def collateResults(self):

		#
		# sql (0)
		# mp term -> mp header term
		# includes parent + children
		self.mpHeaderDict = {}
		self.mpHeaderKeyDict = {}
		(cols, rows) = self.results[0]
		termKey = Gatherer.columnNumber (cols, '_Object_key')
		headerKey = Gatherer.columnNumber (cols, 'header_key')
		header = Gatherer.columnNumber (cols, 'synonym')
	
		for row in rows:
			# map term_keys to their header display
			self.mpHeaderDict.setdefault(row[termKey],[]).append(row[header])
			# also map the header display string to its actual header term_key (for convenience)
			self.mpHeaderKeyDict[row[header]] = row[headerKey]
		#logger.debug (self.mpHeaderDict)

		#
		# sql (1)
		# disease term -> disease header term
		self.diseaseHeaderDict = {}
		(cols, rows) = self.results[1]
		termKey = Gatherer.columnNumber (cols, '_Object_key')
		header = Gatherer.columnNumber (cols, 'synonym')
	
		for row in rows:
			# map term_keys to their header display
			self.diseaseHeaderDict.setdefault(row[termKey],[]).append(row[header])
		#logger.debug (self.diseaseHeaderDict)

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
				'header',
				'term_seq',
				'term_depth'
			]

                diseaseMarkerRefResults = []
                diseaseMarkerRefCols = ['_Marker_key', 
                                '_Refs_key', 
                        ]

                diseaseTermRefResults = []
                diseaseTermRefCols = ['_Term_key',
                                '_Refs_key',
                        ]

                logger.debug ('start : hdp_annotation')
                annotResults, diseaseMarkerRefResults, diseaseTermRefResults = \
                        self.processAnnotations(annotResults, diseaseMarkerRefResults, diseaseTermRefResults)
                logger.debug ('end : processed hdp_annotation')

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

                logger.debug ('start : hdp_gridcluster')
                clusterResults, cmarkerResults, cannotResults = \
                        self.processGridCluster(clusterResults, cmarkerResults, cannotResults)
                logger.debug ('end : processed hdp_gridcluster')

		# relies on 'processGridCluster:self.markerClusterKeyDict'
		logger.debug("start : calculate termSeqs")	
		annotResults = self.calculateTermSeqs(annotResults)
		logger.debug("end : calculate termSeqs")	

		#
		# hdp_genocluster
		# hpd_genocluster_genotype
		# hpd_genocluster_annotation
		#

		gClusterResults = []
		gClusterCols = [ 'hdp_genocluster_key', 
				'_Marker_key', 
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
				'has_backgroundnote',
				'genotermref_count'
			]

		logger.debug ('start : hdp_genocluster')
		gClusterResults, gResults, gannotResults = \
			self.processGenotypeCluster(gClusterResults, gResults, gannotResults)
		logger.debug ('end : hdp_genocluster')

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
        select distinct d._Object_key, h.synonym, h._object_key header_key
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

        select distinct d._Object_key, h.synonym, h._object_key header_key
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

	# sql (1)
	# disease term -> disease header term
	#
	'''
        select distinct s._Object_key, s.synonym
        from MGI_Synonym s
	where s._SynonymType_key = 1031
	''',

	# sql (2-3) 
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

        # sql (4-9)
	# super-simple genotypes
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

        # sql (10-17)
	# simple genotypes
	# include : wild type alleles where allele type is *not* 'Trangenic (Reporter)'
	# include : genotypes that contains mouse/MP or mouse/OMIM annotations
	# exclude : genotypes that contain driver notes (cre) AND is-conditional
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

	#
	# sql (18-19)
	# build a temporary table of mouse genotype/annotation data
	# to include the genotype_type(s) that are of interest for
	# the main hdp_annotation table, grid-cluster and geno-cluster
	#
	# includes:
	#	tmp_supersimple
	#	tmp_simple
	# exclude:
	#	MP:0003012/phenotype not analyzed
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
		t2.term as qualifier_type,
		v._Qualifier_key,
		'super-simple' as genotype_type
	into temporary table tmp_annot_mouse
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
	and a.accID not in ('MP:0003012')
        and t._Vocab_key = vv._Vocab_key
	and m._Marker_key != 37270

	union

        select distinct s._Marker_key, 
                m._Organism_key,
                v._AnnotType_key, 
                v._Term_key,
                v._Object_key as _Genotype_key, 
                a.accID, 
		t.term, 
		vv.name,
		t2.term as qualifier_type,
		v._Qualifier_key,
		'simple' as genotype_type
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
	and a.accID not in ('MP:0003012')
        and t._Vocab_key = vv._Vocab_key
	and m._Marker_key != 37270
	''',

	'''
	create index tmp_annot_mouse_genotype on tmp_annot_mouse (_Genotype_key)
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
		vv.name,
		t2.term as qualifier_type
	into temporary table tmp_annot_human
        from VOC_Annot v , VOC_Term t, VOC_Term t2, VOC_Vocab vv, ACC_Accession a, MRK_Marker m
        where v._AnnotType_key in (1006, 1013)
        and v._Term_key = t._Term_key
	and v._Qualifier_key = t2._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
	and v._Object_key = m._Marker_key
        ''',

	'''
	create index tmp_annot_human_marker on tmp_annot_human (_Marker_key)
	''',

	# sql (22)
	# marker -> mp header term
	'''
	select distinct s._Marker_key, v._AnnotType_key, v._Term_key, ms.synonym, a.accID
	from tmp_annot_mouse s, VOC_AnnotHeader v, MGI_Synonym ms, ACC_Accession a
	where s._Genotype_key = v._Object_key
	and v._AnnotType_key = 1002
	and v._Term_key = ms._Object_key
	and ms._MGIType_key = 13
        and ms._synonymtype_key = 1021
        and v._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a.preferred = 1
	''',

	#
	# sql (23)
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
        # sql (24)
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

        # sql (25)
        # super-simple + simple mouse genotypes (using temp table)
        '''
        select * from tmp_annot_mouse
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
		t2.term as qualifier_type,
		v._Qualifier_key,
		'complex' as genotype_type
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
	and a.accID not in ('MP:0003012')
        and t._Vocab_key = vv._Vocab_key
	and not exists (select 1 from tmp_supersimple s where s._Genotype_key = v._Object_key)
	and not exists (select 1 from tmp_simple s where s._Genotype_key = v._Object_key)
        and not exists (select 1 from tmp_exclude tx
                where g._Genotype_key = tx._Genotype_key
                and g._Marker_key = tx._Marker_key
                )
	and m._Marker_key != 37270
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
        select * from tmp_annot_human
        ''',

        #
        # hdp_gridcluster
	# hdp_gridcluster_marker
	# hdp_gridcluster_annotation (includes OMIM, MP)
        #

        # sql (30)
	# super-simple + simple genotypes
	# that contain homolgene clusters
	# plus
        # human with OMIM annotations where mouse contain homologene clusters
	#
	'''
	select distinct c._Cluster_key, 
		s._Marker_key, s._AnnotType_key, s._Term_key, s.term, s.accID
	into temporary table tmp_cluster
	from tmp_annot_mouse s, MRK_ClusterMember c
	where s._Marker_key = c._Marker_key

	union

	select distinct c._Cluster_key, c._Marker_key,
		v._AnnotType_key, v._Term_key, v.term, v.accID
        from MRK_ClusterMember c, tmp_annot_human v
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
	# super-simple + simple genotypes
	# that do NOT contain homolgene clusters
	# plus
        # human with OMIM annotations where mouse does NOT contain homologene clusters
	#
	'''
	select distinct s._Marker_key, s._AnnotType_key, s._Term_key, s.term, s.accID
	into temporary table tmp_nocluster
	from tmp_annot_mouse s
	where not exists (select 1 from tmp_cluster tc where s._Marker_key = tc._Marker_key)

	union

	select distinct c._Marker_key, v._AnnotType_key, v._Term_key, v.term, v.accID
        from MRK_Marker c, tmp_annot_human v
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
        # super-simple + simple
        # mouse annotations by genotype
        # *make sure the qualifier is order in descending order*
        # as this affects the setting of the mp-header
        # pull in the 'has_backgrounote' for each genotype/annotation
        '''
	(
        select distinct _Genotype_key, _AnnotType_key, _Term_key, _Qualifier_key, term, accID, qualifier_type,
                1 as has_backgroundnote
        from tmp_annot_mouse t
        where exists (select 1 from VOC_Annot a, VOC_Evidence e, MGI_Note n
                where t._AnnotType_key = a._AnnotType_key
                and t._Term_key = a._Term_key
                and t._Genotype_key = a._Object_key
                and t._Qualifier_key = a._Qualifier_key
                and a._Annot_key = e._Annot_key
                and e._AnnotEvidence_key = n._Object_key
                and n._MGIType_key = 25
                and n._NoteType_key = 1015)
        union
        select distinct _Genotype_key, _AnnotType_key, _Term_key, _Qualifier_key, term, accID, qualifier_type,
                0 as has_backgroundnote
        from tmp_annot_mouse t
        where not exists (select 1 from VOC_Annot a, VOC_Evidence e, MGI_Note n
                where t._AnnotType_key = a._AnnotType_key
                and t._Term_key = a._Term_key
                and t._Genotype_key = a._Object_key
                and t._Qualifier_key = a._Qualifier_key
                and a._Annot_key = e._Annot_key
                and e._AnnotEvidence_key = n._Object_key
                and n._MGIType_key = 25
                and n._NoteType_key = 1015)
	)
	order by _Genotype_key, _Term_key, _Qualifier_key, qualifier_type desc
        ''',

        # sql (40)
        # counts by geno-cluster/term/qualifier/reference
        '''
        select distinct v._Object_key as _Genotype_key, v._Term_key, v._Qualifier_key, count(_Refs_key) as refCount
        from VOC_Annot v, VOC_Evidence e
        where v._AnnotType_key in (1002, 1005)
        	and v._Annot_key = e._Annot_key
	        and v._Term_key not in (293594)
        group by v._Object_key, v._Term_key, v._Qualifier_key
        ''',

	# sql (41)
	# distinct genotype/marker
	'''
	select distinct _Genotype_key, _Marker_key
	from tmp_annot_mouse
	''',

	# sql (42)
	# allele pair information in order to generate the genotype-cluster
	# include: super-simple + simple (tmp_annot_mouse)
	# exclude: markers where there exists a double-wild-type allele pair
        #and g._Genotype_key in (64589, 64590)
	#and g._Genotype_key in (29408,29785,55180,58374,9470,55180,21739,58296,59)
	'''
        select distinct p._Genotype_key, p._Marker_key,
               p._Allele_key_1, p._Allele_key_2, p._PairState_key,
               g.isConditional, g._ExistsAs_key
        from GXD_Genotype g, GXD_AllelePair p
        where g._Genotype_key = p._Genotype_key
        	and exists (select 1 from tmp_annot_mouse c where c._Genotype_key = p._Genotype_key)
        	and not exists (select 1 from tmp_exclude tx
                	where p._Genotype_key = tx._Genotype_key
                	and p._Marker_key = tx._Marker_key
                	)
	''',

	# sql (43)
	# term sequencenum values for MP
	'''
	select _Term_key, sequenceNum from VOC_Term where _Vocab_key = 5
	''',

	]

# prefix for the filename of the output file
files = [
	('hdp_annotation',
		[ Gatherer.AUTO, '_Marker_key', '_Organism_key', 
			'_Term_key', '_AnnotType_key', 
			'_Object_key', 'genotype_type', 'qualifier_type',
			'accID', 'term', 'name', 'header','term_seq','term_depth' ],
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
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Marker_key',
		   ],
          'hdp_genocluster'),

	('hdp_genocluster_genotype',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Genotype_key' ],
          'hdp_genocluster_genotype'),

	('hdp_genocluster_annotation',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Term_key',
		  '_AnnotType_key', 'qualifier_type', 'term_type', 'accID', 'term',
		  'has_backgroundnote', 'genotermref_count' ],
          'hdp_genocluster_annotation'),

	]

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
