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

VOCAB = 13		# MGI Type for vocabulary terms
CLUSTER = 39		# MGI Type for marker cluster

HYBRID = 13764519	# cluster source for hybrid (HomoloGene and HGNC)
HOMOLOGENE = 9272151	# cluster source for MRK_Cluster (HomoloGene)
HGNC = 13437099		# cluster source for MRK Cluster (HGNC)
HOMOLOGY = 9272150	# cluster type for MRK_Cluster

NOT_QUALIFIER = 1614157	# term key for NOT qualifier for OMIM annotations

MP_GENOTYPE = 1002		# annot type for MP/genotype annotations
OMIM_GENOTYPE = 1005		# annot type for OMIM/genotype annotations
OMIM_HUMAN_MARKER = 1006	# annot type for OMIM/human marker annotations
OMIM_HUMAN_PHENO_MARKER = 1013	# annot type for OMIM/human phenotypic markers
OMIM_ALLELE = 1012		# annot type for direct OMIM/allele annotations
MP_MARKER = 1015		# annot type for rolled-up MP annotations
OMIM_MARKER = 1016		# annot type for rolled-up OMIM annotations

MP_HEADER_SYNONYM_TYPE = 1021	# synonym type key for phenotype headers
DISEASE_SYNONYM_TYPE = 1031	# synonym type key for disease cluster headers

GT_ROSA = 37270			# marker key for Gt(ROSA)26Sor

MARKER_CACHE = {}	# maps from marker key to (organism, symbol)

# maps from hybrid cluster key to data about the homology cluster which was
# the source for this cluster:
#    hybrid cluster key -> (source cluster key, source name, cluster acc ID)
HYBRID_TO_SOURCE = {}

# maps from marker key to data about the homology cluster which is the source
# cluster (for the hybrid homology cluster containing the marker):
#    marker key -> (source cluster key, source name, cluster acc ID)
CLUSTER_MAP = {}

###--- Functions ---###

def initializeMarkerCache(results34):
	# initialize the cache of marker symbols and organisms

	global MARKER_CACHE

	(cols, rows) = results34

	markerCol = Gatherer.columnNumber(cols, '_Marker_key')
	organismCol = Gatherer.columnNumber(cols, 'commonName')
	symbolCol = Gatherer.columnNumber(cols, 'symbol')

	for row in rows:
		organism = row[symbolCol]
		if organism.startswith('mouse'):
			organism = 'mouse'

		MARKER_CACHE[row[markerCol]] = (organism, row[symbolCol])
	logger.debug('Cached %d marker symbols and organisms' % \
		len(MARKER_CACHE))
	return

def getSymbol(markerKey):
	if MARKER_CACHE.has_key(markerKey):
		return MARKER_CACHE[markerKey][1]
	return None

def getOrganism(markerKey):
	if MARKER_CACHE.has_key(markerKey):
		return MARKER_CACHE[markerKey][0]
	return None 

def initializeClusterMap(results32, results33):
	# initialize the global CLUSTER_MAP, which will tell us which cluster
	# key to associate with each marker

	global CLUSTER_MAP, HYBRID_TO_SOURCE

	# sql 32 : identify which type of cluster is the source for each
	# hybrid cluster

	# maps marker key -> cluster source key
	# Which homology source (HGNC or HomoloGene) is the preferred one for
	# this marker, as determined by the hybrid algorithm?
	source = {}

	# maps marker key -> cluster key
	# What is the hybrid cluster key for this marker?
	markerToCluster = {}

	(cols, rows) = results32

	markerCol = Gatherer.columnNumber(cols, '_Marker_key')
	valueCol = Gatherer.columnNumber(cols, 'value')
	clusterCol = Gatherer.columnNumber(cols, '_Cluster_key')

	for row in rows:
		value = row[valueCol]
		markerKey = row[markerCol]

		# only HGNC source goes to HGNC
		if value == 'HGNC':
			source[markerKey] = HGNC
		else:
			# anything else goes to HomoloGene
			source[markerKey] = HOMOLOGENE

		markerToCluster[markerKey] = row[clusterCol]

	logger.debug('Got source types for %d markers' % len(source))

	# sql 33 : associate the original clusters with the respective markers

	(cols, rows) = results33

	markerCol = Gatherer.columnNumber(cols, '_Marker_key')
	clusterCol = Gatherer.columnNumber(cols, '_Cluster_key')
	sourceCol = Gatherer.columnNumber(cols, '_ClusterSource_key')
	sourceTextCol = Gatherer.columnNumber(cols, 'source')
	idCol = Gatherer.columnNumber(cols, 'accID')

	for row in rows:
		markerKey = row[markerCol]
		sourceText = row[sourceTextCol]
		accID = row[idCol]
		clusterKey = row[clusterCol]

		# If this marker was in a hybrid cluster, then we have its
		# desired source cluster noted.  Match them up.

		if source.has_key(markerKey):
			if row[sourceCol] == source[markerKey]:
				CLUSTER_MAP[markerKey] = ( clusterKey,
					sourceText, accID )

				# if this marker has a hybrid cluster key, we
				# need to map that hybrid cluster key to its
				# source cluster key

				if markerToCluster.has_key(markerKey):
					HYBRID_TO_SOURCE[markerToCluster[markerKey]] = ( clusterKey, sourceText, accID )

	logger.debug('Mapped %d hybrid clusters to source clusters' % \
		len(HYBRID_TO_SOURCE))
	logger.debug('Mapped %d markers to clusters' % len(CLUSTER_MAP))
	return

def getMarkersPerCluster():
	# get a dictionary mapping from cluster key to marker key

	byCluster = {}
	for markerKey in CLUSTER_MAP.keys():
		clusterKey = CLUSTER_MAP[markerKey]

		if byCluster.has_key(clusterKey):
			if markerKey not in byCluster[clusterKey]:
				byCluster[clusterKey].append(markerKey)
		else:
			byCluster[clusterKey] = [ markerKey ]

	logger.debug('Collected %d clusters with %d markers' % (
		len(byCluster), len(CLUSTER_MAP)) )
	return byCluster 

def getClusterKey(markerKey = None, clusterKey = None):
	# get the cluster key chosen by the hybrid rules for the given marker
	# key or hybrid cluster key

	if markerKey:
		if CLUSTER_MAP.has_key(markerKey):
			return CLUSTER_MAP[markerKey][0]
	elif clusterKey:
		if HYBRID_TO_SOURCE.has_key(clusterKey):
			return HYBRID_TO_SOURCE[clusterKey][0]
	return None

def getClusterSource(markerKey = None, clusterKey = None):
	# get the name of the source for the cluster chosen by the hybrid
	# rules for the given marker key or hybrid cluster key

	if markerKey:
		if CLUSTER_MAP.has_key(markerKey):
			return CLUSTER_MAP[markerKey][1]
	elif clusterKey:
		if HYBRID_TO_SOURCE.has_key(clusterKey):
			return HYBRID_TO_SOURCE[clusterKey][1]
	return None

def getClusterID(markerKey = None, clusterKey = None):
	# get the accession ID for the cluster chosen by the hybrid rules for
	# the given marker key or hybrid cluster key

	if markerKey:
		if CLUSTER_MAP.has_key(markerKey):
			return CLUSTER_MAP[markerKey][2]
	elif clusterKey:
		if HYBRID_TO_SOURCE.has_key(clusterKey):
			return HYBRID_TO_SOURCE[clusterKey][2]
	return None

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation data,
	#	collates results, writes tab-delimited text file

	# post-process all the hdp_annotation results and add the term_seq and term_depth columns
	def calculateTermSeqs(self,annotResults):
		# sql (30) : term sequencenum values for MP
		logger.info("preparing to calculate term sequences for hdp_annotation results")
		# get term sequencenum from voc_term
		origTermSeqDict = {}
		(cols, rows) = self.results[30]
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


	def processAnnotations(self, annotResults, diseaseTermRefResults):
		#
		# hdp_annotation
		# hdp_term_to_reference
		#
		# sql (8)
		# former: disease-references by marker
		#
                # sql (9)
                # disease-term-referencees
		#
		# sql (10)
		# MP and OMIM annotations rolled-up to markers
		#
		# sql (11)
		# references for allele/OMIM annotations
		#
		# sql (12)
		# allele/OMIM annotations
		#
		# sql (13)
		# human gene/OMIM annotations
		#

                # sql (9)
                # disease-term-referencees
                diseaseTermRefDict = {}
                (cols, rows) = self.results[9]
                key = Gatherer.columnNumber (cols, '_Term_key')
                for row in rows:
			diseaseTermRefDict.setdefault(row[key],[]).append(row)
		#logger.debug (diseaseTermRefDict[847181])

		# sql (10)
		# MP and OMIM annotations rolled-up to markers

		logger.debug ('start : processing rolled-up annotations')
		(cols, rows) = self.results[10]

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
			key = (genotypeKey, markerKey)

                        # then store the *unique* term/reference association
                        if annotType in [1005] and diseaseTermRefDict.has_key(termKey):
                                for m in diseaseTermRefDict[termKey]:
                                	if [termKey, m[1]] not in diseaseTermRefResults:
                                        	diseaseTermRefResults.append ( [ termKey, m[1], ])

		logger.debug ('end : processing rolled-up annotations')

		# sql (12)
		# allele/OMIM annotations
		# contains OMIM annotations only
		(cols, rows) = self.results[12]
		logger.debug('skipping %d allele/OMIM annotations' % len(rows))

		# sql (13)
		# human gene/OMIM annotations
		# contains OMIM annotations only
		logger.debug ('start : processed human OMIM annotations')
		(cols, rows) = self.results[13]

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

		return annotResults, diseaseTermRefResults

	def processGridCluster(self, clusterResults, cmarkerResults):

		#
		# hdp_gridcluster
		# hdp_gridcluster_marker
		#
		# sql (7)
		# marker -> header term/accession id (MP and Disease/OMIM)
		#
		# sql (19)
		# homologene clusters
		#
		# sql (20)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		#
                # sql (21) : annotations that contain homologene clusters
		#
                # sql (22) : annotations that do-not contain homologene clusters
		#

		logger.debug ('start : processed mouse/human genes with homolgene clusters')

		#
		# sql (7)
		# marker -> header term/accession id
		markerHeaderDict = {}
		(cols, rows) = self.results[7]
		key = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			markerHeaderDict.setdefault(row[key],[]).append(row)

		# sql (19)
		# homologene clusters
		clusterDict1 = {}
		(cols, rows) = self.results[19]
		keyCol = Gatherer.columnNumber (cols, '_Cluster_key')
		for row in rows:
			srcClusterKey = getClusterKey(clusterKey = row[keyCol])
			clusterDict1.setdefault(srcClusterKey, []).append(row)

		#logger.debug (clusterDict1)

		# sql (20)
		# non-homologene clusters
		# use the marker key as the "cluster" key
		clusterDict2 = {}
		(cols, rows) = self.results[20]
		markerKey = Gatherer.columnNumber (cols, '_Marker_key')
		for row in rows:
			clusterDict2.setdefault(row[markerKey],[]).append(row)
		#logger.debug (clusterDict2)

		# sql (21) : annotations that contain homologene clusters
		logger.debug ('start : processed mouse/human genes with homolgene clusters')
		(cols, rows) = self.results[21]

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

		# track highest cluster key so far
		maxClusterKey = 0

		self.markerClusterKeyDict = {}
		for row in rows:

			clusterKey = row[clusterKeyCol]
#			homologeneID = row[homologeneIDCol]
			markerKey = row[markerKeyCol]

			srcClusterKey = getClusterKey(clusterKey = clusterKey)
			homologeneID = getClusterID(clusterKey = clusterKey)
			source = getClusterSource(clusterKey = clusterKey)

			maxClusterKey = max(maxClusterKey, srcClusterKey)

			#
			# clusterResults
			#
			if srcClusterKey not in clusterList:
				clusterResults.append( [ 
                                     	srcClusterKey,
					homologeneID,
					source,
					])
				clusterList.add(srcClusterKey)

			#
			# cmarkerResults
			#
			if markerKey not in markerList:
				self.markerClusterKeyDict[markerKey] = srcClusterKey
				cmarkerResults.append ( [ 
			    		srcClusterKey,
			    		markerKey,
			    		row[organismKeyCol],
			    		row[symbolCol],
					])
				markerList.add(markerKey)

		logger.debug ('end : processed mouse/human genes with homolgene clusters')

		# add other markers to their respective clusters, if those
		# markers didn't have their own annotations

		byCluster = getMarkersPerCluster()
		for clusterKey in byCluster.keys():

			# assume no markers in the cluster had annotations
			clusterHadData = False

			for markerKey in byCluster[clusterKey]:
				if markerKey in markerList:
					clusterHadData = True
					break

			# if any markers had data, then we need to toss any
			# missing markers into the gridcluster

			if clusterHadData:
				for markerKey in byCluster[clusterKey]:
					if markerKey not in markerList:
						self.markerClusterKeyDict[markerKey] = clusterKey
						cmarkerResults.append ( [
							clusterKey,
							markerKey,
							getOrganism(markerKey),
							getSymbol(markerKey),
							] )
						markerList.add(markerKey) 

		# sql (22) : mouse/human markers with annotations that do NOT contain homologene clusters
		logger.debug ('start : processed mouse/human genes without homolgene clusters')
		(cols, rows) = self.results[22]

		# set of columns for common sql fields
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		symbolCol = Gatherer.columnNumber (cols, 'symbol')

                # at most one marker/term in a gridCluster_annotation
                cannotList = set([])

		clusterKey = maxClusterKey

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
				None,
				])

			#
			# cmarkerResults
			#
			if markerKey not in markerList:
				self.markerClusterKeyDict[markerKey] = clusterKey
				cmarkerResults.append ( [ 
                               		clusterKey,
					markerKey,
			     		row[organismKeyCol],
			     		row[symbolCol],
				])
				markerList.add(markerKey)

		logger.debug ('end : processed mouse/human genes without homolgene clusters')

		return clusterResults, cmarkerResults

	def processGenotypeCluster(self, gClusterResults, gResults, gannotResults):

		#
		# hdp_genocluster
		# hpd_genocluster_genotype
		# hpd_genocluster_annotation
		#
		# sql (27) : genotype-cluster : by annotation
                # sql (28) : genotype-cluster : counts
		# sql (29) : genotype-cluster : genotype/marker
		# sql (31) : genotype-cluster : allele pairs
		#

		logger.debug ('start : processed genotype cluster')

		# sql (27) : genotype-cluster : by annotation
		clusterAnnotDict = {}
		(cols, rows) = self.results[27]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		for row in rows:
			clusterAnnotDict.setdefault(row[genotypeKeyCol],[]).append(row)

                # sql (28) : genotype-cluster : counts
                logger.debug ('start : processed genotype cluster counts')
                genoTermRefDict = {}
                (cols, rows) = self.results[28]
                key1 = Gatherer.columnNumber (cols, '_Genotype_key')
                key2 = Gatherer.columnNumber (cols, '_Term_key')
                key3 =  Gatherer.columnNumber (cols, '_Qualifier_key')
                key4 =  Gatherer.columnNumber (cols, 'refCount')
                for row in rows:
			genoTermRefDict.setdefault((row[key1], row[key2], row[key3]),[]).append(row[key4])

		# sql (29) : genotype-cluster : genotype/marker
		genoMarkerList = set([])
		(cols, rows) = self.results[29]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')

		genotypeToMarkers = {}

		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]

			genoMarkerList.add((genotypeKey, markerKey))

			if not genotypeToMarkers.has_key(genotypeKey):
				genotypeToMarkers[genotypeKey] = [ markerKey ]
			elif markerKey not in genotypeToMarkers[genotypeKey]:
				genotypeToMarkers[genotypeKey].append(markerKey) 
		# sql (31) : genotype-cluster : allele pairs
		(cols, rows) = self.results[31]
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

				# if we have both qualifiers, use null-qualifier, but add both geno counts
				# or
				# if this is the only qualifier (whether normal or null) use it

				otherQualifierKey = qualifier == 'normal' and \
					(annotationKey, None, header) or (annotationKey, 'normal', header)

				if otherQualifierKey in gannotHeader:
					otherQualifierGenoCount = gannotHeader[otherQualifierKey]
					gannotResults.append([clusterKey, None, annotationKey,
						None, 'header', None, header, 0, geno_count + otherQualifierGenoCount])
				else:
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
							and ap2[0] != None \
							and ([clusterKey, ap2[0]]) not in gClusterResults:
							gClusterResults.append([clusterKey, ap2[0]])

				# need to pick up any additional markers that
				# this genotype was rolled-up to (like EC
				# markers)

				if genotypeToMarkers.has_key(gKey):
					for mKey in genotypeToMarkers[gKey]:
						t = [ clusterKey, mKey ]
						if t not in gClusterResults:
						    gClusterResults.append(t)


		logger.debug ('end : processed genotype cluster')

		return gClusterResults, gResults, gannotResults

        def collateResults(self):

		# get the mapping from each marker to the source cluster that
		# was chosen for it by the hybrid selection rules
		initializeClusterMap(self.results[32], self.results[33])

		# initialize the cache of marker symbols and organisms
		initializeMarkerCache(self.results[34])

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

                diseaseTermRefResults = []
                diseaseTermRefCols = ['_Term_key',
                                '_Refs_key',
                        ]

                logger.debug ('start : hdp_annotation')
                annotResults, diseaseTermRefResults = \
                        self.processAnnotations(annotResults, diseaseTermRefResults)
                logger.debug ('end : processed hdp_annotation')

		#
		# hdp_gridcluster
		# hdp_gridcluster_marker
		#
		clusterResults = []
		clusterCols = ['_Cluster_key',
				'homologene_id',
				'source'
			]

		cmarkerResults = []
		cmarkerCols = ['_Cluster_key',
				'_Marker_key', 
				'_Organism_key', 
				'symbol',
			]

                logger.debug ('start : hdp_gridcluster')
                clusterResults, cmarkerResults = \
                        self.processGridCluster(clusterResults, cmarkerResults)
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
                self.output.append((diseaseTermRefCols, diseaseTermRefResults))

		self.output.append((clusterCols, clusterResults))
		self.output.append((cmarkerCols, cmarkerResults))

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

	# sql (2-6)
	# In order to trace a rolled-up annotation back to its original
	# genotype, we'll need to use the source annotation key stored as a
	# property for each rolled-up annotation.  These are varchars and need
	# to be pulled into a temp table as integers and indexed.

	# 2. build a temp table mapping from each rolled-up annotation to its
	# source annotation

	'''select va._AnnotType_key, 
		va._Object_key as _Marker_key,
		va._Annot_key as _DerivedAnnot_key,
		vep.value::int as _SourceAnnot_key
	into temporary table source_annotations
	from VOC_Annot va,
		VOC_Evidence ve,
		VOC_Evidence_Property vep,
		VOC_Term t
	where va._AnnotType_key in (%s, %s)
		and va._Annot_key = ve._Annot_key
		and ve._AnnotEvidence_key = vep._AnnotEvidence_key
		and vep._PropertyTerm_key = t._Term_key
		and va._Qualifier_key != %s
		and t.term = '_SourceAnnot_key'
	''' % (MP_MARKER, OMIM_MARKER, NOT_QUALIFIER),

	# 3. index rolled-up annotation key
	'create index tmp_sa_1 on source_annotations (_DerivedAnnot_key)',

	# 4. index source annotation key
	'create index tmp_sa_2 on source_annotations (_SourceAnnot_key)',

	# 5. index marker key for rolled-up annotation
	'create index tmp_sa_3 on source_annotations (_Marker_key)',

	# 6. index annotation type key
	'create index tmp_sa_4 on source_annotations (_AnnotType_key)',

	# sql (7)
	# get the mapping from marker key to header terms (MP + OMIM).
	# Note that OMIM doesn't actually have header terms; they're just a
	# special type of synonym.
	#
	'''select distinct s._Marker_key,
		a._AnnotType_key,
		h._Term_key,
		ms.synonym,
		aa.accID
	from source_annotations s,
		VOC_Annot a,
		VOC_AnnotHeader h,
		MGI_Synonym ms,
		ACC_Accession aa
	where s._AnnotType_key = %s
		and s._SourceAnnot_key = a._Annot_key
		and a._Object_key = h._Object_key
		and a._AnnotType_key = h._AnnotType_key
		and h._Term_key = ms._Object_key
		and ms._MGIType_key = %s
		and ms._SynonymType_key = %s
		and h._Term_key = aa._Object_key
		and aa._MGIType_key = %s
		and aa.preferred = 1
	union
	select distinct s._Marker_key,
		a._AnnotType_key,
		a._Term_key,
		ms.synonym,
		aa.accID
	from source_annotations s,
		VOC_Annot a,
		MGI_Synonym ms,
		ACC_Accession aa
	where s._AnnotType_key = %s
		and s._SourceAnnot_key = a._Annot_key
		and a._Term_key = ms._Object_key
		and ms._MGIType_key = %s
		and ms._SynonymType_key = %s
		and a._Term_key = aa._Object_key
		and aa._MGIType_key = %s
		and aa.preferred = 1 
		and a._Qualifier_key != %s
	''' % (MP_MARKER, VOCAB, MP_HEADER_SYNONYM_TYPE, VOCAB,
		OMIM_MARKER, VOCAB, DISEASE_SYNONYM_TYPE, VOCAB, NOT_QUALIFIER),

	# sql (8)
	# get the distinct mouse marker/genotype pairs for each reference 
	# for OMIM/Genotype annotations
	#
	'select 1 as place_holder',

	# sql (9)
	# get the distinct set of references for each term used for OMIM
	# annotations to mouse (genotypes or directly to alleles)
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

	# sql (10)
	# pre-computed annotations...
	#
	'''select distinct m._Marker_key,
		m._Organism_key,
		src._AnnotType_key,
		src._Term_key,
		src._Object_key as _Genotype_key,
		a.accID,
		t.term,
		v.name,
		q.term as qualifier_type,
		src._Qualifier_key,
		null as genotype_type
	from VOC_Annot va,
		MRK_Marker m,
		source_annotations s,
		VOC_Annot src,
		VOC_Term t,
		ACC_Accession a,
		VOC_Term q,
		VOC_Vocab v
	where va._AnnotType_key in (%d, %d)
		and va._Object_key = m._Marker_key
		and va._Annot_key = s._DerivedAnnot_key
		and s._SourceAnnot_key = src._Annot_key
		and src._Term_key = t._Term_key
		and src._Term_key = a._Object_key
		and a._MGIType_key = %d
		and a.private = 0
		and a.preferred = 1
		and src._Qualifier_key = q._Term_key
		and src._Qualifier_key != %s
		and t._Vocab_key = v._Vocab_key
	''' % (OMIM_MARKER, MP_MARKER, VOCAB, NOT_QUALIFIER),

	# sql (11)
	# disease-references
	# to store distinct marker/reference used for allele/OMIM annotations
	#
	'''
        select distinct al._Marker_key, e._Refs_key
        from VOC_Annot v, VOC_Evidence e, ALL_Allele al
	where al._Allele_key = v._Object_key
	and v._AnnotType_key = %d
        and v._Annot_key = e._Annot_key
	''' % OMIM_ALLELE,

        # sql (12)
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
        where v._AnnotType_key = %d
        and v._Term_key = t._Term_key
        and v._Term_key = a._Object_key
        and a._MGIType_key = %d
        and a.private = 0
        and a.preferred = 1
        and t._Vocab_key = vv._Vocab_key
        and v._Object_key = al._Allele_key
	and al._Marker_key = m._Marker_key
	and m._Marker_key != %d
	and v._Qualifier_key != %d
        ''' % (OMIM_ALLELE, VOCAB, GT_ROSA, NOT_QUALIFIER),

	# sql (13)
	# human data (both genes and phenotypic markers)
	#
	'''select distinct v._Object_key as _Marker_key,
                m._Organism_key,
                v._AnnotType_key,
                v._Term_key,
                a.accID,
                t.term,
                vv.name,
                t2.term as qualifier_type
        from VOC_Annot v,
		VOC_Term t,
		VOC_Term t2,
		VOC_Vocab vv,
		ACC_Accession a,
		MRK_Marker m
        where v._AnnotType_key in (%d, %d)
		and v._Term_key = t._Term_key
		and v._Qualifier_key = t2._Term_key
		and v._Term_key = a._Object_key
		and a._MGIType_key = %d
		and a.private = 0
		and a.preferred = 1
		and t._Vocab_key = vv._Vocab_key
		and v._Object_key = m._Marker_key''' % (OMIM_HUMAN_MARKER,
			OMIM_HUMAN_PHENO_MARKER, VOCAB),

        #
        # hdp_gridcluster
	# hdp_gridcluster_marker
        #

        # sql (14)
	# super-simple + simple genotypes
	# that contain homolgene clusters
	# plus
        # human with OMIM annotations where mouse contain homologene clusters
	#
	'''
	select distinct c._Cluster_key, 
		s._Object_key as _Marker_key,
		s._AnnotType_key, s._Term_key, t.term, a.accID
	into temporary table tmp_cluster
	from VOC_Annot s, MRK_ClusterMember c, MRK_Cluster mc, VOC_Term t,
		ACC_Accession a
	where s._Object_key = c._Marker_key
	and s._AnnotType_key in (%d, %d)
	and c._Cluster_key = mc._Cluster_key
	and mc._ClusterSource_key = %d
	and mc._ClusterType_key = %d
	and s._Term_key = t._Term_key
	and s._Term_key = a._Object_key
	and a._MGIType_key = %d
	and a.preferred = 1
	and s._Qualifier_key != %d

	union

	select distinct c._Cluster_key, c._Marker_key,
		v._AnnotType_key, v._Term_key, t.term, a.accID
        from MRK_ClusterMember c, VOC_Annot v, MRK_Cluster mc, VOC_Term t,
		ACC_Accession a
        where c._Marker_key = v._Object_key
	and v._AnnotType_key in (%d, %d)
	and c._Cluster_key = mc._Cluster_key
	and mc._ClusterSource_key = %d
	and mc._ClusterType_key = %d
	and v._Term_key = t._Term_key
	and v._Term_key = a._Object_key
	and a._MGIType_key = %d
	and v._Qualifier_key != %d
	and a.preferred = 1
	''' % (MP_MARKER, OMIM_MARKER, HYBRID, HOMOLOGY, VOCAB,
		NOT_QUALIFIER, 
		OMIM_HUMAN_MARKER, OMIM_HUMAN_PHENO_MARKER, HYBRID,
		HOMOLOGY, VOCAB, NOT_QUALIFIER),

	# sql (15)
	'''
	create index idx_cluster on tmp_cluster (_Cluster_key)
	''',
	# sql (16)
	'''
	create index idx_cluster_marker on tmp_cluster (_Marker_key)
	''',

        # sql (17) : 
	#
	# mouse markers that do NOT contain homolgene clusters
	# plus
        # human with OMIM annotations where mouse does NOT contain homologene
	# clusters
	#
	'''
	select distinct s._Object_key as _Marker_key,
		s._AnnotType_key, s._Term_key, t.term, a.accID
	into temporary table tmp_nocluster
	from VOC_Annot s, VOC_Term t, ACC_Accession a
	where not exists (select 1 from tmp_cluster tc where s._Object_key = tc._Marker_key)
		and s._AnnotType_key in (%d, %d)
		and s._Qualifier_key != %d
		and s._Term_key = t._Term_key
		and s._Term_key = a._Object_key
		and a._MGIType_key = %d
		and a.preferred = 1

	union

	select distinct c._Marker_key, v._AnnotType_key, v._Term_key, t.term,
		a.accID
        from MRK_Marker c, VOC_Annot v, VOC_Term t, ACC_Accession a
        where c._Marker_key = v._Object_key
		and v._AnnotType_key in (%d, %d)
		and v._Term_key = t._Term_key
		and v._Term_key = a._Object_key
		and a._MGIType_key = %d
		and v._Qualifier_key != %d
		and a.preferred = 1
		and not exists (select 1 from tmp_cluster tc
			where c._Marker_key = tc._Marker_key)''' % (
		MP_MARKER, OMIM_MARKER, NOT_QUALIFIER, VOCAB,
		OMIM_HUMAN_MARKER, OMIM_HUMAN_PHENO_MARKER, VOCAB,
		NOT_QUALIFIER),

	# sql (18)
	'''
	create index idx_nocluster_marker on tmp_nocluster (_Marker_key)
	''',

	# sql (19)
	'''
	select * from tmp_cluster
	''',

	# sql (20)
	'''
	select * from tmp_nocluster
	''',

	# sql (21)
	# additional info for tmp_cluster-ed data
        '''
	select distinct c._Cluster_key,
		c._Marker_key,
		m._Organism_key,
		m.symbol,
		a.accID as homologene_id
	from MRK_Cluster mc
	inner join MRK_ClusterMember c on (c._Cluster_key = mc._Cluster_key)
	inner join MRK_Marker m on (c._Marker_key = m._Marker_key
		and m._Organism_key in (1,2))
	left outer join ACC_Accession a on (mc._Cluster_key = a._Object_key
		and a._LogicalDB_key = 81)
	where mc._ClusterSource_key = %d
		and mc._ClusterType_key = %d
		and exists (select 1 from tmp_cluster tc 
			where tc._Cluster_key = mc._Cluster_key)
	order by c._Cluster_key
	''' % (HYBRID, HOMOLOGY),

	# sql (22)
	# additional info for tmp_nocluster-ed data
        '''
	select distinct c._Marker_key, c._Organism_key, c.symbol
	from MRK_Marker c
	where c._Organism_key in (1,2)
	and exists (select 1 from tmp_nocluster tc where tc._Marker_key = c._Marker_key)
	order by c._Marker_key
	''',

	# sql (23-26)
	# identify which genotypes have background-sensitivity notes and which
	# ones don't, storing info in temp table

	# sql (23)
	# identify the set of unique genotypes with MP/OMIM annotations and
	# assume that none have background-sensitivity notes
	#
	'''select distinct a._Object_key as _Genotype_key,
		a._Annot_key,
		0 as note_exists
	into temporary table has_background_note
	from VOC_Annot a
	where a._AnnotType_key in (1002, 1005)''',

	# sql (24)
	# index the new temp table by genotype
	#
	'create index g_index on has_background_note (_Genotype_key)',

	# sql (25)
	# index the new temp table by genotype
	#
	'create index a_index on has_background_note (_Annot_key)',

	# sql (26)
	# flag those genotypes which do have background sensitivity notes
	#
	'''update has_background_note
	set note_exists = 1
	where _Annot_key in (select a._Annot_key
		from VOC_Annot a,
			VOC_Evidence e,
			MGI_Note n
		where a._AnnotType_key in (1002, 1005)
			and a._Annot_key = e._Annot_key
			and e._AnnotEvidence_key = n._Object_key
			and n._MGIType_key = 25
			and n._NoteType_key = 1015)''',

        # sql (27)
        # super-simple + simple
        # mouse annotations by genotype
        # *make sure the qualifier is order in descending order*
        # as this affects the setting of the mp-header
        # pull in the 'has_backgroundnote' for each genotype/annotation
	'''select distinct a._Object_key as _Genotype_key,
		a._AnnotType_key,
		a._Term_key,
		a._Qualifier_key,
		t.term,
		aa.accID,
		q.term as qualifier_type,
		n.note_exists as has_backgroundnote
	from VOC_Annot a,
		VOC_Term t,
		ACC_Accession aa,
		VOC_Term q,
		has_background_note n
	where a._AnnotType_key in (%d, %d)
		and a._Qualifier_key = q._Term_key
		and a._Term_key = t._Term_key
		and a._Term_key = aa._Object_key
		and aa._MGIType_key = %d
		and aa.preferred = 1
		and a._Annot_key = n._Annot_key
		and a._Object_key = n._Genotype_key
		and a._Qualifier_key != %d
	order by a._Object_key, a._Term_key, a._Qualifier_key, q.term desc
	''' % (MP_GENOTYPE, OMIM_GENOTYPE, VOCAB, NOT_QUALIFIER),

        # sql (28)
        # counts by geno-cluster/term/qualifier/reference
        '''
        select distinct v._Object_key as _Genotype_key, v._Term_key, v._Qualifier_key, count(_Refs_key) as refCount
        from VOC_Annot v, VOC_Evidence e
        where (v._AnnotType_key in (1002)
		or
	     (v._AnnotType_key = 1005 and v._Qualifier_key != 1614157))
        	and v._Annot_key = e._Annot_key
	        and v._Term_key not in (293594)
		and v._Qualifier_key != %d
        group by v._Object_key, v._Term_key, v._Qualifier_key
        ''' % NOT_QUALIFIER,

	# sql (29)
	# distinct genotype/marker - should this be all genotype/marker pairs
	# or only those with rollup?
	'''
	select distinct g._Object_key as _Genotype_key,
		a._Object_key as _Marker_key
	from VOC_Annot a,
		source_annotations s,
		VOC_Annot g
	where a._Annot_key = s._DerivedAnnot_key
		and a._Qualifier_key != %d
		and s._SourceAnnot_key = g._Annot_key''' % NOT_QUALIFIER,

	# sql (30)
	# term sequencenum values for MP
	'''
	select _Term_key, sequenceNum from VOC_Term where _Vocab_key = 5
	''',

	# sql (31)
	# allele pair information in order to generate the genotype-cluster
	# include: super-simple + simple (tmp_annot_mouse)
	# exclude: markers where there exists a double-wild-type allele pair
	'''
        select distinct p._Genotype_key, p._Marker_key,
               p._Allele_key_1, p._Allele_key_2, p._PairState_key,
               g.isConditional, g._ExistsAs_key
        from GXD_Genotype g, GXD_AllelePair p, VOC_Annot c
        where g._Genotype_key = p._Genotype_key
		and g._Genotype_key = c._Object_key
		and c._AnnotType_key in (%d, %d)
		and c._Qualifier_key != %d
	''' % (MP_GENOTYPE, OMIM_GENOTYPE, NOT_QUALIFIER),

	# sql (32)
	# get the hybrid homology data for each marker, specifying which of
	# the homology sources (HomoloGene or HGNC) is the correct one for the
	# derived cluster
	'''
	select m._Organism_key, m._Marker_key, p.value, mc._Cluster_key
	from MRK_ClusterMember mcm,
		MRK_Cluster mc,
		MRK_Marker m,
		MGI_Property p,
		VOC_Term t
	where m._Organism_key in (1,2)
		and m._Marker_key = mcm._Marker_key
		and mcm._Cluster_key = mc._Cluster_key
		and mc._ClusterSource_key = %d
		and mc._ClusterType_key = %d
		and mc._Cluster_key = p._Object_key
		and p._MGIType_key = %d
		and p._PropertyTerm_key = t._Term_key
		and t.term = 'secondary source' ''' % (HYBRID, HOMOLOGY,
			CLUSTER),

	# sql (33)
	# get the basic homology cluster choices (HGNC and HomoloGene) for
	# each mouse marker.  The results of sql 32 will tell us which one of
	# these choices to keep for each marker.
	'''
	select m._Marker_key, mc._Cluster_key, mc._ClusterSource_key,
		s.term as source, a.accID
	from MRK_Cluster mc
	inner join MRK_ClusterMember mcm on (mcm._Cluster_key = mc._Cluster_key)
	inner join MRK_Marker m on (m._Marker_key = mcm._Marker_key
		and m._Organism_key in (1,2) )
	inner join VOC_Term s on (mc._ClusterSource_key = s._Term_key)
	left outer join ACC_Accession a on (
		a._MGIType_key = %d
		and a._Object_key = mc._Cluster_key
		and a.preferred = 1)
	where mc._ClusterSource_key in (%d, %d)
		and mc._ClusterType_key = %d
	order by m._Marker_key''' % (CLUSTER, HOMOLOGENE, HGNC, HOMOLOGY), 

	# sql (34)
	# get the marker symbols and organisms for all mouse and human markers
	# (except withdrawns)
	'''
	select m._Marker_key, m.symbol, o.commonName
	from MRK_Marker m, MGI_Organism o
	where m._Organism_key = o._Organism_key
		and m._Marker_Status_key in (1,3)
		and m._Organism_key in (1,2)
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

        ('hdp_term_to_reference',
                [ Gatherer.AUTO, '_Term_key', '_Refs_key', ],
          'hdp_term_to_reference'),

	('hdp_gridcluster',
		[ '_Cluster_key', 'homologene_id', 'source' ],
          'hdp_gridcluster'),

	('hdp_gridcluster_marker',
		[ Gatherer.AUTO, '_Cluster_key', '_Marker_key',
		  '_Organism_key', 'symbol' ],
          'hdp_gridcluster_marker'),

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
