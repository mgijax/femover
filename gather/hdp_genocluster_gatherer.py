#!/usr/local/bin/python
# 
# gathers data for the HDP (HMDC/Disease Portal) tables in the front-end
# database.
#
# What is a genocluster?  A set of genotypes which shares a common set of
# allele pairs, ignoring of the background strain.

import Gatherer
import logger
import gc
import DiseasePortalUtils
import dbAgnostic

###--- Globals ---###

# name of table which indicates whether genotypes have background sensitivity
# notes or not
BG_TABLE = None	

NO_PHENOTYPIC_ANALYSIS = 293594		# MP vocab term

DPU = DiseasePortalUtils		# module alias for convenience

###--- Function ---###

def getBackgroundSensitivityTableName():
	# build (if needed) and return the name of a temp table with a
	# pre-computed flag for whether a genotype has a background sensitivity
	# note or not.

	global BG_TABLE

	if BG_TABLE:
		return BG_TABLE

	BG_TABLE = 'has_background_note'

	# identify the set of unique genotypes with MP/OMIM annotations and
	# assume that none have background-sensitivity notes

	cmd1 = '''select distinct a._Object_key as _Genotype_key,
			a._Annot_key, 0 as note_exists
		into temporary table %s
		from VOC_Annot a
		where a._AnnotType_key in (%d, %d)''' % (BG_TABLE,
			DPU.MP_GENOTYPE,
			DPU.OMIM_GENOTYPE)

	cmd2 = 'create index %s on %s (_Genotype_key)' % (
		DPU.nextIndex(), BG_TABLE)

	cmd3 = 'create index %s on %s (_Annot_key)' % (
		DPU.nextIndex(), BG_TABLE)

	# flag those genotypes which do have background sensitivity notes

	cmd4 = '''update %s
		set note_exists = 1
		where _Annot_key in (select a._Annot_key
			from VOC_Annot a,
				VOC_Evidence e,
				MGI_Note n
			where a._AnnotType_key in (%d, %d)
				and a._Annot_key = e._Annot_key
				and e._AnnotEvidence_key = n._Object_key
				and n._MGIType_key = 25
				and n._NoteType_key = 1015)''' % (BG_TABLE,
					DPU.MP_GENOTYPE,
					DPU.OMIM_GENOTYPE)

	dbAgnostic.execute(cmd1)
	logger.debug('Built background sensitivity table')

	dbAgnostic.execute(cmd2)
	dbAgnostic.execute(cmd3)
	logger.debug('Indexed background sensitivity table')

	dbAgnostic.execute(cmd4)
	logger.debug('Updated background sensitivity table') 
	return BG_TABLE

###--- Classes ---###

class GenoclusterGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the HDP (HMDC/Disease Portal) tables
	# Has: queries to execute against the source database
	# Does: queries the source database to retrieve data for genoclusters,
	#	collates results, writes tab-delimited text file

	def getAnnotationsByGenotype(self):
		# returns dictionary mapping from genotype key to a list of
		# annotation rows

		# 0 : group annotations by geontype

		clusterAnnotDict = {}
		(cols, rows) = self.results[0]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		for row in rows:
			clusterAnnotDict.setdefault(row[genotypeKeyCol],
				[]).append(row)
		logger.debug('Grouped %d annotations by %d genotypes' % (
			len(rows), len(clusterAnnotDict)) )

		return clusterAnnotDict

	def getReferenceCounts(self):
		# returns dictionary mapping from each (genotype, term,
		# qualifier) triple to a count of its associated references

                # 1 : get reference counts for (genotype, term, qualifier)
		# triples

                genoTermRefDict = {}
                (cols, rows) = self.results[1]
                key1 = Gatherer.columnNumber (cols, '_Genotype_key')
                key2 = Gatherer.columnNumber (cols, '_Term_key')
                key3 =  Gatherer.columnNumber (cols, '_Qualifier_key')
                key4 =  Gatherer.columnNumber (cols, 'refCount')
                for row in rows:
			triple = (row[key1], row[key2], row[key3])
			genoTermRefDict.setdefault(triple,[]).append(row[key4])

		logger.debug('Got ref counts for %d triples' % \
			len(genoTermRefDict))
		return genoTermRefDict

	def getGenotypeMarkerPairs(self):
		# get a tuple of:
		# (set of (genotype, marker) pairs, 
		#  dictionary mapping genotype key to list of marker keys)

		# 2 : unique genotype/marker pairs for rolled-up annotations

		# set of (genotype, marker) pairs
		genoMarkerList = set([])

		# maps genotype key to list of marker keys
		genotypeToMarkers = {}

		(cols, rows) = self.results[2]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')

		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]

			genoMarkerList.add((genotypeKey, markerKey))

			if not genotypeToMarkers.has_key(genotypeKey):
				genotypeToMarkers[genotypeKey] = [ markerKey ]
			elif markerKey not in genotypeToMarkers[genotypeKey]:
				genotypeToMarkers[genotypeKey].append(markerKey) 
		logger.debug('Got marker/genotype pairs for %d genotypes' % \
			len(genotypeToMarkers))

		return genoMarkerList, genotypeToMarkers

	def getAllelePairData(self):
		# returns a dictionary mapping from a genotype key to a list
		# of rows with its allele pairs

		# 3 : allele pairs for each genotype

		(cols, rows) = self.results[3]
		genotypeKeyCol = Gatherer.columnNumber (cols, '_Genotype_key')
		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		allele1KeyCol = Gatherer.columnNumber (cols, '_Allele_key_1')
		allele2KeyCol = Gatherer.columnNumber (cols, '_Allele_key_2')
		pairStateKeyCol = Gatherer.columnNumber (cols, '_PairState_key')
		isConditionalKeyCol = Gatherer.columnNumber (cols, 'isConditional')
		existsAsKeyCol = Gatherer.columnNumber (cols, '_ExistsAs_key')

		# store the genotype/allele-pair info

		# genotype key : [ (marker, allele 1, allele 2, pair state,
		#	conditional flag, exists as), ... ]
		byID = {}
        	for row in rows:
                	gSet = row[genotypeKeyCol]
                	cSet = (row[markerKeyCol], 
                        	row[allele1KeyCol],
                        	row[allele2KeyCol],
                        	row[pairStateKeyCol],
                        	row[isConditionalKeyCol],
                        	row[existsAsKeyCol])
			byID.setdefault(gSet,[]).append(cSet)

		logger.debug('Got allele pairs for %d genotypes' % len(byID))
		return byID

	def groupGenotypesByAllelePairCounts(self, byID):
		# group the genotypes by the count of their allele pairs,
		# returning:
		# { count : { genotype key : [ (allele pair 1), ... ] } }

        	byPairCount = {}
        	for r in byID:
                	key = len(byID[r])
                	byPairCount.setdefault(key,{}).setdefault(r,[]).append(byID[r])
		logger.debug('Grouped allele pairs by count (max %d)' % \
			max(byPairCount.keys()) )
		return byPairCount

	def clusterGenotypes(self, byPairCount):
		# byPairCount is as built by groupGenotypesByAllelePairCounts.
		# goes through genotypes and clusters them based on the number
		# of allele pairs and the actual allele pairs in each genotype,
		# ignoring the background strain for each genotype.  Returns
		# { new cluster key : { genotype key : [ allele pair 1, ... ]}}

		compressSet = {}
		clusterKey = 1

        	for r in byPairCount:

                	# r is a count of allele pairs within a genotype
                	# diff1 is a dictionary where each genotype key refers
			# to a list of allele pairs

                	diff1 = byPairCount[r]

			# flip the data around and cluster by:
			# allele pair => genotype
                	#	cluster[str(allele pairs)] = [ genotype keys ]
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
                                	compressSet[clusterKey][genotypeKey] \
						= diff1[genotypeKey]
                        	clusterKey = clusterKey + 1

		logger.debug('grouped genotypes into %d clusters' % \
			len(compressSet))
		return compressSet 

	def getClusterAnnotationCounts(self, clusterKey, compressSet,
		clusterAnnotDict, genoTermRefDict):
		# return a dictionary of counts of reference counts for each
		# (term, qualifier) pair in the genotypes of the cluster:
		# { (term, qualifier) : count of refs }

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
		return clusterAnnotCount 

	def trackBackgroundNotes(self, clusterKey, compressSet,
		clusterAnnotDict):
		# go through the genotypes in the current cluster and note
		# which term/qualifier pairs have a background note.  Returns:
		# { (term, qualifier) : 1 or 0 }

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
			return backgroundDict

	def filterAnnotations(self, clusterKey, annotList):
		# go through 'annotList' to keep only the first row with each
		# cluster key / term / qualifier triple.  Returns a filtered
		# version of the list (does not alter the original list).

		sublist = []

		for row in annotList:
			genotypeKey, annotType, termKey, qualifierKey, \
				termName, termId, qualifier, bgFlag = row

			triple = (clusterKey, termKey, qualifier)
			if triple not in self.gannotTermList:
				self.gannotTermList.add(triple)
				sublist.append(row)
		return sublist

	def collateResults(self):
		# go through the results of the database queries and produce
		# the sets of results in self.output

		clusterAnnotDict = self.getAnnotationsByGenotype()
		genoTermRefDict = self.getReferenceCounts()
		genoMarkerList,genotypeToMarkers = self.getGenotypeMarkerPairs()
		byID = self.getAllelePairData()
		byPairCount = self.groupGenotypesByAllelePairCounts(byID)
		compressSet = self.clusterGenotypes(byPairCount)

		# list of (genocluster key, genotype key) pairs for the 
		# hdp_genocluster_genotype table
		gResults = []

		# list of rows for the hdp_genocluster_annotation table, each:
		# (genocluster key, term key, annot type key, qualifier,
		#  term type, accID, term, background note flag, ref count)
		gannotResults = []

		# walks through each newly-generated cluster
		for clusterKey in compressSet:

			# every 100 clusters, reclaim any available memory
			if clusterKey % 100 == 0:
				gc.collect()

			# tracks triples like (cluster key, term, qualifier)
			# to ensure each is only processed once.
			self.gannotTermList = set([])

			# maps from (annot type, qualifier, header) to a count
			# of references for that triple
                	gannotHeader = {}

                        # for each genotype in the cluster
                        # track the genotype-term-reference-count for this cluster
			clusterAnnotCount = self.getClusterAnnotationCounts(
				clusterKey, compressSet, clusterAnnotDict,
				genoTermRefDict)

			# get the dictionary of background note flags
			backgroundDict = self.trackBackgroundNotes(clusterKey,
				compressSet, clusterAnnotDict)

			# walk through the genotypes in this genocluster
			for gKey in compressSet[clusterKey]:

				# add record for hdp_genocluster_genotype
				gResults.append( [
					clusterKey,
					gKey,
					])
				
				# if no annotations for this genotype, move on
				if not clusterAnnotDict.has_key(gKey):
					continue

				for c in self.filterAnnotations(clusterKey, clusterAnnotDict[gKey]):

					annotationKey = c[1]
					termKey = c[2]
					qualifierKey = c[3]
					termName = c[4]
					termId = c[5]
					qualifier = c[6]
                                        hasBackgroundNote = backgroundDict[(termKey, qualifierKey)]

                                        # get the cluster-annotation-count
                                        geno_count = 0
					pair = (termKey, qualifierKey)
					if clusterAnnotCount.has_key(pair):
                                                geno_count = clusterAnnotCount[pair]

					# add row for hdp_genocluster_annotation
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

					# roll up the reference counts for the
					# respective header terms...

					# header = mp header term
					if DPU.hasMPHeader(termKey):
						for header in DPU.getMPHeaders(termKey):
							trio = (annotationKey, qualifier, header)
                                			if trio not in gannotHeader:
                                               			gannotHeader[trio] = geno_count
							else:
                                               			gannotHeader[trio] += geno_count

					# header = disease header term
					elif DPU.hasDiseaseHeader(termKey):
                                                for header in DPU.getDiseaseHeaders(termKey):
							trio = (annotationKey, qualifier, header)
                                       			if trio not in gannotHeader:
                                               			gannotHeader[trio] = geno_count
							else:
                                               			gannotHeader[trio] += geno_count

					# header = disease-term
					else:
						trio = (annotationKey, qualifier, termName)
                                       		if trio not in gannotHeader:
                                               		gannotHeader[trio] = geno_count
						else:
                                               		gannotHeader[trio] += geno_count

			# within each cluster
			# determine the header/qualifier for each term
			# all normals trumps non-normals (null) qualifier

			for trio in gannotHeader:

				annotationKey, qualifier, header = trio
				geno_count = gannotHeader[trio]

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

		logger.debug('Found %d genocluster/term annotations' \
			% len(gannotResults))
		logger.debug('Found %d genotype/genocluster pairs' % \
			len(gResults))

		# ready to push the compressedSet into the gClusterResults

		# list of rows for hdp_genocluster table, each with:
		# (genocluster key, marker key)
		gClusterResults = []

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

		logger.debug('Found %d marker/genocluster pairs' % \
			len(gClusterResults))

		gClusterCols = [ 'hdp_genocluster_key', '_Marker_key' ]
		self.output.append ( (gClusterCols, gClusterResults) )

		gCols = [ 'hdp_genocluster_key', '_Genotype_key' ]
		self.output.append ( (gCols, gResults) )

		gannotCols = [ 'hdp_genocluster_key', '_Term_key',
		  '_AnnotType_key', 'qualifier_type', 'term_type', 'accID',
		  'term', 'has_backgroundnote', 'genotermref_count' ]
		self.output.append( (gannotCols, gannotResults) )

		return

###--- globals ---###

cmds = [
        # 0. mouse annotations by genotype
        # *make sure the qualifier is order in descending order*
        # as this affects the setting of the mp-header
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
		%s n
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
	''' % (getBackgroundSensitivityTableName(),
		DPU.MP_GENOTYPE,
		DPU.OMIM_GENOTYPE,
		DPU.VOCAB,
		DPU.NOT_QUALIFIER),

        # 1. counts by genotype/term/qualifier/reference
        '''select distinct v._Object_key as _Genotype_key,
		v._Term_key,
		v._Qualifier_key,
		count(_Refs_key) as refCount
	from VOC_Annot v, VOC_Evidence e
	where (v._AnnotType_key = %d
		or
		(v._AnnotType_key = %d and v._Qualifier_key != %d))
		and v._Annot_key = e._Annot_key
		and v._Term_key != %d
		and v._Qualifier_key != %d
	group by v._Object_key, v._Term_key, v._Qualifier_key''' % (
		DPU.MP_GENOTYPE,
		DPU.OMIM_GENOTYPE,
		DPU.NOT_QUALIFIER,
		NO_PHENOTYPIC_ANALYSIS,
		DPU.NOT_QUALIFIER),

	# 2. distinct genotype/marker pairs which have rolled-up annotations
	'''select distinct g._Object_key as _Genotype_key,
		a._Object_key as _Marker_key
	from VOC_Annot a,
		%s s,
		VOC_Annot g
	where a._Annot_key = s._DerivedAnnot_key
		and a._Qualifier_key != %d
		and s._SourceAnnot_key = g._Annot_key''' % (
			DPU.getSourceAnnotationsTable(),
			DPU.NOT_QUALIFIER),

	# 3. allele pair information in order to generate the genotype-cluster
	'''select distinct p._Genotype_key, p._Marker_key,
               p._Allele_key_1, p._Allele_key_2, p._PairState_key,
               g.isConditional, g._ExistsAs_key
        from GXD_Genotype g, GXD_AllelePair p, VOC_Annot c
        where g._Genotype_key = p._Genotype_key
		and g._Genotype_key = c._Object_key
		and c._AnnotType_key in (%d, %d)
		and c._Qualifier_key != %d''' % (
			DPU.MP_GENOTYPE, 
			DPU.OMIM_GENOTYPE, 
			DPU.NOT_QUALIFIER),
	]

files = [
	('hdp_genocluster',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Marker_key' ],
          'hdp_genocluster'),

	('hdp_genocluster_genotype',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Genotype_key' ],
          'hdp_genocluster_genotype'),

	('hdp_genocluster_annotation',
		[ Gatherer.AUTO, 'hdp_genocluster_key', '_Term_key',
		  '_AnnotType_key', 'qualifier_type', 'term_type', 'accID',
		  'term', 'has_backgroundnote', 'genotermref_count' ],
          'hdp_genocluster_annotation'),
	]

# global instance of a GenoclusterGatherer
gatherer = GenoclusterGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
