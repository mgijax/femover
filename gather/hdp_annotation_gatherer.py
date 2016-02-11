#!/usr/local/bin/python
# 
# gathers data for the 'hdp_annotation' table in the front-end database

import Gatherer
import logger
import DiseasePortalUtils
import HomologyUtils
import MPSorter
import gc

###--- Globals ---###

DPU = DiseasePortalUtils	# module alias for convenience
HU = HomologyUtils		# module alias for convenience

###--- Classes ---###

class HDPAnnotationGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the hdp_annotation table
	# Has: queries to execute against the source database
	# Does: queries the source database for data to include in the
	#	hdp_anntation table (for HMDC), collates results, writes
	#	tab-delimited text file

	def processRolledUpAnnotations(self):
		# process the mouse MP and disease annotations

		cols, rows = DPU.getAnnotations()

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

		annotResults = []

		for row in rows:
			genotypeKey = row[genotypeKeyCol]
			markerKey = row[markerKeyCol]
			annotType = row[annotTypeKeyCol]
			termKey = row[termKeyCol]
			genotype_type = row[genotypeTypeCol]

			# header = mp header term
			if DPU.hasMPHeader(termKey):
				for header in DPU.getMPHeaders(termKey):
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
			elif DPU.hasDiseaseHeader(termKey):
				for header in DPU.getDiseaseHeaders(termKey):
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

		logger.debug('Processed rolled-up annotations')
		return annotResults

	def processHumanDiseaseAnnotations(self):
		# process the disease annotations for human genes and 
		# phenotypic markers

		annotResults = []

		(cols, rows) = self.results[0]

		markerKeyCol = Gatherer.columnNumber (cols, '_Marker_key')
		organismKeyCol = Gatherer.columnNumber (cols, '_Organism_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termIDCol = Gatherer.columnNumber (cols, 'accID')
		termCol = Gatherer.columnNumber (cols, 'term')
		vocabNameCol = Gatherer.columnNumber (cols, 'name')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier_type')

		for row in rows:
			termKey = row[termKeyCol]

			# header = disease header term
			if DPU.hasDiseaseHeader(termKey):
				for header in DPU.getDiseaseHeaders(termKey):
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

		logger.debug('Added %d human annotations' % len(annotResults))
		return annotResults

	def calculateTermSeqs(self, annotResults):
		# calculate and add sequence numbers to 'annotResults' based
		# on the terms and their headers

		# { MP term key : sequence num }
		mpTermSeqNum = {}

		(cols, rows) = self.results[1]

		for row in rows:
			mpTermSeqNum[row[0]] = row[1]

		logger.debug('Got %d MP seq nums' % len(mpTermSeqNum))
		
		# get the maximum cluster key, so we can add fake clusters as
		# needed (for markers outside homology classes)

		maxClusterKey = HomologyUtils.getMaxClusterKey()

		# colums of an annotResult:
		#	0 : _Marker_key	 	7 : accID	
		#	1 : _Organism_key	8 : term	
		#	2 : _Term_key	 	9 : name	
		#	3 : _AnnotType_key	10 : header	
		#	4 : _Object_key	 	11 : term_seq	
		#	5 : genotype_type	12 : term_depth
		#	6 : qualifier_type	

		# loop through once in order to get the terms for each
		# gridcluster/header group

		# {(gridClusterKey, headerKey) : [ (term key, seq num), ... ]}
		clusterHeaderTermDict = {}

		# { marker key : source homology cluster }
		sourceClusters = {}

		for annotResult in annotResults:
			markerKey = annotResult[0]
			termKey = annotResult[2]
			genotype_type = annotResult[5]
			vocab = annotResult[9]

			# if we don't have a marker key, or if it's not an MP
			# annotation, then just skip it

			if (not markerKey) or \
				 (vocab != 'Mammalian Phenotype'):
					 continue

			# get the hybrid homology cluster(s) for this marker;
			# if none, generate a fake cluster key.

			hybridClusterKeys = HU.getHybridClusterKeys(markerKey)
			if not hybridClusterKeys:
				gridClusterKey = maxClusterKey + markerKey

			else:
				# if more than one hybrid cluster, just use the
				# last and use it to look up info on the source
				# cluster that was chosen for the hybrid

				hybridClusterKey = hybridClusterKeys[-1]

				# only HGNC source goes to HGNC, otherwise
				# HomoloGene trumps anything else

				if HU.getSourceOfCluster(hybridClusterKey) == 'HGNC':
					sourceClusterKeys = \
						HU.getHgncClusterKeys(markerKey)
				else:
					sourceClusterKeys = \
						HU.getHomoloGeneClusterKeys(markerKey)

				# if more than one, just pick the last to use
				# as our gridClusterKey

				gridClusterKey = sourceClusterKeys[-1]

			sourceClusters[markerKey] = gridClusterKey

			header = annotResult[10]

			if DPU.isMPHeader(header):
				headerKey = DPU.getMPHeaderKey(header)
				mapKey = (gridClusterKey,headerKey)
				origTermSeq = mpTermSeqNum[termKey]
				clusterHeaderTermDict.setdefault(mapKey,set([])).add((termKey,origTermSeq))

		logger.debug('walked annotResults to group terms')

		# now go through the groups to calculate sorts

		mpSorter = MPSorter.MPSorter()
		clusterHeaderTermSeqDict = {}
		for mapKey,terms in clusterHeaderTermDict.items():
			gridClusterKey,headerKey = mapKey
			termSeqMap = mpSorter.calculateSortAndDepths(terms,headerKey)
			clusterHeaderTermSeqDict[mapKey] = termSeqMap	

		logger.debug('calculated seq nums for term groups')

		# this can be cleared
		clusterHeaderTermDict = {}
		gc.collect()

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
				and sourceClusters.has_key(markerKey) \
				and vocab == 'Mammalian Phenotype':

				header = annotResult[10]
				if DPU.isMPHeader(header):
					headerKey = DPU.getMPHeaderKey(header)
					gridClusterKey = sourceClusters[markerKey]
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

		logger.info("appended seq num + depth info for annotResults")
		return annotResults

	def collateResults(self):
		results = self.processRolledUpAnnotations()
		results = results + self.processHumanDiseaseAnnotations()
		results = self.calculateTermSeqs(results)

		self.finalColumns = [ '_Marker_key', '_Organism_key',
			'_Term_key', '_AnnotType_key', '_Object_key',
			'genotype_type', 'qualifier_type', 'accID', 'term',
			'name', 'header','term_seq','term_depth' ]
		self.finalResults = results
		logger.debug('Collated %d results rows' % len(results))
		return

###--- globals ---###

cmds = [
	# 0. human data (both genes and phenotypic markers)
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
		and v._Object_key = m._Marker_key''' % (
			DPU.OMIM_HUMAN_MARKER,
			DPU.OMIM_HUMAN_PHENO_MARKER,
			DPU.VOCAB),

	# 1. term sequencenum values for MP
	'select _Term_key, sequenceNum from VOC_Term where _Vocab_key = 5'
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Organism_key', '_Term_key',
	'_AnnotType_key', '_Object_key', 'genotype_type', 'qualifier_type',
	'accID', 'term', 'name', 'header','term_seq','term_depth' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_annotation'

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
