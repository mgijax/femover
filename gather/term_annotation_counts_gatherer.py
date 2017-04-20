#!/usr/local/bin/python
# 
# gathers data for the 'term_annotation_counts' table in the front-end database

import Gatherer
import logger
import gc
import dbAgnostic

###--- Constants ---###

MP_GENOTYPE = 'Mammalian Phenotype/Genotype'
GO_MARKER = 'GO/Marker'
HPO_DO = 'HPO/DO'

###--- Classes ---###

class TermCache:
	# Is: a cache of terms, mapping them to a unique integer key (to save space)
	
	def __init__ (self):
		self.cache = {}		# term -> key
		return
	
	def getKey (self, term):
		if term not in self.cache:
			self.cache[term] = len(self.cache)
		return self.cache[term]
	
class AncestorCache:
	# Is: a cache of ancestor term keys for each term in the given vocabulary
	# Note: This cache is self-referential; that is, a term is viewed as an ancestor of itself.

	def __init__ (self, vocabKey):
		logger.debug(' - building AncestorCache for vocab %d' % vocabKey)
		self.cache = {}				# maps from term key to list of ancestor keys
		self.loadAncestors(vocabKey)
		return
	
	def loadAncestors(self, vocabKey):
		# load the term/ancestor relationships for the given vocab from the database
		cmd = '''select t._Term_key, c._AncestorObject_key
			from voc_term t, dag_closure c
			where t._Vocab_key = %d
				and t._Term_key = c._DescendentObject_key''' % vocabKey
		cols, rows = dbAgnostic.execute (cmd)

		termCol = Gatherer.columnNumber(cols, '_Term_key')
		ancestorCol = Gatherer.columnNumber(cols, '_AncestorObject_key')

		for row in rows:
			termKey = row[termCol]
			if termKey not in self.cache:
				self.cache[termKey] = [ termKey, row[ancestorCol] ]
			elif row[ancestorCol] not in self.cache[termKey]:
				self.cache[termKey].append(row[ancestorCol])

		logger.debug(' -- cached %d ancestors for %d terms' % (len(rows), len(self.cache)))
		return
	
	def getAncestors(self, termKey):
		# retrieve a list of keys for terms that are ancestors of the given term key, or an empty list
		# if there are none

		if termKey in self.cache:
			return self.cache[termKey]
		return [ termKey ]
	
class Collector:
	# Is: a collector of annotations for terms, consolidating them in a manner specific to a given vocabulary
	# Note: This is DAG-aware, so annotations for terms are percolated up to their ancestor terms.
	
	def __init__ (self, ancestorCache):
		logger.debug(' - instantiating Collector')
		self.ancestorCache = ancestorCache
		self.termCache = TermCache()
		self.directObjects = {}			# term key -> set of object keys for just this term
		self.directAnnotations = {} 	# term key -> set of annotations for just this term
		self.dagObjects = {}			# term key -> set of object keys for term and its descendants
		self.dagAnnotations = {}		# term key -> set of annotations for term and its descendants
		self.nodesToSkip = set()		# term keys to skip when accumulating annotations
		self.cacheNodesToSkip()
		return
		
	def cacheNodesToSkip(self):
		return
	
	def getObjectCountDirect (self, termKey):
		if termKey in self.directObjects:
			return len(self.directObjects[termKey])
		return 0
	
	def getObjectCountDag (self, termKey):
		if termKey in self.dagObjects:
			return len(self.dagObjects[termKey])
		return 0
	
	def getAnnotationCountDirect (self, termKey):
		if termKey in self.directAnnotations:
			return len(self.directAnnotations[termKey])
		return 0
	
	def getAnnotationCountDag (self, termKey):
		if termKey in self.dagAnnotations:
			return len(self.dagAnnotations[termKey])
		return 0
	
	def addAnnotation (self, termKey, objectKey, qualifier, evidenceCode):
		annotation = self.getAnnotation(termKey, objectKey, qualifier, evidenceCode)

		if termKey in self.nodesToSkip:
			if termKey not in self.directObjects:
				self.directObjects[termKey] = set()
				self.directAnnotations[termKey] = set()
			return 

		if termKey not in self.directObjects:
			self.directObjects[termKey] = set()
		self.directObjects[termKey].add(objectKey)

		if termKey not in self.directAnnotations:
			self.directAnnotations[termKey] = set()
		self.directAnnotations[termKey].add(annotation)
		
		for ancestorKey in self.ancestorCache.getAncestors(termKey):
			if ancestorKey in self.nodesToSkip:
				if ancestorKey not in self.dagObjects:
					self.dagObjects[ancestorKey] = set()
					self.dagAnnotations[ancestorKey] = set()
				continue
			
			if ancestorKey not in self.dagObjects:
				self.dagObjects[ancestorKey] = set()
			self.dagObjects[ancestorKey].add(objectKey)
			
			# note that we do keep the annotated term key in the annotation, even though we are collecting
			# it at the ancestor key level
			
			if ancestorKey not in self.dagAnnotations:
				self.dagAnnotations[ancestorKey] = set()
			self.dagAnnotations[ancestorKey].add(annotation)
		return
	
	def getAnnotation (self, termKey, objectKey, qualifier, evidenceCode):
		# can override in a subclass to define the components of a distinct annotation differently; in this
		# case, we consider all four components to define a unique annotation.
		
		return (termKey, objectKey, self.termCache.getKey(qualifier), self.termCache.getKey(evidenceCode))
	
class MPGenotypeCollector (Collector):
	# Is: a collector that is specifically for MP/Genotype annotations, which do not consider qualifier or
	#	evidence codes when defining unique annotations
	
	def cacheNodesToSkip(self):
		cmd = '''select _Object_key from acc_accession where _MGIType_key = 13 and accID = 'MP:0000001' '''
		cols, rows = dbAgnostic.execute(cmd)
		
		if rows:
			self.nodesToSkip.add(rows[0][0])
			logger.debug(' - added MP root node to set of terms to skip')
		return
	
	def getAnnotation(self, termKey, objectKey, qualifier, evidenceCode):
		return (termKey, objectKey)

class HpoDoCollector (Collector):
	# Is: a collector that is for HPO/DO annotations (mapping from term to term) without considering qualifier
	#	or evidence code when defining unique annotations
	
	def getAnnotation(self, termKey, objectKey, qualifier, evidenceCode):
		return (termKey, objectKey)

class TACGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the term_annotation_counts table
	# Has: queries to execute against the source database
	# Does: queries the source database for annotation counts for vocabulary terms,
	#	collates results, writes tab-delimited text file

	def getMPGenotypeAnnotations(self, annotTypeKey):
		# gets all the annotations for the given annotation type key as a list of tuples, where each tuple is:
		#	(object key, term key, qualifier, evidence code)
		# Note that for MP/Genotype annotations, we require that there be at least one allele defined.
		
		cmd = '''select va._Object_key, va._Term_key, q.term as qualifier, ev.term as evidenceCode
			from voc_annot va, voc_term q, voc_evidence e, voc_term ev
			where va._AnnotType_key = %d
				and va._Qualifier_key = q._Term_key
				and va._Annot_key = e._Annot_key
				and exists (select 1 from gxd_allelegenotype gag where va._Object_key = gag._Genotype_key)
				and e._EvidenceTerm_key = ev._Term_key''' % annotTypeKey
				
		cols, rows = dbAgnostic.execute(cmd)
		logger.debug(' - got %d MP/Genotype annotations from database' % len(rows))
		return rows
	
	def getDiseasesPerHpoTerm(self, annotTypeKey):
		# get the annotations that map from an HPO term to a DO term as a list of tuples, where each tuple is:
		#	(DO term key, HPO term key, qualifier, evidence code)
		# Note: only includes diseases with human annotations, as these are the ones returned by HMDC when
		#	querying by an HPO ID
		
		# commands to generate a couple temp tables...
		prepCmds = [
			# 0. create temp table with mapping from HPO to DO terms
			'''select va._Term_key as hpo_key, va._Object_key as do_key
				into temporary table hpo_to_do
				from voc_annot va
				where va._AnnotType_key = 1024''',
				
			# 1-2. indexing on hpo_to_do mapping table
			'create index i1 on hpo_to_do(hpo_key)',
			'create index i2 on hpo_to_do(do_key)',
			
			# 3. compute closure of DO vocabulary, including self referential links (bottom of union)
			'''select t._Term_key, c._DescendentObject_key as descendant
				into temporary table do_closure
				from voc_term t, voc_vocab v, dag_closure c
				where v.name = 'Disease Ontology'
					and t._Vocab_key = v._Vocab_key
					and t._Term_key = c._AncestorObject_key
				union
				select t._Term_key, t._Term_key
				from voc_term t, voc_vocab v
				where v.name = 'Disease Ontology'
					and t._Vocab_key = v._Vocab_key''',

			# 4-5. indexing on do_closure table
			'create index d1 on do_closure(_Term_key)',
			'create index d2 on do_closure(descendant)', 
			]
		
		i = 0
		for prepCmd in prepCmds:
			dbAgnostic.execute(prepCmd)
			logger.debug(' - executed prep cmd %d' % i)
			i = i + 1

		cmd = '''select distinct htd.do_key, htd.hpo_key, null as qualifier, null as evidence_code
			from hpo_to_do htd, do_closure dc, voc_annot va, voc_term q
			where htd.do_key = dc._Term_key
				and dc.descendant = va._Term_key
				and va._Qualifier_key = q._Term_key
				and q.term is null
				and va._AnnotType_key = 1022'''

		cols, rows = dbAgnostic.execute(cmd)
		logger.debug(' - got %d HPO/DO annotations from database' % len(rows))
		return rows
		
	def getAnnotations(self, annotTypeKey):
		# gets all the annotations for the given annotation type key as a list of tuples, where each tuple is:
		#	(object key, term key, qualifier, evidence code)
		
		cmd = '''select va._Object_key, va._Term_key, q.term as qualifier, ev.term as evidenceCode
			from voc_annot va, voc_term q, voc_evidence e, voc_term ev
			where va._AnnotType_key = %d
				and va._Qualifier_key = q._Term_key
				and va._Annot_key = e._Annot_key
				and e._EvidenceTerm_key = ev._Term_key''' % annotTypeKey
				
		cols, rows = dbAgnostic.execute(cmd)
		logger.debug(' - got %d annotations from database' % len(rows))
		return rows
	
	def getAllTermKeys(self, vocabKey):
		cmd = '''select _Term_key
			from voc_term
			where _Vocab_key = %d
			and isObsolete = 0''' % vocabKey
			
		cols, rows = dbAgnostic.execute(cmd)
		
		termKeys = []
		for row in rows:
			termKeys.append(row[0])
		logger.debug(' - got %d term keys' % len(termKeys))
		return termKeys

	def collateResults(self):
		self.finalColumns = [ 'termKey', 'mgitype', 'objectsToTerm', 'objectsWithDesc', 'annotToTerm', 'annotWithDesc' ]
		self.finalResults = []
		
		cols, rows = self.results[0]
		
		annotTypeKeyCol = Gatherer.columnNumber(cols, '_AnnotType_key')
		annotTypeNameCol = Gatherer.columnNumber(cols, 'annot_type')
		vocabNameCol = Gatherer.columnNumber(cols, 'vocab_name')
		vocabKeyCol = Gatherer.columnNumber(cols, '_Vocab_key')
		mgiTypeCol = Gatherer.columnNumber(cols, 'mgi_type')
		
		for row in rows:
			logger.debug('Beginning annotation type "%s"...' % row[annotTypeNameCol])
			vocabKey = row[vocabKeyCol]
			mgiType = row[mgiTypeCol]

			ancestors = AncestorCache(vocabKey)

			if row[annotTypeNameCol] == MP_GENOTYPE:
				collector = MPGenotypeCollector(ancestors)
				annotations = self.getMPGenotypeAnnotations(row[annotTypeKeyCol])

			elif row[annotTypeNameCol] == HPO_DO:
				collector = HpoDoCollector(ancestors)
				annotations = self.getDiseasesPerHpoTerm(row[annotTypeKeyCol])

			else:
				collector = Collector(ancestors)
				annotations = self.getAnnotations(row[annotTypeKeyCol])

			for (objectKey, termKey, qualifier, evidenceCode) in annotations:
				collector.addAnnotation(termKey, objectKey, qualifier, evidenceCode)
				
			logger.debug(' - collated annotations by term')
			
			previousRowCount = len(self.finalResults)
			for termKey in self.getAllTermKeys(vocabKey):
				self.finalResults.append( (termKey, mgiType, collector.getObjectCountDirect(termKey),
					collector.getObjectCountDag(termKey), collector.getAnnotationCountDirect(termKey),
					collector.getAnnotationCountDag(termKey)) )
			
			logger.debug(' - added %d rows to final results' % (len(self.finalResults) - previousRowCount))

			del collector
			del ancestors
			gc.collect()
			logger.debug(' - finished cleanup')
		return

###--- globals ---###

cmds = [
	# 0. get the vocabulary keys we need to process
	'''select vat._AnnotType_key, vat.name as annot_type, vat._Vocab_key, v.name as vocab_name,
			t.name as mgi_type
		from voc_annottype vat, voc_vocab v, acc_mgitype t
		where vat.name in ('%s', '%s', '%s')
			and vat._MGIType_key = t._MGIType_key
			and vat._Vocab_key = v._Vocab_key''' % (MP_GENOTYPE, GO_MARKER, HPO_DO),
	]

# order of fields (from the query results) to be written to the output file
fieldOrder = [ Gatherer.AUTO, 'termKey', 'mgitype', 'objectsToTerm', 'objectsWithDesc', 'annotToTerm', 'annotWithDesc' ]

# prefix for the filename of the output file
filenamePrefix = 'term_annotation_counts'

# global instance of a TACGatherer
gatherer = TACGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
