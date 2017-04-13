#!/usr/local/bin/python
# 
# gathers data for the 'annotation_*' and '*_to_annotation' tables in the
# front-end database
#
# Some local functions unit-tested in annotation_gatherer_tests.py
#

import dbAgnostic
import Gatherer
import logger
import GenotypeClassifier
import gc
import top

# annotation data specific imports
import go_annot_extensions
import go_isoforms
from annotation import transform
from annotation import constants as C
from annotation import sequence_num
from annotation.organism import OrganismFinder
from annotation.tooltip import TooltipFinder

###--- Constants ---###

# name of table that will hold all the _annot_keys 
#	for the currently processed batch
BATCH_TEMP_TABLE = 'annotation_batch_tmp'


class AnnotationGatherer (Gatherer.CachingMultiFileGatherer):
	
	def __init__(self,
				files,
				cmds = None):
		"""
		Extra setup for this gatherer
		"""
		Gatherer.CachingMultiFileGatherer.__init__(self, files, cmds)
		
		self.curAnnotationKey = 1
		
		self.clearGlobals()

			
	def clearGlobals(self):
		"""
		Reset/Initialize all globals
		"""
		
		self.annotGroupsMap = {}
		self.annotPropertiesMap = {}
		self.inferredfromIdMap = {}
		self.evidenceKeyToNew = {}
		
	def preprocessCommands(self):
		"""
		Pre-processing & initialization queries
		"""
				
		# Create the sequence num sorts for all the annotations
		sequence_num._createSequenceNumTables()
		
		
	### Queries ###
			
	def queryAnnotationProperties(self):
		"""
		Query a batch of annotation properties from
			BATCH_TEMP_TABLE

		Supports only GO annotation properties
		
		return as { _annotevidence_key: [ {type, property, stanza, sequencenum, value}, ] }
		"""
		propertiesMap = {}

		# Handle the GO annotation extension properties
		self._queryAnnotationExtensions(propertiesMap)
			
		# Handle the GO isoform properties
		self._queryIsoforms(propertiesMap)
		
		
		return propertiesMap
	
	def _queryAnnotationExtensions(self, propertiesMap):
		"""
		GO annotation extension propeties
		
		populates propertiesMap
		"""
		# query the correct keys to use for GO evidence and properties
		extensionProcessor = go_annot_extensions.Processor()
		propertyTermKeys = extensionProcessor.querySanctionedPropertyTermKeys()
		evidenceTermKeys = extensionProcessor.querySanctionedEvidenceTermKeys()

		propertyKeyClause = ",".join([str(k) for k in propertyTermKeys])
		evidenceKeyClause = ",".join([str(k) for k in evidenceTermKeys])

		cmd = '''
			select vep._annotevidence_key,
			prop.term as property,
			vep.stanza,
			vep.sequencenum,
			vep.value,
			nc.note as display_value
			from voc_evidence ve
			join %s abt on
				abt._annot_key = ve._annot_key
			join voc_evidence_property vep on 
				vep._annotevidence_key = ve._annotevidence_key
			join voc_term prop on
				prop._term_key = vep._propertyterm_key
			left outer join mgi_note n on
				n._object_key = vep._evidenceproperty_key
				and n._notetype_key = %d
			left outer join mgi_notechunk nc on
				nc._note_key = n._note_key
			where ve._evidenceterm_key in (%s)
				and vep._propertyterm_key in (%s)
			order by ve._annot_key, ve._refs_key, vep.stanza, vep.sequencenum
		''' % (
			BATCH_TEMP_TABLE,
			C.GO_EXTENSION_NOTETYPE_KEY,
			evidenceKeyClause, 
			propertyKeyClause
		)
	
		(cols, rows) = dbAgnostic.execute(cmd)
		
		evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
		propertyCol = dbAgnostic.columnNumber (cols, 'property')
		stanzaCol = dbAgnostic.columnNumber (cols, 'stanza')
		seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
		valueCol = dbAgnostic.columnNumber (cols, 'value')
		displayValueCol = dbAgnostic.columnNumber (cols, 'display_value')
		
		for row in rows:
			evidenceKey = row[evidenceKeyCol]
			property = row[propertyCol]
			stanza = row[stanzaCol]
			seqnum = row[seqnumCol]
			value = row[valueCol]
			displayValue = row[displayValueCol]

			# process the annotation extensions to remove comments, etc
			value = extensionProcessor.processValue(value)
			
			if not displayValue:
				displayValue = value
			
			propObj = {
					'type': 'Annotation Extension',
					'property':property,
					'stanza':stanza,
					'sequencenum':seqnum,
					'value':displayValue,
					}
			propertiesMap.setdefault(evidenceKey, []).append(propObj)
	
	
	def _queryIsoforms(self, propertiesMap):
		"""
		GO isoforms
		
		populates propertiesMap
		"""
		# query the correct keys to use for GO isoform properties
		isoformProcessor = go_isoforms.Processor()
		propertyTermKeys = isoformProcessor.querySanctionedPropertyTermKeys()

		propertyKeyClause = ",".join([str(k) for k in propertyTermKeys])

		cmd = '''
			select vep._annotevidence_key,
			prop.term as property,
			vep.stanza,
			vep.sequencenum,
			vep.value,
			nc.note displayValue
			from voc_evidence ve
			join %s abt on
				abt._annot_key = ve._annot_key
			join voc_evidence_property vep on 
				vep._annotevidence_key = ve._annotevidence_key
			join voc_term prop on
				prop._term_key = vep._propertyterm_key
			join mgi_note n on
				n._object_key = vep._evidenceproperty_key
				and n._notetype_key = %s
			join mgi_notechunk nc on
				nc._note_key = n._note_key
			where vep._propertyterm_key in (%s)
			order by ve._annot_key, ve._refs_key, vep.stanza, vep.sequencenum
		''' % (BATCH_TEMP_TABLE, 
			C.GO_ISOFORM_NOTETYPE_KEY,
			propertyKeyClause
		)
	
		(cols, rows) = dbAgnostic.execute(cmd)
		
		evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
		propertyCol = dbAgnostic.columnNumber (cols, 'property')
		stanzaCol = dbAgnostic.columnNumber (cols, 'stanza')
		seqnumCol = dbAgnostic.columnNumber (cols, 'sequencenum')
		valueCol = dbAgnostic.columnNumber (cols, 'value')
		displayValueCol = dbAgnostic.columnNumber (cols, 'displayValue')
		
		for row in rows:
			evidenceKey = row[evidenceKeyCol]
			property = row[propertyCol]
			stanza = row[stanzaCol]
			seqnum = row[seqnumCol]
			displayValue = row[displayValueCol]
				
			propObj = {
					'type': 'Isoform',
					'property':property,
					'stanza':stanza,
					'sequencenum':seqnum,
					'value':displayValue,
					}
			propertiesMap.setdefault(evidenceKey, []).append(propObj)
				
	
	def queryPropertyDisplayNames(self):
		"""
		Query annotation property display names
			
		Returns propDisplayMap as {property -> [displayName] }
		
		Looks specifically fo GO Property synonyms
		"""
		
		# Get synonyms for GO Properties
		
		cmd = '''select t.term as property,
			ms.synonym as display
		from voc_term t
		join mgi_synonym ms on
			ms._object_key = t._term_key
			and ms._synonymtype_key = %d
		''' % C.GOREL_SYNONYM_TYPE
		
		(cols, rows) = dbAgnostic.execute(cmd)
		
		propertyCol = dbAgnostic.columnNumber (cols, 'property')
		displayNameCol = dbAgnostic.columnNumber (cols, 'display')
		
		propDisplayMap = {}
		for row in rows:
			property = row[propertyCol]
			displayName = row[displayNameCol]
			
			propDisplayMap[property] = displayName
			
		return propDisplayMap
	
	
	def queryInferredFromIds(self):
		"""
		Query all inferredfrom IDs for _annot_key from BATCH_TEMP_TABLE
		
		Return map { _annotevidence_key: [{ id, tooltip, logicaldb}] }
		"""
		
		# initialize an organism finder for the inferredfrom ID data
		organismFinder = OrganismFinder(annotBatchTableName=BATCH_TEMP_TABLE)
		
		# initialize a tooltip finder for the the other inferredfrom tooltips
		tooltipFinder = TooltipFinder(annotBatchTableName=BATCH_TEMP_TABLE)
				
		cmd = '''
			select ve._annotevidence_key, a.accid, ldb._logicaldb_key, ldb.name as logicaldb
			from voc_evidence ve
			join acc_accession a on
				a._object_key = ve._annotevidence_key
				and a._mgitype_key = 25
			join acc_logicaldb ldb on
				ldb._logicaldb_key = a._logicaldb_key
		'''
		
		(cols, rows) = dbAgnostic.execute(cmd)
		
		evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
		accidCol = dbAgnostic.columnNumber (cols, 'accid')
		ldbCol = dbAgnostic.columnNumber (cols, 'logicaldb')
		
		inferredfromIdMap = {}
		for row in rows:
			evidenceKey = row[evidenceKeyCol]
			accid = row[accidCol]
			ldb = row[ldbCol]
			
			# prefer object-specific tooltip, fall back on general organism tooltip
			tooltip = tooltipFinder.getTooltip(accid)
			if not tooltip:
				tooltip = organismFinder.getOrganism(accid)
			
			inferredfromObj = {'id':accid, 'tooltip': tooltip, 'logicaldb': ldb}
			inferredfromIdMap.setdefault(evidenceKey, []).append(inferredfromObj)
		
		return inferredfromIdMap
	
	
	def queryHeaderTerms(self):
		"""
		Query annotation header terms for
			_annot_key in BATCH_TEMP_TABLE
			
		Returns headerMap as {_term_key -> [_header_key] }
			
		Note: Only processes GO header terms
		"""
		
		# Get terms for GO
		
		cmd = '''select t._Term_key as header_key,
		t._term_key
		from voc_term t
		join voc_annot va on
			va._term_key = t._term_key
		join %s abt on
			abt._annot_key = va._annot_key
		where t._Vocab_key = 4
			and t.abbreviation is not null
			and t.sequenceNum is not null
		union
		select h._Term_key as header_key, 
		t._term_key
		from voc_term h
		join dag_closure dc on
			dc._ancestorobject_key = h._term_key
		join voc_term t on
			t._term_key = dc._descendentobject_key
		join voc_annot va on
			va._term_key = t._term_key
		join %s abt on
			abt._annot_key = va._annot_key
		where h._Vocab_key = 4
			and h.abbreviation is not null
			and h.sequenceNum is not null
		''' % (BATCH_TEMP_TABLE, BATCH_TEMP_TABLE)
		
		(cols, rows) = dbAgnostic.execute(cmd)
		
		termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
		headerKeyCol = dbAgnostic.columnNumber (cols, 'header_key')
		
		headerMap = {}
		for row in rows:
			termKey = row[termKeyCol]
			headerKey = row[headerKeyCol]
			
			headerMap.setdefault(termKey, []).append(headerKey)
			
		return headerMap
	
	
	### Helper Methods ###
	
	def transformAnnotations(self):
		"""
		Perform data transforms on the base annotation record data
		"""
		
		# make query results editable
		self.results[3] = ( self.results[3][0], dbAgnostic.tuplesToLists(self.results[3][1]) )
		
		cols, rows = self.results[3]
		
		
		transform.removeGONoDataAnnotations(cols, rows)
		transform.transformAnnotationType(cols, rows)
			
		

	### Building specific tables ###

	def buildAnnotation(self):
		"""
		build the annotation table
		"""
		
		cols, rows = self.results[3]
		
		annotKeyCol = Gatherer.columnNumber (cols, '_annot_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		termCol = Gatherer.columnNumber (cols, 'term')
		termKeyCol = Gatherer.columnNumber (cols, '_term_key')
		termIdCol = Gatherer.columnNumber (cols, 'term_id')
		objectKeyCol = Gatherer.columnNumber (cols, '_object_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		evidenceTermCol = Gatherer.columnNumber (cols, 'evidence_term')
		evidenceCodeCol = Gatherer.columnNumber (cols, 'evidence_code')
		refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
		inferredfromCol = Gatherer.columnNumber (cols, 'inferredfrom')
		vocabCol = Gatherer.columnNumber (cols, 'vocab')
		dagCol = Gatherer.columnNumber (cols, 'dag_name')
		mgitypeCol = Gatherer.columnNumber (cols, 'mgitype')
		
		if not rows:
			return
			
		# fetch properties for grouping
		propertiesMap = self.queryAnnotationProperties()
		
		# fetch inferredfrom IDs
		inferredfromIdMap = self.queryInferredFromIds()
			
		# group/roll up annotations
		groupMap = transform.groupAnnotations(cols, rows,
										propertiesMap=propertiesMap)
			
		annots = []
		for rows in groupMap.values():
			
			repRow = rows[0]
			
			# get basic info shared between grouped annotations
			annotType = repRow[annotTypeCol]
			term = repRow[termCol]
			termKey = repRow[termKeyCol]
			termId = repRow[termIdCol]
			objectKey = repRow[objectKeyCol]
			qualifier = repRow[qualifierCol]
			evidenceKey = repRow[evidenceKeyCol]
			evidenceTerm = repRow[evidenceTermCol]
			evidenceCode = repRow[evidenceCodeCol]
			dagName = repRow[dagCol]
			vocabName = repRow[vocabCol]
			mgitype = repRow[mgitypeCol]
			
			# count references
			refKeys = set([])
			for row in rows:
				refsKey = row[refsKeyCol]
				refKeys.add(refsKey)
			refsCount = len(refKeys)
			
			# count inferredfrom IDs
			inferredIds = set([])
			for row in rows:
				evidenceKey = row[evidenceKeyCol]
				if evidenceKey in inferredfromIdMap:
					for idObj in inferredfromIdMap[evidenceKey]:
						inferredIds.add(idObj['id'])
			inferredIdCount = len(inferredIds)
			
			# set the new annotation/evidence keys
			for row in rows:
				evidenceKey = row[evidenceKeyCol]
				self.evidenceKeyToNew[evidenceKey] = self.curAnnotationKey
			
			# append new annotation row
			annots.append((
						self.curAnnotationKey,
						dagName,
						qualifier,
						vocabName,
						term,
						termId,
						termKey,
						evidenceCode,
						evidenceTerm,
						mgitype,
						annotType,
						refsCount,
						inferredIdCount
						))
			
			self.curAnnotationKey += 1
			
			
		self.addRows('annotation', annots)
		
		# store these for sub-table processing
		self.annotPropertiesMap = propertiesMap
		self.inferredfromIdMap = inferredfromIdMap
		self.annotGroupsMap = groupMap
		
		
	def buildAnnotationProperty(self):
		"""
		Build the annotation_property table
		
		Assumes self.annotPropertiesMap has been initialized (in buildAnnotation)
		"""
		
		# get alternate display names for properties
		propDisplayMap = self.queryPropertyDisplayNames()
		
		propertyRows = []
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		
		for rows in self.annotGroupsMap.values():
			
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
				
			# get any properties
			aggregatedProps = transform.getAggregatedProperties(cols, rows, self.annotPropertiesMap)
			
			if aggregatedProps:
				
				for prop in aggregatedProps:
					
					# if we have an alternate display synonym, use it
					property = prop['property']
					if property in propDisplayMap:
						property = propDisplayMap[property]
					
					propertyRows.append((
						newAnnotationKey,
						prop['type'],
						property,
						prop['value'],
						prop['stanza'],
						prop['sequencenum']
					))
		
		self.addRows('annotation_property', propertyRows)
		
	
	def buildAnnotationInferredFromId(self):
		"""
		Build the annotation_inferred_from_id table
		
		Assumes that self.inferredfromIdMap has been initialized (in buildAnnotation)
		"""
		
		# build the inferred_from_id rows
		
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		inferredfromCol = Gatherer.columnNumber (cols, 'inferredfrom')
		
		inferredIdRows = []
		
		for rows in self.annotGroupsMap.values():
		
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
			
			# gather all the IDs for this grouped annotation 
			inferredIdObjs = []
			seenIds = set([])
			for row in rows:
				evidenceKey = row[evidenceKeyCol]
				
				if evidenceKey in self.inferredfromIdMap:
					for idObj in self.inferredfromIdMap[evidenceKey]:
						if (idObj['id'], idObj['logicaldb']) in seenIds:
							continue
						seenIds.add((idObj['id'], idObj['logicaldb']))
						inferredIdObjs.append(idObj)
						
			# sort by IDs
			inferredIdObjs.sort(key=lambda x: x['id'])
			
			seqnum = 1
			for idObj in inferredIdObjs:
				
				logicalDb = idObj['logicaldb']
				id = idObj['id']
				tooltip = idObj['tooltip']
				
				inferredIdRows.append((
					newAnnotationKey,
					logicalDb,
					id,
					tooltip,
					seqnum
				))
				
				seqnum += 1
						
						
		self.addRows('annotation_inferred_from_id', inferredIdRows)
		
		
	def buildAnnotationReference(self):
		"""
		Build the annotation_reference table
		"""
		
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
		jnumidCol = Gatherer.columnNumber (cols, 'jnumid')
		
		referenceRows = []
		
		for rows in self.annotGroupsMap.values():
		
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
			
			
			# aggregate the references for this annotation group
			seen = set([])
			refs = []
			for row in rows:
				evidenceKey = row[evidenceKeyCol]
				refsKey = row[refsKeyCol]
				jnumid = row[jnumidCol]
				
				if refsKey in seen:
					continue
				seen.add(refsKey)
				
				refs.append( (refsKey, jnumid) )
				
			# sort by refsKey
			refs.sort(key=lambda x: x[0])
				
			seqnum = 1
			for ref in refs:
				
				referenceRows.append((
					newAnnotationKey,
					ref[0],
					ref[1],
					seqnum
				))
				
				seqnum += 1
				
		self.addRows('annotation_reference', referenceRows)
				
				
	def buildMarkerToAnnotation(self):
		"""
		Build the marker_to_annotation table
		"""
		
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		objectKeyCol = Gatherer.columnNumber (cols, '_object_key')
		refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		mgitypeCol = Gatherer.columnNumber (cols, 'mgitype')
		
		markerAnnotRows = []
		
		for rows in self.annotGroupsMap.values():
		
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
			objectKey = repRow[objectKeyCol]
			mgitype = repRow[mgitypeCol]
			
			if mgitype == 'Marker':
				
				markerAnnotRows.append((
					objectKey,
					newAnnotationKey,
					annotType
				))
				
				
		self.addRows('marker_to_annotation', markerAnnotRows)
		
	def buildGenotypeToAnnotation(self):
		"""
		Build the genotype_to_annotation table
		"""
		
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		objectKeyCol = Gatherer.columnNumber (cols, '_object_key')
		refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		mgitypeCol = Gatherer.columnNumber (cols, 'mgitype')
		
		genotypeAnnotRows = []
		
		for rows in self.annotGroupsMap.values():
		
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
			objectKey = repRow[objectKeyCol]
			mgitype = repRow[mgitypeCol]
			
			if mgitype == 'Genotype':
				
				genotypeAnnotRows.append((
					objectKey,
					newAnnotationKey,
					annotType
				))
				
				
		self.addRows('genotype_to_annotation', genotypeAnnotRows)
		
	
	def buildAnnotationHeader(self):
		"""
		Build the annotation_to_header table
		"""
		
		# query the _term_key -> header _term_key mappings
		headerMap = self.queryHeaderTerms()
		
		
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		termKeyCol = Gatherer.columnNumber (cols, '_term_key')
		
		headerRows = []
		
		for rows in self.annotGroupsMap.values():
		
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
			termKey = repRow[termKeyCol]
			
			if termKey in headerMap:
			
				for headerKey in headerMap[termKey]:
					
					headerRows.append((
						newAnnotationKey,
						headerKey
					))
				
				
		self.addRows('annotation_to_header', headerRows)
		
		
	def buildAnnotationSequenceNum(self):
		"""
		Build the annotation_sequence_num table
		"""
		
		byVocabMap = sequence_num.queryByVocabSeqnums()
		byAnnotTypeMap = sequence_num.queryByAnnotTypeSeqnums()
		byTermAlphaMap = sequence_num.queryByTermAlphaSeqnums( annotBatchTableName=BATCH_TEMP_TABLE )
		byTermDagMap = sequence_num.queryByTermDagSeqnums( annotBatchTableName=BATCH_TEMP_TABLE )
		byVocabDagMap = sequence_num.queryByVocabDagSeqnums( annotBatchTableName=BATCH_TEMP_TABLE )
		byObjectDagMap = sequence_num.queryByObjectDagSeqnums( annotBatchTableName=BATCH_TEMP_TABLE )
		byIsoformMap = sequence_num.queryByIsoformSeqnums( annotBatchTableName=BATCH_TEMP_TABLE )
		
		cols = self.results[3][0]
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
		annotKeyCol = Gatherer.columnNumber (cols, '_annot_key')
		termKeyCol = Gatherer.columnNumber (cols, '_term_key')
		vocabCol = Gatherer.columnNumber (cols, 'vocab')
		
		seqnumRows = []
		
		for rows in self.annotGroupsMap.values():
		
			repRow = rows[0]
			
			newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
			annotType = repRow[annotTypeCol]
			annotKey = repRow[annotKeyCol]
			termKey = repRow[termKeyCol]
			vocab = repRow[vocabCol]
			repEvidenceKey = repRow[evidenceKeyCol]
			
			byVocab = byVocabMap[vocab]
			byAnnotType = byAnnotTypeMap[annotType]
			byTermAlpha = byTermAlphaMap[termKey]
			
			byDagStructure = 0
			if termKey in byTermDagMap:
				byDagStructure = byTermDagMap[termKey]
			
			byVocabDagTerm = 0
			if termKey in byVocabDagMap:
				byVocabDagTerm = byVocabDagMap[termKey]
				
			byObjectDagTerm = 0
			if annotKey in byObjectDagMap:
				byObjectDagTerm = byObjectDagMap[annotKey]
				
			byIsoform = 0
			if repEvidenceKey in byIsoformMap:
				byIsoform = byIsoformMap[repEvidenceKey]
					
			seqnumRows.append((
				newAnnotationKey,
				byDagStructure,
				byTermAlpha,
				byVocab,
				byAnnotType,
				byVocabDagTerm,
				byObjectDagTerm,
				byIsoform
			))
				
		self.addRows('annotation_sequence_num', seqnumRows)
		
	
	def collateResults (self):
		"""
		Process the results of cmds queries
		
		Builds all the tables in files 
			e.g. annotation, annotation_inferred_from_id, etc
		"""
		
		# perform any necessary data transforms on the base query
		self.transformAnnotations()
		
		# Build the tables for each batch
		
		self.buildAnnotation()
		
		self.buildAnnotationProperty()
		
		self.buildAnnotationInferredFromId()
		
		self.buildAnnotationReference()
		
		self.buildMarkerToAnnotation()
		
		self.buildGenotypeToAnnotation()
		
		self.buildAnnotationHeader()
		
		self.buildAnnotationSequenceNum()
		
		# clear the memory state for this batch of annotations
		self.clearGlobals()
		
		

###--- globals ---###

cmds = [
	
	#
	# 0 - 2. setup a temp table for each batch
	#
	'''
	drop table if exists %s
	''' % BATCH_TEMP_TABLE,
	'''
	select _annot_key into
	temp %s
	from voc_annot va
	order by va._annottype_key, va._object_key, va._term_key
	limit 250000 		-- limit must match chunkSize
	offset %%d -- %%d
	''' % BATCH_TEMP_TABLE,
	'''
	create index annotation_batch_tmp_idx on %s (_annot_key)
	''' % BATCH_TEMP_TABLE,
	
	#
	# 3. Base Annotation/Evidence data
	#
	#	All other information is considered secondary and is
	#	gathered at runtime for the specific sub-tables that
	#	require secondary information (e.g. evidence properties & inferredfrom IDs)
	#
	'''select va._annot_key, 
		vat.name as annottype, 
		va._object_key, 
		term._term_key,
		term.term, 
		term_acc.accid as term_id,
		qual.term as qualifier,
		ve._annotevidence_key, 
		ev_term.term as evidence_term,
		ev_term.abbreviation as evidence_code,
		ve._refs_key, 
		ref_acc.accid as jnumid,
		ve.inferredfrom,
		voc.name as vocab,
		dag.name as dag_name,
		mtype.name as mgitype
		from voc_annot va
		join %s abt on 			-- batch annotations with above temp table
			abt._annot_key = va._annot_key
		join voc_evidence ve on 
			ve._annot_key = va._annot_key
		join voc_annottype vat on
			vat._annottype_key = va._annottype_key
		join voc_term term on						-- annotated term
			term._term_key = va._term_key
		join voc_term qual on 						-- qualifier
			qual._term_key = va._qualifier_key
		join voc_term ev_term on 					-- evidence term/code
			ev_term._term_key = ve._evidenceterm_key
		join voc_vocab voc on						-- annotated vocab
			voc._vocab_key = term._vocab_key
		join acc_mgitype mtype on					-- mgi type
			mtype._mgitype_key = vat._mgitype_key
		join acc_accession ref_acc on				-- j number
			ref_acc._object_key = ve._refs_key
			and ref_acc._mgitype_key = 1
			and ref_acc.prefixpart = 'J:'
			and ref_acc.preferred = 1
		left outer join acc_accession term_acc on	-- term accid
			term_acc._object_key = va._term_key
			and term_acc._mgitype_key = 13
			and term_acc.preferred = 1
			and term_acc.private = 0
		left outer join dag_node dn on				-- dag name
			dn._object_key = va._term_key
		left outer join dag_dag dag on
			dag._dag_key = dn._dag_key
	''' % BATCH_TEMP_TABLE,
	
	]

# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('annotation',
		[ 'annotation_key', 'dag_name', 'qualifier', 'vocab_name',
			'term', 'term_id', 'term_key',
			'evidence_code', 'evidence_term',
			'object_type', 'annotation_type', 'reference_count',
			'inferred_id_count' ],
		[ 'annotation_key', 'dag_name', 'qualifier', 'vocab_name',
			'term', 'term_id', 'term_key',
			'evidence_code', 'evidence_term',
			'object_type', 'annotation_type', 'reference_count',
			'inferred_id_count' ]
	),
		
	('annotation_property',
		['annotation_key', 'type', 'property', 'value', 'stanza', 'sequencenum' ],
		[Gatherer.AUTO, 'annotation_key', 'type', 'property', 'value', 'stanza', 'sequencenum' ]
	),

	('annotation_inferred_from_id',
		[ 'annotation_key', 'logical_db', 'acc_id',
			'tooltip', 'sequence_num' ],
		[ Gatherer.AUTO, 'annotation_key', 'logical_db', 'acc_id',
			'tooltip', 'sequence_num' ]
	),

	('annotation_reference',
		[ 'annotation_key', 'reference_key',
			'jnum_id', 'sequence_num' ],
		[ Gatherer.AUTO, 'annotation_key', 'reference_key',
			'jnum_id', 'sequence_num' ]
	),

	('marker_to_annotation',
		[ 'marker_key', 'annotation_key','annotation_type' ],
		[ Gatherer.AUTO, 'marker_key', 'annotation_key',
			'annotation_type' ]
	),

	('genotype_to_annotation',
		[ 'genotype_key', 'annotation_key','annotation_type' ],
		[ Gatherer.AUTO, 'genotype_key', 'annotation_key',
			'annotation_type' ]
	),

	('annotation_to_header',
		['annotation_key', 'header_term_key' ],
		[Gatherer.AUTO, 'annotation_key', 'header_term_key' ]
	),
		
	('annotation_sequence_num',
		[ 'annotation_key', 'by_dag_structure', 'by_term_alpha',
			'by_vocab', 'by_annotation_type', 'by_vocab_dag_term',
			'by_object_dag_term', 'by_isoform'
			],
		[ 'annotation_key', 'by_dag_structure', 'by_term_alpha',
			'by_vocab', 'by_annotation_type', 'by_vocab_dag_term',
			'by_object_dag_term', 'by_isoform'
			]
	),
	]

# global instance of a AnnotationGatherer
gatherer = AnnotationGatherer (files, cmds)

# voc_annot is sparsely populated, so we use limit/offset instead
#	of the traditional min(key) - max(key) approach
gatherer.setupChunking(
	minKeyQuery = '''select 0''',
	maxKeyQuery = '''select count(*) from voc_annot''',
	chunkSize = 250000				
	)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
