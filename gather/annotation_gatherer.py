#!./python
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
import Lookup
import VocabUtils

# annotation data specific imports
import go_annot_extensions
import go_isoforms
from annotation import mapper
from annotation import transform
from annotation import constants as C
from annotation import sequence_num
from annotation.organism import OrganismFinder
from annotation.tooltip import TooltipFinder

###--- Constants ---###

# name of table that will hold all the _annot_keys 
#       for the currently processed batch
BATCH_TEMP_TABLE = 'annotation_batch_tmp'

# name of table that will hold all the _annot_keys, properly ordered
TEMP_TABLE = 'ordered_annotations'

# header terms for GO, MP, EMAPA vocabularies
HEADER_TEMP_TABLE = VocabUtils.getHeaderTermTempTable()

# minimum and maximum row_num values included in TEMP_TABLE
MIN_ROW_NUM = 0
MAX_ROW_NUM = 0

###--- controlled vocabulary lookups ---###

MGITYPE_LOOKUP = Lookup.Lookup('acc_mgitype', '_MGIType_key', 'name', initClause = '_MGIType_key > 0')
JNUM_LOOKUP = Lookup.Lookup('bib_citation_cache', '_Refs_key', 'jnumid', initClause = '_Refs_key > 0')
VOCAB_LOOKUP = Lookup.Lookup('voc_vocab', '_Vocab_key', 'name', initClause = '_Vocab_key > 0')
DAG_LOOKUP = Lookup.Lookup('dag_dag', '_DAG_key', 'name', initClause = '_DAG_key > 0')
EVIDENCE_TERM_LOOKUP = Lookup.Lookup('voc_term', '_Term_key', 'term',
        initClause = '_Vocab_key in (2, 3, 43, 45, 57, 80, 85, 107)')
EVIDENCE_CODE_LOOKUP = Lookup.Lookup('voc_term', '_Term_key', 'abbreviation',
        initClause = '_Vocab_key in (2, 3, 43, 45, 57, 80, 85, 107)')
QUALIFIER_LOOKUP = Lookup.Lookup('voc_term', '_Term_key', 'term',
        initClause = '_Vocab_key in (52, 53, 54, 84, 108)')

###--- Global caches ---###

# get the shared organism finder for the inferredfrom ID data
organismFinder = OrganismFinder()

# initialize a tooltip finder for the the other inferredfrom tooltips
tooltipFinder = TooltipFinder()
                                
annotKeyMapper = mapper.AnnotationMapper()              # maps prod _Annot_key to fe annotation_key
evidenceCols = None             # rather than just repeatedly sending the same evidence query, cache results here
evidenceRows = None

###--- Functions ---###

def intSortKey(s):
        # return a sort key for a single integer item 's' (protecting against None)
        if s != None:
                return s
        return -99999

def strSortKey(s):
        # return a sort key for a single string item 's' (protecting against None)
        if s != None:
                return s
        return '      '

def plainSortKey(s):
        return str(s)

def orderedKeys(dictionary):
        # returns the keys of 'dictionary' as a sorted list
        keys = list(dictionary.keys())
        fn = plainSortKey
        for k in keys:
                if type(k) == int:
                        fn = intSortKey
                        break
                elif type(k) == str:
                        fn = strSortKey
                        break
        keys.sort(key=fn)
        return keys

def orderedValues(dictionary):
        # returns a list of values from the dictionary, ordered according to the sorted list of keys
        
        values = []
        for key in orderedKeys(dictionary):
                values.append(dictionary[key])
        return values

###--- Classes ---###

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
                
                self.annotGroupsRows = []
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
                        n.note as display_value
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
                        n.note displayValue
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
                global evidenceCols, evidenceRows, organismFinder, tooltipFinder
                
                # This function is called for each batch.  Rather than process this query each time, only
                # ask the database once and then use the cached result thereafter.
                if evidenceCols == None:
                        cmd = '''
                                select ve._annotevidence_key, a.accid, ldb._logicaldb_key, ldb.name as logicaldb
                                from voc_evidence ve
                                join acc_accession a on
                                        a._object_key = ve._annotevidence_key
                                        and a._mgitype_key = 25
                                join acc_logicaldb ldb on
                                        ldb._logicaldb_key = a._logicaldb_key
                                order by 1, 3, 2'''
                
                        (evidenceCols, evidenceRows) = dbAgnostic.execute(cmd)
                
                evidenceKeyCol = dbAgnostic.columnNumber (evidenceCols, '_annotevidence_key')
                accidCol = dbAgnostic.columnNumber (evidenceCols, 'accid')
                ldbCol = dbAgnostic.columnNumber (evidenceCols, 'logicaldb')
                
                inferredfromIdMap = {}
                for row in evidenceRows:
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
                
                cmd = '''select t._Term_key as header_key, t._term_key
                from voc_term t
                join voc_annot va on (va._term_key = t._term_key)
                join %s abt on (abt._annot_key = va._annot_key)
                join %s headers on (t._Term_key = headers._Term_key)
                where t._Vocab_key = 4
                union
                select h._Term_key as header_key, t._term_key
                from %s h
                join dag_closure dc on (dc._ancestorobject_key = h._term_key)
                join voc_term t on (t._term_key = dc._descendentobject_key)
                join voc_annot va on (va._term_key = t._term_key)
                join %s abt on (abt._annot_key = va._annot_key)
                where h._Vocab_key = 4
                order by 2, 1
                ''' % (BATCH_TEMP_TABLE, HEADER_TEMP_TABLE, HEADER_TEMP_TABLE, BATCH_TEMP_TABLE)
                
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
                self.results[-1] = ( self.results[-1][0], dbAgnostic.tuplesToLists(self.results[-1][1]) )
                
                cols, rows = self.results[-1]
                
                
                transform.removeGONoDataAnnotations(cols, rows)
                transform.transformAnnotationType(cols, rows)
                        
                

        ### Building specific tables ###

        def buildAnnotation(self):
                """
                build the annotation table
                """
                global annotKeyMapper
                
                cols, rows = self.results[-1]
                
                annotKeyCol = Gatherer.columnNumber (cols, '_annot_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                termCol = Gatherer.columnNumber (cols, 'term')
                termKeyCol = Gatherer.columnNumber (cols, '_term_key')
                termIdCol = Gatherer.columnNumber (cols, 'term_id')
                objectKeyCol = Gatherer.columnNumber (cols, '_object_key')
                qualifierKeyCol = Gatherer.columnNumber (cols, '_qualifier_key')
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                evidenceTermKeyCol = Gatherer.columnNumber (cols, '_evidenceterm_key')
                refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
                inferredfromCol = Gatherer.columnNumber (cols, 'inferredfrom')
                vocabKeyCol = Gatherer.columnNumber (cols, '_vocab_key')
                dagKeyCol = Gatherer.columnNumber (cols, '_dag_key')
                mgitypeKeyCol = Gatherer.columnNumber (cols, '_mgitype_key')
                
                if not rows:
                        return
                        
                # fetch properties for grouping
                propertiesMap = self.queryAnnotationProperties()
                
                # fetch inferredfrom IDs
                inferredfromIdMap = self.queryInferredFromIds()
                        
                # group/roll up annotations
                self.annotGroupsMap = transform.groupAnnotations(cols, rows,
                                                                                propertiesMap=propertiesMap)
                self.annotGroupsRows = orderedValues(self.annotGroupsMap)
                        
                for rows in self.annotGroupsRows:
                        
                        repRow = rows[0]
                        
                        # get basic info shared between grouped annotations
                        annotType = repRow[annotTypeCol]
                        term = repRow[termCol]
                        termKey = repRow[termKeyCol]
                        termId = repRow[termIdCol]
                        objectKey = repRow[objectKeyCol]
                        evidenceKey = repRow[evidenceKeyCol]
                        evidenceTerm = EVIDENCE_TERM_LOOKUP.get(repRow[evidenceTermKeyCol])
                        evidenceCode = EVIDENCE_CODE_LOOKUP.get(repRow[evidenceTermKeyCol])
                        dagName = None
                        if repRow[dagKeyCol]:
                            dagName = DAG_LOOKUP.get(repRow[dagKeyCol])
                        mgitype = MGITYPE_LOOKUP.get(repRow[mgitypeKeyCol])
                        
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
                        
                        vocab = VOCAB_LOOKUP.get(repRow[vocabKeyCol])

                        # append new annotation row
#                       annots.append((
                        self.addRow('annotation', (
                                                self.curAnnotationKey,
                                                dagName,
                                                QUALIFIER_LOOKUP.get(repRow[qualifierKeyCol]),
                                                vocab,
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

                        # only need to cache mappings for MP and DO, as those are the ones that are programmatically
                        # rolled up to markers (and so the only ones that have source annotation properties.
                        if vocab in ('Mammalian Phenotype', 'Disease Ontology'):
                                annotKeyMapper.map(repRow[annotKeyCol], self.curAnnotationKey)

                        self.curAnnotationKey += 1
                        
                # store these for sub-table processing
                self.annotPropertiesMap = propertiesMap
                self.inferredfromIdMap = inferredfromIdMap
                
                
        def buildAnnotationProperty(self):
                """
                Build the annotation_property table
                
                Assumes self.annotPropertiesMap has been initialized (in buildAnnotation)
                """
                
                # get alternate display names for properties
                propDisplayMap = self.queryPropertyDisplayNames()
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                
                for rows in self.annotGroupsRows:
                        
                        repRow = rows[0]
                        
                        newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
                        annotType = repRow[annotTypeCol]
                                
                        # get any properties
                        aggregatedProps = transform.getAggregatedProperties(cols, rows, self.annotPropertiesMap)
                        
                        if aggregatedProps:
                                
                                for prop in aggregatedProps:
                                        
                                        # if we have an alternate display synonym, use it
                                        property = prop['property']
                                        if property in orderedKeys(propDisplayMap):
                                                property = propDisplayMap[property]
                                        
                                        self.addRow('annotation_property', (
                                                newAnnotationKey,
                                                prop['type'],
                                                property,
                                                prop['value'],
                                                prop['stanza'],
                                                prop['sequencenum']
                                        ))
                
                
        
        def buildAnnotationInferredFromId(self):
                """
                Build the annotation_inferred_from_id table
                
                Assumes that self.inferredfromIdMap has been initialized (in buildAnnotation)
                """
                
                # build the inferred_from_id rows
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                inferredfromCol = Gatherer.columnNumber (cols, 'inferredfrom')
                
                for rows in self.annotGroupsRows:
                
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
                                
                                self.addRow('annotation_inferred_from_id', (
                                        newAnnotationKey,
                                        logicalDb,
                                        id,
                                        tooltip,
                                        seqnum
                                ))
                                
                                seqnum += 1
                                                
                                                
        def buildAnnotationReference(self):
                """
                Build the annotation_reference table
                """
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
                
                for rows in self.annotGroupsRows:
                
                        repRow = rows[0]
                        
                        newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
                        annotType = repRow[annotTypeCol]
                        
                        
                        # aggregate the references for this annotation group
                        seen = set([])
                        refs = []
                        for row in rows:
                                evidenceKey = row[evidenceKeyCol]
                                refsKey = row[refsKeyCol]
                                
                                if refsKey in seen:
                                        continue
                                seen.add(refsKey)
                                
                                refs.append( (refsKey, JNUM_LOOKUP.get(refsKey)) )
                                
                        # sort by refsKey
                        refs.sort(key=lambda x: x[0])
                                
                        seqnum = 1
                        for ref in refs:
                                
                                self.addRow('annotation_reference', (
                                        newAnnotationKey,
                                        ref[0],
                                        ref[1],
                                        seqnum
                                ))
                                
                                seqnum += 1
                                
                                
        def buildMarkerToAnnotation(self):
                """
                Build the marker_to_annotation table
                """
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                objectKeyCol = Gatherer.columnNumber (cols, '_object_key')
                refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
                mgitypeKeyCol = Gatherer.columnNumber (cols, '_mgitype_key')
                
                for rows in self.annotGroupsRows:
                
                        repRow = rows[0]
                        
                        newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
                        annotType = repRow[annotTypeCol]
                        objectKey = repRow[objectKeyCol]
                        mgitype = MGITYPE_LOOKUP.get(repRow[mgitypeKeyCol])
                        
                        if mgitype == 'Marker':
                                
                                self.addRow('marker_to_annotation', (
                                        objectKey,
                                        newAnnotationKey,
                                        annotType
                                ))
                                
                
        def buildGenotypeToAnnotation(self):
                """
                Build the genotype_to_annotation table
                """
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                objectKeyCol = Gatherer.columnNumber (cols, '_object_key')
                refsKeyCol = Gatherer.columnNumber (cols, '_refs_key')
                mgitypeKeyCol = Gatherer.columnNumber (cols, '_mgitype_key')
                
                for rows in self.annotGroupsRows:
                
                        repRow = rows[0]
                        
                        newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
                        annotType = repRow[annotTypeCol]
                        objectKey = repRow[objectKeyCol]
                        mgitype = MGITYPE_LOOKUP.get(repRow[mgitypeKeyCol])
                        
                        if mgitype == 'Genotype':
                                
                                self.addRow('genotype_to_annotation', (
                                        objectKey,
                                        newAnnotationKey,
                                        annotType
                                ))
                
        
        def buildAnnotationHeader(self):
                """
                Build the annotation_to_header table
                """
                
                # query the _term_key -> header _term_key mappings
                headerMap = self.queryHeaderTerms()
                
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                termKeyCol = Gatherer.columnNumber (cols, '_term_key')
                
                headerRows = []
                
                for rows in self.annotGroupsRows:
                
                        repRow = rows[0]
                        
                        newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
                        annotType = repRow[annotTypeCol]
                        termKey = repRow[termKeyCol]
                        
                        if termKey in headerMap:
                        
                                for headerKey in headerMap[termKey]:
                                
                                        self.addRow('annotation_to_header', (
                                                newAnnotationKey,
                                                headerKey
                                        ))
                
                
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
                
                cols = self.results[-1][0]
                evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
                annotTypeCol = Gatherer.columnNumber (cols, 'annottype')
                annotKeyCol = Gatherer.columnNumber (cols, '_annot_key')
                termKeyCol = Gatherer.columnNumber (cols, '_term_key')
                vocabKeyCol = Gatherer.columnNumber (cols, '_vocab_key')
                
                for rows in self.annotGroupsRows:
                
                        repRow = rows[0]
                        
                        newAnnotationKey = self.evidenceKeyToNew[repRow[evidenceKeyCol]]
                        annotType = repRow[annotTypeCol]
                        annotKey = repRow[annotKeyCol]
                        termKey = repRow[termKeyCol]
                        vocab = VOCAB_LOOKUP.get(repRow[vocabKeyCol])
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
                                        
                        self.addRow('annotation_sequence_num', (
                                newAnnotationKey,
                                byDagStructure,
                                byTermAlpha,
                                byVocab,
                                byAnnotType,
                                byVocabDagTerm,
                                byObjectDagTerm,
                                byIsoform
                        ))
                
        
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
                
        def postscript (self):
                # Purpose: Now that we've handled all the annotations and mapped them to new keys
                #       (cached in annotKeyMapper), we can produce the contents of the 'annotation_source' table.
                # Returns: nothing
                # Effects: queries the database for the source (genotype-level) annotation keys of MP & DO
                #       annotations that are rolled up to markers.  Converts them to their equivalent front-end
                #       keys and builds the data file for the 'annotation_source' table.
                
                logger.info('Entering postscript() method with %d key mappings' % annotKeyMapper.size())
                
                # first find the range of annotation keys that have source annotation properties defined
                minMaxCmd = '''select min(a._Annot_key) as minKey, max(a._Annot_key) as maxKey
                        from voc_annot a, voc_evidence e, voc_evidence_property vep, voc_term p
                        where a._Annot_key = e._Annot_key
                                and e._AnnotEvidence_key = vep._AnnotEvidence_key
                                and vep._PropertyTerm_key = p._Term_key
                                and p.term = '_SourceAnnot_key' '''

                (cols, rows) = dbAgnostic.execute(minMaxCmd)
                
                startAnnotKey = rows[0][dbAgnostic.columnNumber(cols, 'minKey')]
                maxAnnotKey = rows[0][dbAgnostic.columnNumber(cols, 'maxKey')]
                
                logger.info('Processing source annotations for keys %d..%d' % (startAnnotKey, maxAnnotKey))
                
                # Now step through chunks of annotations in that defined range, finding the source annotation(s)
                # for each derived annotation, converting the annotation keys to their front-end db equivalents,
                # and building the output data file. 
                
                cmd = '''select a._Annot_key, vep.value::integer as sourceAnnotKey
                        from voc_evidence_property vep, voc_term p, voc_annot a, voc_evidence e
                        where a._Annot_key = e._Annot_key
                                and e._AnnotEvidence_key = vep._AnnotEvidence_key
                                and vep._PropertyTerm_key = p._Term_key
                                and p.term = '_SourceAnnot_key'
                                and a._Annot_key >= %d
                                and a._Annot_key < %d
                        order by 1, 2'''
                
                rowCount = 0
                while startAnnotKey <= maxAnnotKey:
                        endAnnotKey = startAnnotKey + self.chunkSize
                        
                        (cols, rows) = dbAgnostic.execute(cmd % (startAnnotKey, endAnnotKey))
                        
                        annotKeyCol = dbAgnostic.columnNumber(cols, '_Annot_key')
                        sourceAnnotKeyCol = dbAgnostic.columnNumber(cols, 'sourceAnnotKey')
                        
                        for row in rows:
                                feSourceAnnotKeys = annotKeyMapper.getFeAnnotKeys(row[sourceAnnotKeyCol])
                        
                                for annotKey in annotKeyMapper.getFeAnnotKeys(row[annotKeyCol]):
                                        for sourceKey in feSourceAnnotKeys:
                                                self.addRow('annotation_source', (annotKey, sourceKey) )
                                                rowCount = rowCount + len(feSourceAnnotKeys)

                        startAnnotKey = endAnnotKey
                
                logger.info('Produced %d source rows' % rowCount)
                return

###--- functions ---###

def initialize():
        # create a temp table that contains the annotation keys (properly ordered for iteration) and a
        # generated key for each.  Updates global MAX_ROW_NUM to show highest value of generated key.
        global MAX_ROW_NUM
        
        logger.debug('Building temp table of annotations')

        cmd1 = '''select row_number() over (
                        order by va._annottype_key, va._object_key, va._term_key, va._Annot_key) as row_num,
                        va._Annot_key
                into temp %s
                from voc_annot va
                order by va._annottype_key, va._object_key, va._term_key, va._Annot_key''' % TEMP_TABLE

        cmd2 = '''create unique index tempIndex1 on %s (row_num)''' % TEMP_TABLE
        cmd3 = '''cluster %s using tempIndex1''' % TEMP_TABLE
        cmd4 = '''create index tempIndex2 on %s (_annot_key)''' % TEMP_TABLE
        
        for (cmd, msg) in [ (cmd1, 'built table'), (cmd2, 'indexed row_num'),
                        (cmd3, 'clustered data'), (cmd4, 'indexed _Annot_key') ]:
                dbAgnostic.execute(cmd)
                logger.debug(' - %s' % msg)
                
        (cols, rows) = dbAgnostic.execute('select max(row_num) from %s' % TEMP_TABLE)
        MAX_ROW_NUM = rows[0][0]
        
        logger.debug(' - included %d annotations' % MAX_ROW_NUM)
        return
        
###--- globals ---###

cmds = [
        #
        # 0 - 2. setup a temp table for each batch.
        #
        '''drop table if exists %s''' % BATCH_TEMP_TABLE,

        '''select row_num, _annot_key
                into temp %s
                from %s
                where row_num > %%d and row_num <= %%d
                order by 1''' % (BATCH_TEMP_TABLE, TEMP_TABLE),

        '''create index annotation_batch_tmp_idx1 on %s (_annot_key)''' % BATCH_TEMP_TABLE,
        '''create index annotation_batch_tmp_idx2 on %s (row_num)''' % BATCH_TEMP_TABLE,
        
        # 3. Base Annotation/Evidence data
        #
        #       All other information is considered secondary and is
        #       gathered at runtime for the specific sub-tables that
        #       require secondary information (e.g. evidence properties & inferredfrom IDs)
        #
        '''select va._annot_key, 
                        vat.name as annottype, 
                        va._object_key, 
                        term._term_key,
                        term.term, 
                        term_acc.accid as term_id,
                        va._qualifier_key,
                        ve._annotevidence_key, 
                        ve._evidenceterm_key,
                        ve._refs_key, 
                        ve.inferredfrom,
                        term._vocab_key,
                        dn._dag_key,
                        vat._mgitype_key,
                        btt.row_num
                from %s btt
                inner join voc_annot va on (btt._Annot_key = va._Annot_key)
                inner join voc_evidence ve on (ve._annot_key = va._annot_key)
                inner join voc_annottype vat on (vat._annottype_key = va._annottype_key)
                inner join voc_term term on     (term._term_key = va._term_key)
                left outer join acc_accession term_acc on (term_acc._object_key = va._term_key
                        and term_acc._mgitype_key = 13
                        and term_acc.preferred = 1
                        and term_acc.private = 0)
                left outer join dag_node dn on (dn._object_key = va._term_key)
                order by btt.row_num, ve._AnnotEvidence_key
        ''' % BATCH_TEMP_TABLE,
        ]

# definition of output files, each as:
#       (filename prefix, list of fieldnames, table name)
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

        ('annotation_source',
                ['annotation_key', 'source_annotation_key' ],
                [Gatherer.AUTO, 'annotation_key', 'source_annotation_key' ]
        ),
                
        ]

# create the necessary temp table
initialize()

# global instance of a AnnotationGatherer
gatherer = AnnotationGatherer (files, cmds)

# voc_annot is sparsely populated, so we use limit/offset instead
#       of the traditional min(key) - max(key) approach
gatherer.setupChunking(
        minKeyQuery = '''select %d''' % MIN_ROW_NUM,
        maxKeyQuery = '''select %d''' % MAX_ROW_NUM,
        chunkSize = 250000                              
        )

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
