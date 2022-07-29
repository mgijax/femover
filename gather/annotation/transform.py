#
# Module to handle data transformations in the annotation_gatherer
#
# *** ALL methods in this module are unit-tested ***
#

import dbAgnostic
import GOFilter
from . import constants as C
import Lookup

EVIDENCE_CODE_LOOKUP = Lookup.Lookup('voc_term', '_Term_key', 'abbreviation')
QUALIFIER_LOOKUP = Lookup.Lookup('voc_term', '_Term_key', 'term')
        
def transformAnnotationType(cols, rows):
    """
    Transform annotation type column
        for every row in rows
    """
    
    annotTypeCol = dbAgnostic.columnNumber( cols, 'annottype')
    
    for row in rows:
        
        annotType = row[annotTypeCol]
    
        if annotType == 'Mammalian Phenotype/Marker (Derived)':
            row[annotTypeCol] = 'Mammalian Phenotype/Marker'
            
        elif annotType == 'DO/Marker (Derived)':
            row[annotTypeCol] = 'DO/Marker'
            
        elif annotType == 'OMIM/Marker (Derived)':
            row[annotTypeCol] = 'OMIM/Marker'

def removeGONoDataAnnotations(cols, rows):
    """
    Filter out GO annotations with 'ND' evidence code that
        have other data associated to that DAG (e.g. Process, Component, Function)
    """
    
    annotKeyCol = dbAgnostic.columnNumber( cols, '_annot_key')
    annotTypeCol = dbAgnostic.columnNumber( cols, 'annottype')
    
    def filter(row):
        
        if row[annotTypeCol] == "GO/Marker":
            return GOFilter.shouldInclude(row[annotKeyCol])
        
        return True
    
    # modify list in place
    rows[:] = [row for row in rows if filter(row)]
            
            
def groupAnnotations(cols, rows,
                        propertiesMap={}):
        """
        Groups rows of annotation records.  Assumes these fields are present:
            _AnnotEvidence_key, annotType, _Term_key, _Object_key,
            _Qualifier_key, _EvidenceTerm_key, inferredFrom
        Other fields can be in the records, too, but those seven must be there.
        
        Returns map of {newKey: [grouped rows]}
        """
        evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
        annotTypeCol = dbAgnostic.columnNumber (cols, 'annottype')
        termKeyCol = dbAgnostic.columnNumber (cols, '_term_key')
        objectKeyCol = dbAgnostic.columnNumber (cols, '_object_key')
        qualifierKeyCol = dbAgnostic.columnNumber (cols, '_qualifier_key')
        evidenceTermKeyCol = dbAgnostic.columnNumber (cols, '_evidenceterm_key')
        inferredfromCol = dbAgnostic.columnNumber (cols, 'inferredfrom')
        refsKeyCol = dbAgnostic.columnNumber (cols, '_refs_key')
        
        groupMap = {}
        for row in rows:
            evidenceKey = row[evidenceKeyCol]
            annotType = row[annotTypeCol]
            objectKey = row[objectKeyCol]
            termKey = row[termKeyCol]
            qualifier = QUALIFIER_LOOKUP.get(row[qualifierKeyCol])
            evidenceCode = EVIDENCE_CODE_LOOKUP.get(row[evidenceTermKeyCol])
            inferredfrom = row[inferredfromCol]
            refsKey = row[refsKeyCol]
            
            properties = []
            if evidenceKey in propertiesMap:
                properties = propertiesMap[evidenceKey]
            
            uniqueFactor = (objectKey, termKey, evidenceCode, inferredfrom)
            
            if annotType == C.GO_ANNOTTYPE:
                
                # incorporate the evidence properties
                propKey = []
                for prop in properties:
                    propKey.append((prop['property'],prop['value']))
                propKey = tuple(propKey)
                
                uniqueFactor = (objectKey, 
                            termKey, 
                            qualifier,
                            evidenceCode,
                            inferredfrom,
                            #propKey
                            refsKey,
                            )
                
            elif annotType == C.OMIM_MARKER_TYPE_NAME:
                uniqueFactor = (objectKey, qualifier, termKey, evidenceCode, inferredfrom)
                
            elif annotType == C.MP_GENOTYPE_TYPE:
                uniqueFactor = (objectKey, termKey, qualifier)
            
            elif annotType == C.OMIM_MARKER_TYPE_NAME or annotType == C.DO_MARKER_TYPE_NAME:
                uniqueFactor = (objectKey, qualifier, termKey, evidenceCode, inferredfrom)    
            
            groupMap.setdefault(uniqueFactor, []).append(row)
            
        return groupMap
    

        
def getAggregatedProperties(cols, rows, propertiesMap):
    """
    Return list of properties contained in propertiesMap aggregated
        across all rows of annotation records
        with duplicate stanza groups collapsed
        
        propertiesMap format : { _annotevidence_key: [ {type, property, stanza, sequencenum, value}, ] }
    """
    aggregatedProps = []
    evidenceKeyCol = dbAgnostic.columnNumber (cols, '_annotevidence_key')
    
    seen = set([])
    for row in rows:
        
        evidenceKey = row[evidenceKeyCol]
        if evidenceKey in propertiesMap:
        
            properties = propertiesMap[evidenceKey]
            
            # ignore duplicate stanzas
            stanzas = groupPropertiesByStanza(properties)
            for stanza in stanzas:
                propKey = []
                for property in stanza:
                    propKey.append((property['property'],property['value']))
                    
                propKey = tuple(propKey)
                if propKey in seen:
                    continue
                seen.add(propKey)
            
                aggregatedProps.extend(stanza)
    
    return aggregatedProps


def groupPropertiesByStanza(properties):
    """
    Given properties as [{type, property, stanza, sequencenum, value},...]
    return list of properties for each stanza as [[properties],[properties],...]
    """
    # ensure order by stanza
    properties.sort(key=lambda x : x['stanza'])
    stanzas = []
    curStanza = 0
    curPropsInStanza = []
    for prop in properties:
        
        if curPropsInStanza and curStanza != prop['stanza']:
            stanzas.append(curPropsInStanza)
            curPropsInStanza = []
            
        curPropsInStanza.append(prop)
        curStanza = prop['stanza']
            
    if curPropsInStanza:
        stanzas.append(curPropsInStanza)
    
    return stanzas

    
    
