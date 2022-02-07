# Name: GOAnnotations.py
# Purpose: provides utilities for dealing with GO annotations, how to count
#   them, etc.

import dbAgnostic
import logger
import go_annot_extensions
import go_isoforms

GO_EXTENSION_NOTETYPE_KEY = 1045    # GO Annotation Extension (Property) display _notetype_key
GO_ISOFORM_NOTETYPE_KEY = 1046      # GO Isoform (Property) display _notetype_key

def getContextOfAnnotations(
    tableName,              # name of table (temp or otherwise) that has the annotations to process
    propertiesMap = {}      # property map to populate
    ):
    # Purpose: get a mapping from evidence keys to a list of context data for each
    # Returns: updated version of 'propertiesMap'
    # Effects: adds data to the given 'propertiesMap'
    
    logger.debug('In getContextOfAnnotations()')
    logger.debug(' - len(propertiesMap) = %d' % len(propertiesMap))

    extensionProcessor = go_annot_extensions.Processor()

    validPropertyKeys = extensionProcessor.querySanctionedPropertyTermKeys()
    logger.debug(' - got %d property keys' % len(validPropertyKeys))

    validEvidenceKeys = extensionProcessor.querySanctionedEvidenceTermKeys()
    logger.debug(' - got %d evidence keys' % len(validEvidenceKeys))

    propertyKeyClause = ",".join([str(k) for k in validPropertyKeys])
    evidenceKeyClause = ",".join([str(k) for k in validEvidenceKeys])
    
    cmd = '''select vep._annotevidence_key,
            prop.term as property,
            vep.stanza,
            vep.sequencenum,
            vep.value,
            n.note as display_value
        from voc_evidence ve
        join %s abt on (abt._annot_key = ve._annot_key
            and abt._AnnotType_key = 1000)
        join voc_evidence_property vep on (vep._annotevidence_key = ve._annotevidence_key)
        join voc_term prop on (prop._term_key = vep._propertyterm_key)
        left outer join mgi_note n on (n._object_key = vep._evidenceproperty_key) and n._notetype_key = %d
        where ve._evidenceterm_key in (%s)
                    and vep._propertyterm_key in (%s)
        order by ve._annot_key, ve._refs_key, vep.stanza, vep.sequencenum''' % (
                        tableName,
                        GO_EXTENSION_NOTETYPE_KEY,
                        evidenceKeyClause, 
                        propertyKeyClause)

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
            'type' : 'Annotation Extension',
            'property' : property,
            'stanza' : stanza,
            'sequencenum' : seqnum,
            'value' : displayValue,
            }
        propertiesMap.setdefault(evidenceKey, []).append(propObj)
        
    logger.debug(' - processed %d context rows' % len(rows))
    return propertiesMap

def getIsoformsOfAnnotations(
    tableName,              # name of table (temp or otherwise) that has the annotations to process
    propertiesMap = {}      # property map to populate
    ):
    # Purpose: get a mapping from evidence keys to a list of isoform data for each
    # Returns: updated version of 'propertiesMap'
    # Effects: adds data to the given 'propertiesMap'

    logger.debug('In getIsoformsOfAnnotations()')
    logger.debug(' - len(propertiesMap) = %d' % len(propertiesMap))

        # query the correct keys to use for GO isoform properties
    isoformProcessor = go_isoforms.Processor()

    validPropertyKeys = isoformProcessor.querySanctionedPropertyTermKeys()
    logger.debug(' - got %d property keys' % len(validPropertyKeys))

    propertyKeyClause = ",".join([str(k) for k in validPropertyKeys])

    cmd = '''select vep._annotevidence_key,
                prop.term as property,
                        vep.stanza,
                        vep.sequencenum,
                        vep.value,
                        n.note displayValue
                from voc_evidence ve
                join %s abt on (abt._annot_key = ve._annot_key
                    and abt._AnnotType_key = 1000)
                join voc_evidence_property vep on (vep._annotevidence_key = ve._annotevidence_key)
                join voc_term prop on (prop._term_key = vep._propertyterm_key)
                join mgi_note n on (n._object_key = vep._evidenceproperty_key
                    and n._notetype_key = %s)
                where vep._propertyterm_key in (%s)
                order by ve._annot_key, ve._refs_key, vep.stanza, vep.sequencenum
                ''' % (tableName,
                        GO_ISOFORM_NOTETYPE_KEY,
                        propertyKeyClause)
        
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
                        'type' : 'Isoform',
                        'property' : property,
                        'stanza' : stanza,
                        'sequencenum' : seqnum,
                        'value' : displayValue,
                }
                propertiesMap.setdefault(evidenceKey, []).append(propObj)

    logger.debug(' - processed %d isoform rows' % len(rows))
    return propertiesMap
