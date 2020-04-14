# Module: ReferenceUtils.py
# Purpose: to provide handy utility functions for dealing with reference data

import dbAgnostic
import logger
import gc
import MarkerUtils

###--- globals ---###

# refs key -> jnum ID
jnumCache = {}

# marker key -> dictionary of { reference key : 1 } for disease-relevant refs
diseaseRefs = {}

###--- private functions ---###

def _populateJnumCache(tableName = None):
        # populate the global 'jnumCache' with J: number for each refs key

        global jnumCache

        if tableName != None:
                whereClause = '''and exists (select 1 from %s t
                        where r._Object_key = t._Refs_key)''' % tableName
                tableMessage = tableName
        else:
                whereClause = ''
                tableMessage = '(all)'

        cmd = '''select r._Object_key as _Refs_key, r.accID as jnumID
                from acc_accession r
                where r._MGIType_key = 1
                        and r.prefixPart = 'J:'
                        and r._LogicalDB_key = 1
                        and r.preferred = 1
                %s''' % whereClause

        (cols, rows) = dbAgnostic.execute(cmd)

        keyCol = dbAgnostic.columnNumber(cols, '_Refs_key')
        jnumCol = dbAgnostic.columnNumber(cols, 'jnumID')

        for row in rows:
                jnumCache[row[keyCol]] = row[jnumCol]


        logger.debug ('Cached %d J: numbers for table %s' % (len(jnumCache),
                tableMessage))
        return

def _populateDiseaseRelevantReferenceCache():
        # populate the global 'diseaseRefs' cache, associating markers with
        # their set of disease-relevant references

        global diseaseRefs

        diseaseRefs = {}

        # get the name of a temp table that maps from a marker to its
        # (traditional) alleles, as well as any alleles that express it (via
        # "expresses component" relationshps) or that are associated with it
        # through "mutation involves" relationships

        markerAlleleTable = MarkerUtils.getMarkerAlleleTable(
                [ MarkerUtils.ec, MarkerUtils.mi ], 'pairs_ec_mi')

        # query returns all references for disease annotations that have been
        # made to genotypes (annotation type 1020) which can be tied to a
        # marker though the allele/marker relationships in the temp table.

        cmdMain = '''select distinct ma._Marker_key, e._Refs_key
                from voc_annot a,
                        voc_evidence e,
                        gxd_allelegenotype gag,
                        %s ma
                where a._AnnotType_key = 1020
                        and a._Annot_key = e._Annot_key
                        and a._Object_key = gag._Genotype_key
                        and gag._Allele_key = ma._Allele_key''' % \
                                markerAlleleTable

        # query returns all references which directly tie a disease to an
        # allele (bypassing a genotype, annotation type 1012), where those
        # genotypes include alleles that can be tied to markers via the set of
        # relationships in the temp table.

        cmdSpecial = '''select distinct aa._Marker_key,
                        e._Refs_key
                from voc_annot a,
                        voc_evidence e,
                        %s aa
                where a._AnnotType_key = 1012
                        and a._Annot_key = e._Annot_key
                        and a._Object_key = aa._Allele_key''' % \
                                markerAlleleTable

        for cmd in [ cmdMain, cmdSpecial ]:
                (cols, rows) = dbAgnostic.execute(cmd)

                markerCol = dbAgnostic.columnNumber(cols, '_Marker_key')
                refsCol = dbAgnostic.columnNumber(cols, '_Refs_key')

                for row in rows:
                        markerKey = row[markerCol]
                        refsKey = row[refsCol]

                        if markerKey in diseaseRefs:
                                diseaseRefs[markerKey][refsKey] = 1
                        else:
                                diseaseRefs[markerKey] = { refsKey : 1 }

                logger.debug('Processed %d disease reference associations' % \
                        len(rows))

        logger.debug('Got disease refs for %d markers' % len(diseaseRefs))
        return

###--- functions dealing with reference data ---###

def restrict(tableName):
        # if you'd like to restrict J: number collection to only those cited
        # in a certain table, call this first and specify the table name
        # (to save memory)

        _populateJnumCache(tableName)
        return

def getJnumID(refsKey):
        # get J: number ID for the given refs key

        if len(jnumCache) == 0:
                _populateJnumCache()

        if refsKey in jnumCache:
                return jnumCache[refsKey]

        return None

def getDiseaseRelevantReferences(markerKey):
        # get a list of reference keys where those references are considered
        # disease-relevant for the given marker

        if len(diseaseRefs) == 0:
                _populateDiseaseRelevantReferenceCache()

        if markerKey in diseaseRefs:
                refs = list(diseaseRefs[markerKey].keys())
                refs.sort()
                return refs

        return []

def getMarkersWithDiseaseRelevantReferences():
        # get a list of marker keys for markers with disease-relevant
        # references

        if len(diseaseRefs) == 0:
                _populateDiseaseRelevantReferenceCache()

        markers = list(diseaseRefs.keys())
        markers.sort()

        return markers

def unload():
        global diseaseRefs, jnumCache

        diseaseRefs = {}
        jnumCache = {}

        gc.collect()
        return
