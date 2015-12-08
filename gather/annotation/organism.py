#
# Handle inferredfrom organisms
#
#

import logger

import utils
import dbAgnostic
import constants as C

class OrganismFinder:
    """
    Finds Organisms for inferredfrom IDs
    """

    def __init__ (self, annotBatchTableName):
        """
        Initialize all lookups for annotations where
            _annot_key exists in annotBatchTableName
        """
        
        self.idCache = {}    # maps from ID -> organism key
        self.organismCache = {}    # maps from organism key -> name

        # base query for object lookups; fill in organism key field,
        # extra table(s), and extra where clause(s)
        self.baseQuery = '''select distinct aa.accID, %%s
            from voc_evidence ve,
                %s abt,
                acc_accession aa,
                voc_annot va,
                acc_accession bb,
                %%s
            where ve._AnnotEvidence_key = aa._Object_key
                and aa._MGIType_key = 25
                and ve._Annot_key = va._Annot_key
                and va._AnnotType_key not in (%s, %s)
                and aa.accID = bb.accID
                and aa._LogicalDB_key != %d
                and abt._annot_key = va._annot_key
                and %%s
        ''' % ( annotBatchTableName, C.OMIM_MARKER_TYPE, C.MP_MARKER_TYPE, \
                C.CHEBI_LDB_KEY)
        
        self.cacheYeast()
        self.cacheSequences()
        self.cacheAlleles()
        self.cacheMarkers()
        self.cacheOrganisms()
        

    def cacheGeneric(self, cmd, objectType):
        (cols, rows) = dbAgnostic.execute(cmd)

        idCol = dbAgnostic.columnNumber (cols, 'accID')
        organismCol = dbAgnostic.columnNumber (cols, '_Organism_key')

        for row in rows:
            self.idCache[row[idCol]] = row[organismCol]
        logger.debug('Cached organisms for %d %s IDs' % (
            len(rows), objectType))


    def cacheMarkers(self):
        cmd = self.baseQuery % ('m._Organism_key',
            'mrk_marker m',
            'bb._MGIType_key = 2 ' + \
                'and bb._Object_key = m._Marker_key')
        self.cacheGeneric(cmd, 'marker')
        

    def cacheAlleles(self):
        cmd = self.baseQuery % ('m._Organism_key',
            'all_allele a, mrk_marker m',
            'bb._MGIType_key = 11 ' + \
                'and bb._Object_key = a._Allele_key ' + \
                'and a._Marker_key = m._Marker_key')
        self.cacheGeneric(cmd, 'allele')


    def cacheSequences(self):
        cmd = self.baseQuery % ('s._Organism_key',
            'seq_sequence s',
            'bb._MGIType_key = 19 ' + \
                'and bb._Object_key = s._Sequence_key')
        self.cacheGeneric(cmd, 'sequence')
        

    def cacheYeast(self):
        cmd = '''select distinct accID from acc_accession
            where _MGIType_key = 25 and _LogicalDB_key = %d'''

        for (name, ldb) in [ (C.BUDDING_YEAST_NAME, C.BUDDING_YEAST_LDB_KEY),
                    (C.FISSION_YEAST_NAME, C.FISSION_YEAST_LDB_KEY) ]:
            (cols, rows) = dbAgnostic.execute(cmd % ldb)

            idCol = dbAgnostic.columnNumber (cols, 'accID')

            for row in rows:
                self.idCache[row[idCol]] = name

            self.organismCache[name] = name

            logger.debug('Cached organisms for %d %s IDs' % (
                len(rows), name) )


    def cacheOrganisms(self):
        cmd = 'select _Organism_key, commonName from MGI_Organism'
        (cols, rows) = dbAgnostic.execute(cmd)

        keyCol = dbAgnostic.columnNumber (cols, '_Organism_key')
        nameCol = dbAgnostic.columnNumber (cols, 'commonName')

        for row in rows:
            self.organismCache[row[keyCol]] = \
                utils.cleanupOrganism(row[nameCol])
        logger.debug('Cached %d organisms' % len(self.organismCache))
        

    def getOrganism(self, accID):
        if self.idCache.has_key(accID):
            return self.organismCache[self.idCache[accID]]
    
    