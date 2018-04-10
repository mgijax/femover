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
    Finds Organisms for inferredfrom IDs.
    Note: Despite my best efforts, this is still terribly slow to process a batch at a time.
        (30 seconds per batch for sequences, 10 for alleles, 45 for markers, etc.)  So, I'm
        going to trade memory for time here and try just loading them all at once.  At this
        point (March 2018), there are about 307,000 IDs that can be mapped to an organism.
        Shouldn't be an unreasonable amount of memory.
    """

    def __init__ (self):
        """
        Initialize all lookups for annotations where inferred-from ID can be mapped to an organism
        """
        
        self.idCache = {}    # maps from ID -> organism key
        self.organismCache = {}    # maps from organism key -> name
        self.buildTempTable()

        # base query for object lookups; fill in organism key field,
        # extra table(s), and extra where clause(s)
        self.baseQuery = '''select distinct eid.accID, %s
            from evidence_id eid,
                acc_accession bb,
                %s
            where eid.accID = bb.accID
                and %s''' 
        
        self.cacheYeast()
        self.cacheSequences()
        self.cacheAlleles()
        self.cacheMarkers()
        self.cacheOrganisms()
        self.dropTempTable()
        
    def buildTempTable(self):
        dbAgnostic.execute('''select distinct aa.accID
            into temp evidence_id
            from voc_annot va,
                voc_evidence ve, 
                acc_accession aa
            where ve._Annot_key = va._Annot_key
                and va._AnnotType_key not in (%s, %s)
                and aa._LogicalDB_key != %s
                and ve._AnnotEvidence_key = aa._Object_key
                and aa._MGIType_key = 25''' % (C.DO_MARKER_TYPE, C.MP_MARKER_TYPE, C.CHEBI_LDB_KEY) )

        dbAgnostic.execute('create unique index on evidence_id (accID)')
        (cols, rows) = dbAgnostic.execute('select count(1) from evidence_id')
        logger.debug('Built evidence_id table with %d rows' % rows[0][0])
        return
    
    def dropTempTable(self):
        dbAgnostic.execute('drop table evidence_id')
        return
    
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
    