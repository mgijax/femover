#
# Handle tooltips (other than organism) for inferred-from IDs.
# Currently handling:
#    1. markers (mostly mouse, some rat/human, a few others)
#    2. alleles (all mouse)
#    3. vocab terms (some GO, mostly InterPro domains)

import logger

import dbAgnostic
import constants as C

class TooltipFinder:
    """
    Finds (non-organism) tooltips for various types of inferred-from IDs
    """

    def __init__ (self, annotBatchTableName):
        """
        Initialize (non-organism) tooltips for annotations where
            _annot_key exists in annotBatchTableName
        """
        
        self.idCache = {}       # maps from ID -> tooltip text
        self.tempTable = annotBatchTableName

        self._cacheMarkers()
        self._cacheAlleles()
        self._cacheTerms()
    

    def _cacheMarkers(self):
        # include MGI and RGD marker IDs (as those uniquely identify a marker)
        # exclude rolled-up MP / OMIM annotations from consideration
        cmd = '''with inferred_from_ids as (select distinct aa.accID, aa._LogicalDB_key
                    from voc_evidence ve, acc_accession aa, voc_annot va, %s t
                    where ve._AnnotEvidence_key = aa._Object_key
                        and aa._MGIType_key = 25
                        and ve._Annot_key = va._Annot_key
                        and va._AnnotType_key not in (1015, 1016)
                        and aa._LogicalDB_key in (1,47)
                        and ve._Annot_key = t._Annot_key
                    )
                select a.accID, m.symbol, m.name, o.commonName
                from inferred_from_ids i, acc_accession a, mrk_marker m, mgi_organism o
                where i.accID = a.accID
                    and i._LogicalDB_key = a._LogicalDB_key
                    and a._MGIType_key = 2
                    and a._Object_key = m._Marker_key
                    and m._Organism_key = o._Organism_key''' % self.tempTable
        
        cols, rows = dbAgnostic.execute(cmd)
        organismCol = dbAgnostic.columnNumber(cols, 'commonName')
        symbolCol = dbAgnostic.columnNumber(cols, 'symbol')
        nameCol = dbAgnostic.columnNumber(cols, 'name')
        idCol = dbAgnostic.columnNumber(cols, 'accID')
        
        for row in rows:
            self.idCache[row[idCol]] = '%s Gene: %s\n%s' % (row[organismCol].capitalize().replace(", laboratory", ""),
                row[symbolCol], row[nameCol])

        logger.debug("Cached %d marker tooltips" % len(rows))
        

    def _cacheAlleles(self):
        cmd = '''with inferred_from_ids as (select distinct aa.accID, aa._LogicalDB_key
                    from voc_evidence ve, acc_accession aa, voc_annot va, %s t
                    where ve._AnnotEvidence_key = aa._Object_key
                        and aa._MGIType_key = 25
                        and ve._Annot_key = va._Annot_key
                        and va._AnnotType_key not in (1015, 1016)
                        and aa._LogicalDB_key in (1,47)
                        and ve._Annot_key = t._Annot_key
                    )
                    select a.accID, m.symbol as marker_symbol, aa.symbol as allele_symbol, aa.name as allele_name
                    from inferred_from_ids i, acc_accession a, all_allele aa, mrk_marker m
                    where i.accID = a.accID
                        and i._LogicalDB_key = a._LogicalDB_key
                        and a._MGIType_key = 11
                        and a._Object_key = aa._Allele_key
                        and aa._Marker_key = m._Marker_key''' % self.tempTable
        
        cols, rows = dbAgnostic.execute(cmd)
        alleleSymbolCol = dbAgnostic.columnNumber(cols, 'allele_symbol')
        markerSymbolCol = dbAgnostic.columnNumber(cols, 'marker_symbol')
        nameCol = dbAgnostic.columnNumber(cols, 'allele_name')
        idCol = dbAgnostic.columnNumber(cols, 'accID')
        
        for row in rows:
            self.idCache[row[idCol]] = 'Allele: %s\n%s\nGene: %s' % (row[alleleSymbolCol], row[nameCol], row[markerSymbolCol])

        logger.debug("Cached %d allele tooltips" % len(rows))


    def _cacheTerms(self):
        # allow special case where evidence GO IDs use logical db 1 (MGI) and vocab term GO IDs use 31
        cmd = '''with inferred_from_ids as (select distinct aa.accID, aa._LogicalDB_key
                    from voc_evidence ve, acc_accession aa, voc_annot va, %s t
                    where ve._AnnotEvidence_key = aa._Object_key
                        and aa._MGIType_key = 25
                        and ve._Annot_key = va._Annot_key
                        and va._AnnotType_key not in (1015, 1016)
                        and aa._LogicalDB_key in (1,28,31)
                        and ve._Annot_key = t._Annot_key
                    )
                    select a.accID, t.term, v.name
                    from inferred_from_ids i, acc_accession a, voc_term t, voc_vocab v
                    where i.accID = a.accID
                        and (i._LogicalDB_key = a._LogicalDB_key or a._LogicalDB_key = 31)
                        and a._MGIType_key = 13
                        and a._Object_key = t._Term_key
                        and t._Vocab_key = v._Vocab_key''' % self.tempTable


        cols, rows = dbAgnostic.execute(cmd)
        vocabCol = dbAgnostic.columnNumber(cols, 'name')
        termCol = dbAgnostic.columnNumber(cols, 'term')
        idCol = dbAgnostic.columnNumber(cols, 'accID')
        
        for row in rows:
            self.idCache[row[idCol]] = '%s (%s)' % (row[termCol], row[vocabCol])

        logger.debug("Cached %d term tooltips" % len(rows))


    def getTooltip(self, accID):
        if self.idCache.has_key(accID):
            return self.idCache[accID]