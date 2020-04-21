#!./python
# 
# gathers data for the 'batch_marker_terms' table in the front-end database

import os
import Gatherer
import logger
import config
import dbAgnostic
import gc

###--- Globals ---###

mouseMarkerTable = 'tmp_markers'
seqIDTable = 'tmp_seq_id'
snpIDTable = 'tmp_snp_id'
otherIDTable = 'tmp_other_id'

ldbCache = None                 # dictionary mapping from _LogicalDB_key to name

###--- Functions ---###

def getLogicalDB(ldbKey):
        # return the string logical database key for the given key, populating the global cache if needed
        global ldbCache
        
        if ldbCache == None:
                ldbCache = {}
                cmd = '''select _LogicalDB_key, case
                                when _LogicalDB_key = 9 then 'Genbank'
                                else name
                                end as name
                        from acc_logicaldb'''
                
                cols, rows = dbAgnostic.execute(cmd)
                keyCol = Gatherer.columnNumber(cols, '_LogicalDB_key')
                nameCol = Gatherer.columnNumber(cols, 'name')
                
                for row in rows:
                        ldbCache[row[keyCol]] = row[nameCol]
                logger.debug('Cached %d logical dbs' % len(ldbCache))

                ldbCache[-999] = 'RefSNP'               # special value

        return ldbCache[ldbKey]

def createMarkerTable():
        # create a temp table with only those markers we want to be able to return
        
        cmd0 = '''select row_number() over () as row_num, _Marker_key
                into temp table %s
                from mrk_marker
                where _Organism_key = 1
                        and _Marker_Status_key = 1
                order by _Marker_key''' % mouseMarkerTable
        cmd1 = 'create unique index mm1 on %s(_Marker_key)' % mouseMarkerTable
        cmd2 = 'create unique index mm2 on %s(row_num)' % mouseMarkerTable
        
        logger.debug('Creating %s' % mouseMarkerTable)
        for cmd in [ cmd0, cmd1, cmd2 ]:
                dbAgnostic.execute(cmd)
        logger.debug(' - done')
        return 
        
def createSeqIDTable():
        # create a temp table with seq IDs for each marker
        
        allSeqTable = 'allSeqIDs'
        
        cmd0 = '''select _Object_key as _Sequence_key, _LogicalDB_key, accID
                into temp table %s
                from acc_accession
                where _MGIType_key = 19
                        and private = 0
                        and _LogicalDB_key in (9, 13, 27, 41)''' % allSeqTable
        
        cmd1 = 'create index tmpIndex1 on %s (_Sequence_key)' % allSeqTable
        
        logger.debug('Creating %s' % allSeqTable)
        dbAgnostic.execute(cmd0)
        logger.debug(' - done')
        dbAgnostic.execute(cmd1)
        logger.debug(' - indexed')
        
        tempTable2 = '''select a.accID, a._LogicalDB_key, m._Marker_key
                into temp table %s
                from seq_marker_cache s,
                        %s m,
                        %s a
                where s._Marker_key = m._Marker_key
                        and s._Sequence_key = a._Sequence_key
                order by m._Marker_key''' % (seqIDTable, mouseMarkerTable, allSeqTable)

        tempIdx2 = 'create index idx_seq_mrk_key on %s (_Marker_key)' % seqIDTable
        analyzeTable2 = 'analyze %s' % seqIDTable

        logger.debug('Creating %s' % seqIDTable)
        dbAgnostic.execute(tempTable2)
        logger.debug(' - done')
        dbAgnostic.execute(tempIdx2)
        logger.debug(' - indexed')
        dbAgnostic.execute(analyzeTable2)
        logger.debug(' - analyzed')
        
        dbAgnostic.execute('drop table %s' % allSeqTable)
        logger.debug('Dropped %s' % allSeqTable)
        return

def createOtherIDTable():
        # create a temp table with other (non-seq) IDs for each marker
        
        cmd0 = '''select _Object_key as _Marker_key, _LogicalDB_key, accID
                into temp table %s
                from acc_accession
                where _MGIType_key = 2
                        and private = 0
                        and _LogicalDB_key not in (9, 13, 27, 41)''' % otherIDTable
        
        cmd1 = 'create index aoiIndex1 on %s (_Marker_key)' % otherIDTable
        
        logger.debug('Creating %s' % otherIDTable)
        dbAgnostic.execute(cmd0)
        logger.debug(' - done')
        dbAgnostic.execute(cmd1)
        logger.debug(' - indexed')
        return

def createSnpTable():
        # create a temp table for SNP IDs for each marker
        
        # build temp table of only SNPs within 2kb of a marker
        tmpSnp1 = 'tmp_snp_marker'
        cmd0 = '''select distinct m._ConsensusSNP_key, m._Marker_key
                into temp table %s
                from snp_consensussnp_marker m, %s t
                where t._Marker_key = m._Marker_key
                        and m.distance_from <= 2000''' % (tmpSnp1, mouseMarkerTable)

        cmd1 = 'create index tsmIndex1 on %s (_ConsensusSNP_key)' % tmpSnp1
        cmd2 = 'create index tsmIndex2 on %s (_Marker_key)' % tmpSnp1
        
        logger.debug('Creating %s' % tmpSnp1)
        dbAgnostic.execute(cmd0)
        logger.debug(' - done')
        dbAgnostic.execute(cmd1)
        dbAgnostic.execute(cmd2)
        logger.debug(' - indexed')
        
        # build temp table of only SNPs with a single coord and with the right variation class
        tmpSnp2 = 'tmp_snp_selected'
        cmd3 = '''select distinct t._ConsensusSNP_key
                into temp table %s
                from snp_coord_cache c, snp_consensussnp s, %s t
                where t._ConsensusSNP_key = c._ConsensusSNP_key
                        and t._ConsensusSNP_key = s._ConsensusSNP_key
                        and c.isMultiCoord = 0
                        and s._VarClass_key = 1878510''' % (tmpSnp2, tmpSnp1)

        cmd4 = 'create unique index tssIndex1 on %s (_ConsensusSNP_key)' % tmpSnp2
                        
        logger.debug('Creating %s' % tmpSnp2)
        dbAgnostic.execute(cmd3)
        logger.debug(' - done')
        dbAgnostic.execute(cmd4)
        logger.debug(' - indexed')

        cmd7='''select a.accid,
                        m._Marker_key
                into temp table %s
                from %s m, snp_accession a, %s c
                where m._ConsensusSnp_key = a._Object_key
                        and c._ConsensusSNP_key = m._ConsensusSNP_key
                        and a._MGIType_key = 30
                order by m._Marker_key''' % (snpIDTable, tmpSnp1, tmpSnp2)
        
        cmd8='create index idx_snp_mrk_key on %s (_Marker_key)' % snpIDTable
        cmd9 = 'analyze %s' % snpIDTable

        logger.debug('Creating %s' % snpIDTable)
        dbAgnostic.execute(cmd7)
        logger.debug(' - done')
        dbAgnostic.execute(cmd8)
        logger.debug(' - indexed')
        dbAgnostic.execute(cmd9)
        logger.debug(' - analyzed')
        
        dbAgnostic.execute('drop table %s' % tmpSnp1)
        logger.debug('Dropped %s' % tmpSnp1)
        dbAgnostic.execute('drop table %s' % tmpSnp2)
        logger.debug('Dropped %s' % tmpSnp2)
        return

def createTempTables():
        # create the three major temp tables (named in global constants at top)
        createMarkerTable()
        createOtherIDTable()
        createSeqIDTable()
        createSnpTable()
        return
        
def loadGOCaches():
        logger.debug('Loading GO Caches')
        ids={}
        ancestors={}

        # get the list of GO term IDs and keys
        idQuery = '''select _Object_key, accID
                from acc_accession
                where _LogicalDB_key = 31
                        and private = 0
                        and _MGIType_key = 13'''

        # get the GO DAG, mapping a term to all of its subterms
        dagQuery = '''select c._AncestorObject_key, c._DescendentObject_key
                from dag_closure c,
                        voc_term t
                where c._MGIType_key = 13
                        and c._AncestorObject_key = t._Term_key
                        and t._Vocab_key = 4'''

        # gather GO IDs for each term key

        (cols, rows) = dbAgnostic.execute(idQuery)
        termCol = Gatherer.columnNumber (cols, '_Object_key')
        idCol = Gatherer.columnNumber (cols, 'accID')

        ids = {}        # ids[term key] = [list of IDs for that term]

        for row in rows:
                term = row[termCol]
                if term in ids:
                        ids[term].append(row[idCol])
                else:
                        ids[term] = [ row[idCol] ]

        logger.debug (' - Collected %d GO IDs for %d terms' % (len(rows), len(ids) ))

        # gather ancestor term keys for each term key

        (cols, rows) = dbAgnostic.execute(dagQuery)
        parentCol = Gatherer.columnNumber (cols, '_AncestorObject_key')
        childCol = Gatherer.columnNumber (cols, '_DescendentObject_key')

        ancestors = {}  # ancestors[term key] = [list of parent keys]

        for row in rows:
                parent = row[parentCol]
                child = row[childCol]
                if child in ancestors:
                        if parent not in ancestors[child]:
                                ancestors[child].append(parent)
                else:
                        ancestors[child] = [ parent ]

        logger.debug (' - Collected %d relationships for %d child GO terms' % (len(rows), len(ancestors) ))
        return ids,ancestors

###--- Classes ---###

class BatchMarkerTermsGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the batch_marker_terms table
        # Has: queries to execute against the source database
        # Does: queries the source database for strings to be searched by the
        #       batch query interface for markers, collates results, writes
        #       tab-delimited text file

        #ids[term key] = [list of IDs for that term]
        #ancestors[term key] = [list of parent keys]
        ids={}
        ancestors={}

        def getMinKeyQuery (self):
                return 'select min(row_num) from %s' % mouseMarkerTable

        def getMaxKeyQuery (self):
                return 'select max(row_num) from %s' % mouseMarkerTable

        def getNomenclatureRows (self):
                # returns a two-item tuple containing:
                #       1. list of nomenclature-based rows with columns as:     [ term, term_type, marker_key, None ]
                #       2. dictionary of valid marker keys

                # nomenclature: for each marker key, a given term should only
                # appear once, with preference to the lowest priority values
                # (priority is handled by the order-by on the query)

                done = {}                               # (marker key, term) -> 1
                validKeys = {}                  # marker key -> 1
                out = []                                # list of output rows

                cols, rows = self.results[0]

                termCol = Gatherer.columnNumber (cols, 'term')
                typeCol = Gatherer.columnNumber (cols, 'term_type')
                keyCol = Gatherer.columnNumber (cols, '_Marker_key')

                for row in rows:
                        pair = (row[keyCol], row[termCol].lower())
                        if pair not in done:
                                out.append ( [ row[termCol], row[typeCol], row[keyCol] ] )
                                done[pair] = 1
                                validKeys[row[keyCol]] = 1

                logger.debug ('Got %d nomen rows' % len(rows))
                return out, validKeys

        def getIDRows(self,
                        validKeys,                      # dictionary with keys as valid marker keys
                        resultIndex,            # integer -- specifies which query results to process
                        dataType                        # name of type of ID being processed (Sequence, Other, SNP)
                        ):
                # returns a list of ID-based rows with columns as: [ term, term_type, marker_key, None ]

                isOtherType = (dataType == 'Other')
                
                out = []                                                                # list of output rows
                cols, rows = self.results[resultIndex]
                
                markerCol = Gatherer.columnNumber(cols, '_Marker_key')
                idCol = Gatherer.columnNumber(cols, 'accID')
                ldbCol = Gatherer.columnNumber(cols, '_LogicalDB_key')
                
                # We need to exclude NCBI Gene Model (and evidence) IDs if we
                # encounter a matching Entrez Gene ID for that marker.

                ncbiExcluded = {}                       # marker key -> [ list of Entrez Gene IDs ]
                done = {}                                       # (id, ldb, marker) -> 1

                for row in rows:
                        accID = row[idCol]
                        ldb = getLogicalDB(row[ldbCol])
                        marker = row[markerCol]

                        if marker not in validKeys:
                                continue

                        triple = (accID, ldb, marker)
                        if triple in done:
                                continue

                        # if processing sequence IDs, then:
                        # eliminate NCBI Gene Model (and evidence) if it duplicates a Entrez Gene ID

                        if isOtherType:
                                if ldb == 'Entrez Gene':
                                        if marker in ncbiExcluded:
                                                ncbiExcluded[marker].append(accID)
                                        else:
                                                ncbiExcluded[marker] = [ accID ]

                        # if we made it this far, add the ID

                        out.append ( [ accID, ldb, marker ] )
                        done[triple] = 1

                logger.debug ('Got %d %s rows' % (len(out), dataType))

                # if processing sequences, need to remove any NCBI Gene Model rows for
                # markers that also have Entrez Gene IDs
                
                if isOtherType:
                        logger.debug (' - found Entrez Gene IDs for %d markers' % len(ncbiExcluded))
                        delCount = 0
                        i = len(out) - 1
                        while i >= 0:
                                [ accID, ldb, marker ] = out[i]
                                
                                if marker in ncbiExcluded:
                                        if ldb.startswith ('NCBI Gene Model'):
                                                if accID in ncbiExcluded[marker]:
                                                        del out[i]
                                                        delCount = delCount + 1
                                                
                                i = i - 1

                        logger.debug(' - removed %d NCBI Gene Model rows' % delCount)

                self.results[resultIndex] = (self.results[resultIndex][0], [])
                del done
                del ncbiExcluded
                gc.collect()
                return out
        
        def getGORows(self, validKeys):
                # returns a list of ID-based rows with columns as: [ term, term_type, marker_key, None ]
                
                out = []                                                                # list of output rows
                cols, rows = self.results[4]

                markerCol = Gatherer.columnNumber (cols, '_Object_key')
                termCol = Gatherer.columnNumber (cols, '_Term_key')
                
                # go through our marker/GO annotations and produce a row in
                # finalResults for each term and rows for its ancestors

                label = 'Gene Ontology (GO)'
                noID = {}       # keys for terms without IDs, as we find them (for reporting)

                # A marker may be annotated to multiple children from the same parent node in the DAG.
                # We do not want to replicate the marker / GO ID relationships for those, so we track
                # those we have already handled.

                done = {}       # done[marker key] = { term key : 1 }

                for row in rows:
                        termKey = row[termCol]
                        markerKey = row[markerCol]

                        if markerKey not in validKeys:
                                continue

                        # include IDs for all of the term's IDs and all of the
                        # IDs for its ancestors

                        termKeys = [ termKey ]
                        if termKey in self.ancestors:
                                termKeys = termKeys + self.ancestors[termKey]

                        for key in termKeys:

                                # if we've already done this marker/term pair,
                                # skip it
                                if markerKey in done:
                                        if key in done[markerKey]:
                                                continue

                                # if this GO term has no IDs, skip it
                                if key not in self.ids:
                                        if key not in noID:
                                            logger.debug ('No ID for GO term key: %s' % key)
                                            noID[key] = 1
                                        continue
                                
                                for id in self.ids[key]:
                                        row = [ id, label, markerKey ]
                                        out.append (row)

                                # remember that we've done this marker/term pair

                                if markerKey in done:
                                        done[markerKey][key] = 1
                                else:
                                        done[markerKey] = { key : 1 }

                logger.debug('Got %d GO rows' % len(out))

                del done
                gc.collect()
                return out
        
        def getStrainMarkerRows (self, validKeys):
                # returns a list of rows for strain marker IDs with columns as:
                #       [ term, term_type, marker_key  ]
                # Note that we are associating the strain marker IDs with their respective
                # canonical markers, not with the strain markers themselves (which are not
                # returned by the batch query tool).
                
                out = []                                                                # list of output rows
                cols, rows = self.results[5]

                markerCol = Gatherer.columnNumber (cols, 'canonical_marker_key')
                idCol = Gatherer.columnNumber (cols, 'accID')
                ldbCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
                
                for row in rows:
                        markerKey = row[markerCol]
                        if markerKey not in validKeys:
                                continue

                        accID = row[idCol]
                        ldb = getLogicalDB(row[ldbCol])
                        out.append( [ accID, ldb, markerKey ] )
                        
                logger.debug('Got %d strain marker ID rows' % len(out))
                return out

        def collateResults (self):
                if not self.ids:
                        self.ids, self.ancestors=loadGOCaches()

                gc.collect()
                logger.debug('Ran garbage collection')

                nomenRows, validKeys = self.getNomenclatureRows()
                otherRows = self.getIDRows(validKeys, 1, 'Other')
                sequenceRows = self.getIDRows(validKeys, 2, 'Sequence')
                snpRows = self.getIDRows(validKeys, 3, 'SNP')
                goRows = self.getGORows(validKeys)
                strainMarkerRows = self.getStrainMarkerRows(validKeys)
                
                self.finalColumns = [ 'term', 'term_type', '_Marker_key' ]
                self.finalResults = nomenRows + sequenceRows + otherRows + goRows + snpRows + strainMarkerRows
                return

###--- globals ---###

cmds = [
        # 0. nomenclature for current mouse markers, including symbol, name,
        # synonyms, old symbols, old names, human synonyms, related
        # synonyms, ortholog symbols, and ortholog names (not allele symbols,
        # not allele names)
        '''select ml.label as term,
                        ml.labelTypeName as term_type,
                        t._Marker_key,
                        ml.priority
                from %s t,
                        mrk_label ml
                where t._Marker_key = ml._Marker_key
                        and ml.labeltypename not in ('allele symbol','allele name')
                        and t.row_num >= %%d
                        and t.row_num < %%d
                order by t._Marker_key, ml.priority''' % mouseMarkerTable,

        # 1. all public accession IDs for current mouse markers (excluding
        # the sequence IDs picked up in a later query from the related
        # sequences).  Will need to order these in code so that Entrez IDs
        # will come before NCBI.
        '''select a.accID,
                        a._LogicalDB_key,
                        a._Marker_key
                from %s a,
                        %s m
                where a._Marker_key = m._Marker_key
                        and m.row_num >= %%d
                        and m.row_num < %%d
                order by a._Marker_key''' % (otherIDTable, mouseMarkerTable),

        # 2. Genbank (9), RefSeq (27), and Uniprot (13 and 41) IDs for
        # sequences associated to current mouse markers.
        '''select s.accID, s._LogicalDB_key, s._Marker_key
                from %s s, %s t
                where s._Marker_key = t._Marker_key
                        and t.row_num >= %%d
                        and t.row_num < %%d''' % (seqIDTable, mouseMarkerTable),

        # 3. RefSNP IDs for RefSNPs that are associated with markers either by
        # dbSNP or are within 2kb (as computed by MGI)
        ''' select s.accID,
                        -999::integer as _LogicalDB_key,
                        s._Marker_key
                from %s s, %s t
                where s._Marker_key = t._Marker_key
                        and t.row_num >= %%d
                        and t.row_num < %%d''' % (snpIDTable, mouseMarkerTable),

        # need to do GO IDs and their descendent terms, so a marker can be
        # retrieved for either its directly annotated terms or any of their
        # ancestor terms for its annotated terms
        
        # 4. get the marker/GO annotations
        '''select a._Object_key,
                        a._Term_key
                from voc_annot a, %s t
                where a._Qualifier_key not in (1614151, 1614155)
                        and a._Object_key = t._Marker_key
                        and t.row_num >= %%d
                        and t.row_num < %%d
                        and a._AnnotType_key = 1000''' % mouseMarkerTable, 
                        
        # 5. strain markers IDs that should be associated with their corresponding
        #       canonical markers
        '''select msm._Marker_key as canonical_marker_key,
                        a.accID, a._LogicalDB_key
                from %s t, mrk_strainmarker msm, acc_accession a
                where t.row_num >= %%d and t.row_num < %%d
                        and t._Marker_key = msm._Marker_key
                        and msm._StrainMarker_key = a._Object_key
                        and a._MGIType_key = 44
                order by 1''' % mouseMarkerTable,
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'term', 'term_type', '_Marker_key', ]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_terms'

createTempTables()

# global instance of a BatchMarkerTermsGatherer
gatherer = BatchMarkerTermsGatherer (filenamePrefix, fieldOrder, cmds)
gatherer.setChunkSize(25000)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
