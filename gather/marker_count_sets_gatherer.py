#!./python
# 
# gathers data for the 'marker_count_sets' table in the front-end database
#
# 01/21/2014    lec
#       - TR11515/allele type changes
#


import Gatherer
import logger
import InteractionUtils
import MarkerUtils
import GXDUtils
import OutputFile
import gc

###--- Constants ---###

MARKER_KEY = '_Marker_key'
SET_TYPE = 'setType'
COUNT_TYPE = 'countType'
COUNT = 'count'
SEQUENCE_NUM = 'sequenceNum'

###--- Functions ---###

def stageSortKey (a):
        return int(a)

def assayTypeSortKey (a):
        return GXDUtils.getAssayTypeSeq(a)

###--- Classes ---###

class MarkerCountSetsGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the marker_count_sets table
        # Has: queries to execute against the source database
        # Does: queries the source database for counts for sets of items
        #       related to a marker (like counts of alleles by type),
        #       collates results, writes tab-delimited text file

        def report (self):
                logger.debug ('finished set %d, %d rows so far' % (
                        self.j, self.i))
                self.j = self.j + 1
                return

        def seqNum (self):
                self.i = self.i + 1
                return self.i

        def collateResultsByTheilerStage (self, resultIndex):

                setType = 'Expression Results by Theiler Stage'

                cols, rows = self.results[resultIndex]

                markerCol = Gatherer.columnNumber (cols, MARKER_KEY)
                emapsKeyCol = Gatherer.columnNumber (cols, '_emaps_key')
                stageCol = Gatherer.columnNumber (cols, '_stage_key')
                countCol = Gatherer.columnNumber (cols, COUNT)


                # resultCounts[markerKey] = { stage : count of results }
                resultCounts = {}

                for row in rows:
                        markerKey = row[markerCol]
                        emapsKey = row[emapsKeyCol]
                        stage = row[stageCol]
                        resultCount = row[countCol]

                        if not stage:
                                continue

                        if markerKey not in resultCounts:
                                resultCounts[markerKey] = {
                                        stage : resultCount }

                        elif stage not in resultCounts[markerKey]:
                                resultCounts[markerKey][stage] = resultCount

                        else:
                                resultCounts[markerKey][stage] = resultCount \
                                        + resultCounts[markerKey][stage]

                # free up memory for result set, once we've walked through it
                self.results[resultIndex] = (cols, [])
                del rows
                gc.collect()

                # At this point, we have collected our counts of expression
                # results by marker and Theiler stage.  Time to assemble rows
                # from those data.

                markerKeys = list(resultCounts.keys())
                markerKeys.sort()

                for markerKey in markerKeys:
                        stages = list(resultCounts[markerKey].keys())
                        stages.sort(key=stageSortKey)

                        for stage in stages:
                                newRow = [ markerKey, setType, stage,
                                        resultCounts[markerKey][stage],
                                        self.seqNum() ]
                                self.finalResults.append (newRow)
                self.report()
                self.writeSoFar()
                return

        def collateByAssayType (self, resultIndex, setType):

                cols, rows = self.results[resultIndex]

                markerCol = Gatherer.columnNumber (cols, MARKER_KEY)
                countCol = Gatherer.columnNumber (cols, COUNT)
                assayTypeCol = Gatherer.columnNumber (cols, COUNT_TYPE)

                # resultCounts[markerKey] = { assay type : count of results }
                resultCounts = {}

                for row in rows:
                        markerKey = row[markerCol]
                        count = row[countCol]
                        assayType = row[assayTypeCol]

                        if markerKey not in resultCounts:
                                resultCounts[markerKey] = { assayType : count }

                        elif assayType not in resultCounts[markerKey]:
                                resultCounts[markerKey][assayType] = count

                        else:
                                resultCounts[markerKey][assayType] = count \
                                        + resultCounts[markerKey][assayType]

                # free up memory for result set, once we've walked through it
                self.results[resultIndex] = (cols, [])
                del rows
                gc.collect()

                # At this point, we have collected our counts of expression
                # results by marker and Theiler stage.  Time to assemble rows
                # from those data.

                markerKeys = list(resultCounts.keys())
                markerKeys.sort()

                for markerKey in markerKeys:
                        assayTypes = list(resultCounts[markerKey].keys())
                        assayTypes.sort(key=assayTypeSortKey)

                        for assayType in assayTypes:
                                newRow = [ markerKey, setType, assayType,
                                        resultCounts[markerKey][assayType],
                                        self.seqNum() ]
                                self.finalResults.append (newRow)
                self.report()
                self.writeSoFar()
                return

        def collateResultsByAssayType (self, resultIndex):
                setType = 'Expression Results by Assay Type'
                self.collateByAssayType (resultIndex, setType)
                return

        def collateAssaysByAssayType (self, resultIndex):
                # expression results per Theiler stage (now custom, because
                # we need to translate from AD structures to EMAPS terms)

                setType = 'Expression Assays by Assay Type'
                self.collateByAssayType (resultIndex, setType)
                return

        def collateReagents (self, resultIndex):
                (columns, rows) = self.results[resultIndex]
                keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
                termCol = Gatherer.columnNumber (columns, 'term')
                countCol = Gatherer.columnNumber (columns, 'myCount')

                byMarker = {}                   # byMarker[marker key] = {}

                nucleic = 'All nucleic'         # keys per marker in byMarker
                genomic = 'Genomic'
                cdna = 'cDNA'
                primerPair = 'Primer pair'
                other = 'Other'

                # collate counts per marker

                for row in rows:
                        term = row[termCol]
                        ct = row[countCol]
                        key = row[keyCol]

                        if key not in byMarker:
                                byMarker[key] = {}

                        if term == 'primer':
                                byMarker[key][primerPair] = ct
                        elif term == 'genomic':
                                byMarker[key][genomic] = ct
                        elif term == 'cDNA':
                                byMarker[key][cdna] = ct
                        else:
                                if other in byMarker[key]:
                                    byMarker[key][other] += ct
                                else:
                                    byMarker[key][other] = ct

                # free up memory for result set, once we've walked through it
                self.results[resultIndex] = (columns, [])
                del rows
                gc.collect()

                # combine genomic, cDNA, primer pair, other into nucleic

                markerKeys = list(byMarker.keys())
                markerKeys.sort()
                for key in markerKeys:
                        byMarker[key][nucleic] = 0
                        for term in [ genomic, cdna, primerPair, other ]:
                                if term in byMarker[key]:
                                        byMarker[key][nucleic] = \
                                                byMarker[key][nucleic] + \
                                                byMarker[key][term]
                        
                # generate rows, one per marker/count pair

                orderedTerms = [ nucleic, genomic, cdna, primerPair, other ]
                for key in markerKeys:
                        for term in orderedTerms:
                                if term in byMarker[key]:
                                        newRow = [ key, 'Molecular reagents',
                                                term, byMarker[key][term],
                                                self.seqNum() ]
                                        self.finalResults.append (newRow)
                self.report()
                self.writeSoFar()
                return

        def collatePolymorphisms (self, resultIndex, snpIndex):
                logger.debug ('Retrieving SNP data')

                # assume SNP and marker coordinates are in sync.  If not, then
                # change the message.

                snpMsg = 'SNPs within 2kb'
                try:
                        if config.BUILDS_IN_SYNC == 0:
                                snpMsg = 'SNPs'
                except:
                        pass

                byMarker = {}                   # byMarker[marker key] = {}

                cols, rows = self.results[snpIndex]

                markerCol = Gatherer.columnNumber(cols, '_Marker_key')
                snpCol = Gatherer.columnNumber(cols, 'snp_count')

                for row in rows:
                        byMarker[row[markerCol]] = { snpMsg : int(row[snpCol]) } 

                self.results[snpIndex] = (cols, [])
                del rows
                gc.collect()

                logger.debug ('Found %d markers with SNPs' % len(byMarker))

                # move on to the other polymorphisms

                (columns, rows) = self.results[resultIndex]
                keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
                termCol = Gatherer.columnNumber (columns, 'term')
                countCol = Gatherer.columnNumber (columns, 'myCount')

                all = 'All PCR and RFLP'        # keys per marker in byMarker
                pcr = 'PCR'
                rflp = 'RFLP'

                # collate counts per marker

                for row in rows:
                        term = row[termCol]
                        ct = row[countCol]
                        key = row[keyCol]

                        if key not in byMarker:
                                # note that it has no SNPs (they would have
                                # already been populated otherwise)

                                byMarker[key] = { snpMsg : 0 }

                        if term == 'primer':
                                byMarker[key][pcr] = ct
                        elif rflp not in byMarker[key]:
                                byMarker[key][rflp] = ct
                        else:
                                byMarker[key][rflp] = byMarker[key][rflp] + ct

                # free up memory for result set, once we've walked through it
                self.results[resultIndex] = (columns, [])
                del rows
                gc.collect()

                # if we had both PCR and RFLP then we need a count for the sum

                markerKeys = list(byMarker.keys())
                markerKeys.sort()
                for key in markerKeys:
                        if pcr in byMarker[key] \
                            and rflp in byMarker[key]:
                                byMarker[key][all] = byMarker[key][pcr] + \
                                        byMarker[key][rflp]
                        
                # generate rows, one per marker/count pair

                orderedTerms = [ all, pcr, rflp, snpMsg ]
                for key in markerKeys:
                        for term in orderedTerms:
                                if term in byMarker[key]:
                                        newRow = [ key, 'Polymorphisms',
                                                term, byMarker[key][term], 
                                                self.seqNum() ]
                                        self.finalResults.append (newRow)
                self.report()
                self.writeSoFar()
                return

        def collateMarkerInteractions (self):

                counts = InteractionUtils.getMarkerInteractionCounts()

                fTerm = 'interacts with'

                countList = list(counts.items())
                countList.sort()
                for (key, count) in countList:
                        self.finalResults.append ( [ key, 'Interaction',
                                fTerm, count, 1 ] )

                logger.debug('Added %d rows for Interactions' % \
                        len(counts))
                self.writeSoFar()

                del counts
                gc.collect()
                return

        def collateAlleleCounts (self):
                # collate the allele counts, including the special handling
                # for markers associated with alleles via 'mutation involves'
                # relationships

                counts, countSets = MarkerUtils.getAlleleCountsByType()

                orderedSets = list(countSets.items())
                orderedSets.sort()

                markerKeys = list(counts.keys())
                markerKeys.sort()

                setType = 'Alleles'
                i = 0

                for markerKey in markerKeys:
                        i = i + len(counts[markerKey])

                        for (seqNum, countType) in orderedSets:
                                if countType in counts[markerKey]:
                                        row = [ markerKey,
                                                setType,
                                                countType, 
                                                counts[markerKey][countType],
                                                seqNum ]

                                        self.finalResults.append (row)

                del counts
                del countSets
                del orderedSets
                del markerKeys
                gc.collect()

                logger.debug('Added %d rows for Alleles' % i) 
                self.writeSoFar() 
                return

        def writeSoFar (self):
                if self.finalResults:
                        self.outFile.writeToFile (fieldOrder,
                                self.finalColumns, self.finalResults)

                        logger.debug('Wrote %d rows to output file' % \
                                len(self.finalResults))

                        self.finalResults = []
                        gc.collect()
                else:
                        logger.debug('Nothing to write')
                return

        def go (self):
                # override the go() method, so we can customize how we deal
                # with the output data file

                self.preprocessCommands()
                logger.info('Pre-processed queries')

                self.results = Gatherer.executeQueries (self.cmds)
                logger.info('Finished queries of source %s db' % \
                        Gatherer.SOURCE_DB)

                self.outFile = OutputFile.OutputFile('marker_count_sets')
                logger.info('Created output file: %s' % self.outFile.getPath())

                self.collateResults()
                self.postprocessResults()
                self.writeSoFar()
                self.outFile.close()

                print('%s %s' % (self.outFile.getPath(), 'marker_count_sets')) 
                return

        def collateResults (self):
                # combine the result sets from the various queries into a
                # single set of final results

                self.finalColumns = [ MARKER_KEY, SET_TYPE, COUNT_TYPE,
                        COUNT, SEQUENCE_NUM ]
                self.finalResults = []

                # we need to store an ordering for the items which ensures
                # that the counts for a various set of a various marker are
                # ordered correctly.  This does not require starting the order
                # for each set at 1, so just use a common ascending counter.

                self.i = 0              # counter for ordering of rows
                self.j = 0              # counter of result sets

                # first do sets which are special cases:  

                self.collateResultsByTheilerStage(0)
                self.collateAssaysByAssayType(1)
                self.collateResultsByAssayType(2)

                self.collateReagents(3)
                self.collatePolymorphisms(4, 5)
                self.collateMarkerInteractions()
                self.collateAlleleCounts()

                # the remaining sets (5 to the end) have a standard format
                # and can be done in a nested loop

# currently no queries from 5...
#               for (columns, rows) in self.results[5:]:
#                       keyCol = Gatherer.columnNumber (columns, MARKER_KEY)
#                       setCol = Gatherer.columnNumber (columns, SET_TYPE)
#                       typeCol = Gatherer.columnNumber (columns, COUNT_TYPE)
#                       countCol = Gatherer.columnNumber (columns, COUNT)
#
#                       for row in rows:
#                               newRow = [ row[keyCol], row[setCol],
#                                       row[typeCol], row[countCol], 
#                                       self.seqNum() ]
#                               self.finalResults.append (newRow)
#                       self.report()
                return

###--- globals ---###

cmds = [
        # 0. expression results by theiler stages 
        '''select ge._Marker_key,
                vte._term_key as _emaps_key,
                ge._stage_key,
                count(distinct ge._Expression_key) as %s
        from gxd_expression ge
                join voc_term_emaps vte on
                        vte._emapa_term_key = ge._emapa_term_key
                        and vte._stage_key = ge._stage_key
        where ge.isForGXD = 1
                and exists (select 1 from mrk_marker m
                        where m._Marker_key = ge._Marker_key)
        group by ge._Marker_key, vte._term_key, ge._stage_key
        order by ge._Marker_key, vte._term_key''' % COUNT,

        # 1. expression assays by type
        '''select ge._Marker_key,
                vte._term_key as _emaps_key,
                gat.assayType as %s,
                count(distinct ge._Assay_key) as %s
        from gxd_expression ge,
                gxd_assaytype gat,
                voc_term_emaps vte
        where ge._AssayType_key = gat._AssayType_key
                and vte._emapa_term_key = ge._emapa_term_key
                and vte._stage_key = ge._stage_key
                and ge.isForGXD = 1
                and exists (select 1 from mrk_marker m
                        where m._Marker_key = ge._Marker_key)
        group by ge._Marker_key, gat.assayType, vte._term_key
        order by ge._Marker_key, gat.assayType, vte._term_key''' % (COUNT_TYPE, COUNT),

        # 2. expression results by type
        '''select ge._Marker_key,
                vte._term_key as _emaps_key,
                gat.assayType as %s,
                count(distinct ge._Expression_key) as %s
        from gxd_expression ge,
                gxd_assaytype gat,
                voc_term_emaps vte
        where ge._AssayType_key = gat._AssayType_key
                and vte._emapa_term_key = ge._emapa_term_key
                and vte._stage_key = ge._stage_key
                and ge.isForGXD = 1
                and exists (select 1 from mrk_marker m
                        where m._Marker_key = ge._Marker_key)
        group by ge._Marker_key, gat.assayType, vte._term_key
        order by ge._Marker_key, gat.assayType, vte._term_key''' % (COUNT_TYPE, COUNT),

        # 3. counts of reagents by type (these need to be grouped in code, but
        # this will give us the raw counts)
        '''select pm._Marker_key,
                vt.term,
                count(distinct pp._Probe_key) as myCount
        from prb_marker pm,
                prb_probe pp,
                voc_term vt
        where pp._SegmentType_key = vt._Term_key
                and pm._Probe_key = pp._Probe_key
                and exists (select 1 from mrk_marker m
                        where m._Marker_key = pm._Marker_key)
        group by pm._Marker_key, vt.term''',

        # 4. counts of RFLP/PCR polymorphisms by type
        '''select rflv._Marker_key,
                t.term,
                count(rflv._Reference_key) as myCount
        from prb_probe p,
                prb_rflv rflv,
                prb_reference r,
                voc_term t
        where p._SegmentType_key = t._Term_key
                and rflv._Reference_key = r._Reference_key
                and r._Probe_key = p._Probe_key
                and exists (select 1 from mrk_marker m
                        where m._Marker_key = rflv._Marker_key)
        group by rflv._Marker_key, t.term''',

        # 5. counts of SNPs per marker (really, SNP locations.  A SNP that is
        # within 2kb of a marker will have all of its locations counted, even
        # if some are not within 2 kb of that marker.)  Only count SNPs with a
        # variation class of "SNP" for now.  Include join to MRK_Marker just to
        # ensure that we have legitimate marker keys.
        '''with coord_counts as (
                        select _ConsensusSNP_key,
                                count(distinct startCoordinate) as coord_count
                        from snp_coord_cache
                        where isMultiCoord = 0
                        group by _ConsensusSNP_key),
                pairs as (select distinct m._Marker_key, m._ConsensusSNP_key
                        from snp_consensussnp_marker m, snp_coord_cache c,
                                snp_consensussnp s
                        where m._ConsensusSNP_key = c._ConsensusSNP_key
                        and m._ConsensusSNP_key = s._ConsensusSNP_key
                        and s._VarClass_key = 1878510
                        and m.distance_from <= 2000)
        select m._Marker_key, sum(f.coord_count) as snp_count
        from pairs m, coord_counts f, mrk_marker mm
        where m._ConsensusSNP_key = f._ConsensusSNP_key
                and m._Marker_key = mm._Marker_key
        group by m._Marker_key
        order by m._Marker_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, MARKER_KEY, SET_TYPE, COUNT_TYPE, COUNT,
        SEQUENCE_NUM ]

# prefix for the filename of the output file
filenamePrefix = 'marker_count_sets'

# global instance of a MarkerCountSetsGatherer
gatherer = MarkerCountSetsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
