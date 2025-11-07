#!./python
# 
# gathers data for the recombinase tables in the front-end database.
#
# Because we are generating unique keys that only exist in the front-end
# database, and because we need those keys to drive relationships among the
# recombinase tables, we generate all those connected tables within this
# single script.  (This allows us to keep the keys in sync.)
#
# 04/24/2013    lec
#       - TR11248
#               : added alleleSystemOtherMap (added 'age', other info)
#               : added alleleStructureMap
#               : re-organized some data structures/logic
#
# An age row of data can match more than one translation,
# but a translation can only match one range of numbers.
#
# That is, "postnatal 3-24" can match p1, p2
# but p1 and p2 must have unique min/max values
#

import Gatherer
import logger
import re
import TagConverter
import json

# ageBins : used to translate an all_cre_cache.age/ageMin/ageMax
# to the appropriate age/ageMin/ageMax buckets for recombinase display
ageBins = [
    { "index": 0, "name": 'e1', "agemin":0,  "agemax":8.99,    "label":"E0-8.9" },
    { "index": 1, "name": 'e2', "agemin":9,  "agemax":13.99,   "label":"E9-13.9" },
    { "index": 2, "name": 'e3', "agemin":14, "agemax":21.0,    "label":"E14-21" }, 
    { "index": 3, "name": 'p1', "agemin":21.01, "agemax":24.99,"label":"P0-3.9" },
    { "index": 4, "name": 'p2', "agemin":25, "agemax":42.99,   "label":"P4-21.9" },
    { "index": 5, "name": 'p3', "agemin":43, "agemax":63.99,   "label":"P22-42.9" },
    { "index": 6, "name": 'p4', "agemin":64, "agemax":1864,    "label":"P >43" },
    # special-case bin for annotations where age = "postnatal" with no further info
    { "index": 7, "name": 'p5', "agemin":21.01, "agemax":1864, "label":"Postnatal" },
]

# Special case the following ages, assign to specific bins:
#       postnatal newborn -> 'P0-3.9'
#       postnatal adult -> 'P >43'
#       postnatal -> 'Postnatal'

###--- Functions ---###

def explode (x):
        # expand list x into a new list such that for each element i of x, we
        # have a tuple (i, all elements of x other than i)

        out = []
        for i in x:
                no_i = []
                for j in x:
                        if i == j:
                                continue
                        no_i.append (j)
                out.append ( (i, no_i) )
        return out

def convert (s):
        # convert any ampersands (&) in s to be ands (and)
        if not s:
                return s
        return re.sub ('&', 'and', s)

###--- Classes ---###

class RecombinaseGatherer (Gatherer.MultiFileGatherer):
        # Is: a data gatherer for the recombinase tables
        # Has: queries to execute against the source database
        # Does: queries for primary data for recombinases,
        #       collates results, writes multiple tab-delimited text files

        def findAlleleSystemPairs (self):
                # Purpose: processes query 0 to identify allele/system/structure objects
                # Returns: alleleSystemMap, alleleStructureMap, \
                #       alleleData, columns, out, \
                #       affectedCols, affected, unaffectedCols, unaffected

                #-----------------------------------------------
                # Load a lookup table of recombinase system labels and their EMAPA term keys
                # Why: the ALL_Cre_Cache table contains the recombinase system names but not their
                # keys, which we need to do rollup of annotation counts.
                sysLabel2emapaKey = {}
                emapaKey2sysLabel = {}
                cols, rows = self.results[7]
                sysKeyCol = Gatherer.columnNumber (cols, '_emapa_term_key')
                sysLabelCol = Gatherer.columnNumber (cols, 'label')
                for row in rows:
                    sysLabel2emapaKey[row[sysLabelCol]] = row[sysKeyCol]
                    emapaKey2sysLabel[row[sysKeyCol]] = row[sysLabelCol]


                #-----------------------------------------------
                # Load a mapping from EMAPS key to the distinct EMAPA keys of its ancestors
                emaps2ancestors = {}
                cols, rows = self.results[8]
                emapsCol = Gatherer.columnNumber (cols, '_emaps_term_key')
                emapaCol = Gatherer.columnNumber (cols, '_emapa_term_key')
                for row in rows:
                    emapsKey = row[emapsCol]
                    emapaKey = row[emapaCol]
                    emaps2ancestors.setdefault(emapsKey, set()).add(emapaKey)

                #-----------------------------------------------
                # Load mapping from EMAPA term key to dpc age range of that term
                emapaKey2ages = {}
                cols, rows = self.results[9]
                emapaCol = Gatherer.columnNumber (cols, '_emapa_term_key')
                dpcMinCol = Gatherer.columnNumber (cols, 'dpcmin')
                dpcMaxCol = Gatherer.columnNumber (cols, 'dpcmax')
                for row in rows:
                    emapaKey2ages[row[emapaCol]] = [row[dpcMinCol], row[dpcMaxCol]]

                # returns true iff the specified EMAPA term exists (at all) during the specified age range.
                def existsAt (emapaKey, ageMin, ageMax):
                    existsMin, existsMax = emapaKey2ages[emapaKey]
                    return ageMax >= existsMin and ageMin <= existsMax

                #-----------------------------------------------
                #
                # alleleSystemMap[allele key] = {system : allele_system_key}
                #
                alleleSystemMap = {}

                #
                # alleleSystemOtherMap[allele_system_key] = 
                #       {'cellData': ''}
                # alleleSystemOtherMap should really be merged with alleleSystemMap
                # however alleleSystemMap has many dependencies
                # and re-organizing this dictionary will require changes
                # to other functions.  so, if I have more time..I will do this
                #
                alleleSystemOtherMap = {}

                #
                #
                # Structure information for a given allele/system
                #
                # alleleStructureMap[key][system] = { structure : {
                #       'cellData':'',
                #       'alleleSystemKey': the allele/system key (see alleleSystemMap)
                #       'printName':'',
                #       'expressed':0,
                #
                alleleStructureMap = {}

                # alleleKey -> emapaKey -> system or structure counts obj
                allele2counts = {}

                #-----------------------------------------------
                # Functions for initializing accounting data structures
                #
                # init a record of counts for one cell
                def initCellCounts (allKey, emapaKey, ageBin) :
                    sv = existsAt(emapaKey, ageBin['agemin'], ageBin['agemax'])
                    return {
                        'd' : 0,    # detected in this structure/system or descendants
                        'nd' : 0,   # not detected in the structure
                        'amb' : 0,  # ambiguous in this structure
                        'ndd': 0,   # not detected (or ambiguous) in descendants
                        'sv': sv,   # is structure valid for this age range
                    }

                # init data for a system level row
                def initAlleleSystemRow (allKey, emapaKey, row) :
                    acounts = allele2counts.setdefault(allKey,{})
                    cts = acounts.get(emapaKey, None)
                    if not cts:
                        cts = [ initCellCounts(allKey, emapaKey, b) for b in ageBins ]
                        acounts[emapaKey] = cts
                    return {
                        'row':row,
                        'cellData':cts,
                        'image':0
                    }
                # init data for a structure level row
                def initAlleleStructureRow (allKey, emapaKey, alleleSystemKey) :
                    counts = initAlleleSystemRow(allKey, emapaKey, None)
                    counts.update({
                        'isPostnatal':0,
                        'isPostnatalOther':0,
                        'alleleSystemKey':alleleSystemKey,
                        'printName':'',
                        'expressed':0
                    })
                    return counts

                #-----------------------------------------------
                # main query - defines allele / system pairs.  We need to number
                # these and store them in data set 0

                cols, rows = self.results[0]
                keyCol = Gatherer.columnNumber (cols, '_Allele_key')
                idCol = Gatherer.columnNumber (cols, 'accID')
                emapsKeyCol = Gatherer.columnNumber (cols, '_emaps_key')
                structureCol = Gatherer.columnNumber (cols, 'structure')
                emapaKeyCol = Gatherer.columnNumber (cols, '_emapa_term_key')
                stageCol = Gatherer.columnNumber (cols, '_stage_key')
                symCol = Gatherer.columnNumber (cols, 'symbol')
                ageCol = Gatherer.columnNumber (cols, 'age')
                ageMinCol = Gatherer.columnNumber (cols, 'ageMin')
                ageMaxCol = Gatherer.columnNumber (cols, 'ageMax')
                expCol = Gatherer.columnNumber (cols, 'expressed')
                strCol = Gatherer.columnNumber (cols, 'strength')
                hasImageCol = Gatherer.columnNumber (cols, 'hasImage')
                systemCol = Gatherer.columnNumber (cols, 'cresystemlabel')
                assayKeyCol = Gatherer.columnNumber (cols, '_assay_key')
                resultKeyCol = Gatherer.columnNumber (cols, '_result_key')

                #
                # alleleData: dictonary of unique allele id:symbol for use in other functions
                # (findOtherSystems, findOtherAlleles)
                #
                alleleData = {}

                #
                # dictionary of affected (expressed) allele : system
                # dictionary of unaffected (not expressed) allele : system
                # for use in other functions
                #
                affectedCols = [ 'alleleKey', 'alleleSystemKey', 'system' ]
                unaffectedCols = [ 'alleleKey', 'alleleSystemKey', 'system' ]
                affected = []
                unaffected = []

                #
                # out: dictionary of output data
                #
                out = []

                #
                # allele_system_key
                #
                alleleSystemKey = 0

                #
                # first iteration.  Initialize count data structures for all the rows.
                # create index of emapaKey -> counts for that row
                #
                # P.S. Remember that rows returned by the query are annotations to EMAPS terms,
                # while rows in the result are at the EMAPA level. I.e., there is an implicit rollup
                # of EMAPS annotations to EMAPA terms. Then there is the additional (explicit) rollup
                # to EMAPA ancestors.
                #
                # P.P.S. Also remember that each row of the ALL_Cre_Cache table is represented once for each
                # recombinase system the row's structure falls under.
                #
                for row in rows:
                        allKey = row[keyCol]
                        structure = row[structureCol] # name of the annotated structure
                        emapsKey = row[emapsKeyCol]   # EMAPS key of the annotated structure
                        emapaKey = row[emapaKeyCol]   # EMAPA key corresp to the EMAPS key
                        system = row[systemCol]       # name of cre system for the annotated structure
                        systemKey = sysLabel2emapaKey[system] # emapa key of the system

                        # emapaDpcMin, emapaDpcMax = emapaKey2ages[emapaKey]
                        # ageMinTrans <= emapaDpcMax and ageMaxTrans >= emapaDpcMin

                        #
                        # Set flag: this is a "cre system row" if the annotated structure's EMAPA
                        # key matches the systems's key.
                        isSystemRow = emapaKey == systemKey

                        if not allKey in alleleSystemMap:
                            alleleSystemMap[allKey] = {}

                        if not system in alleleSystemMap[allKey]:
                            alleleSystemKey = alleleSystemKey + 1
                            alleleSystemMap[allKey][system] = alleleSystemKey
                            alleleSystemOtherMap[alleleSystemKey] = initAlleleSystemRow(allKey, systemKey, row)

                        if not isSystemRow:
                            if allKey not in alleleStructureMap :
                                    alleleStructureMap[allKey] = {}
                            if system not in alleleStructureMap[allKey] :
                                    alleleStructureMap[allKey][system] = {}
                            if structure not in alleleStructureMap[allKey][system] :
                                    alleleStructureMap[allKey][system][structure] \
                                        = initAlleleStructureRow(allKey, emapaKey, alleleSystemKey)
                                    alleleStructureMap[allKey][system][structure]['printName'] = structure
                # end for


                #
                # second iteration. iterate thru each row of allele/system/structure/age and tally the counts.
                #
                tallied = set()
                for row in rows:

                        allKey = row[keyCol]          # allele key
                        allId = row[idCol]            # MGI id of allele
                        structure = row[structureCol] # name of the annotated structure
                        emapsKey = row[emapsKeyCol]   # EMAPS key of the annotated structure
                        emapaKey = row[emapaKeyCol]   # EMAPA key corresp to the EMAPS key
                        system = row[systemCol]       # name of cre system for the annotated structure
                        systemKey = sysLabel2emapaKey[system] # emapa key of the system
                        age = row[ageCol]
                        ageMin = row[ageMinCol]
                        ageMax = row[ageMaxCol]
                        hasImage = row[hasImageCol]
                        printName = structure
                        expressed = row[expCol]
                        strength = row[strCol]
                        assayKey = row[assayKeyCol]
                        ambiguous = strength == "Ambiguous"
                        isSystemRow = emapaKey == systemKey
                        resultKey = row[resultKeyCol]

                        alleleSystemKey = alleleSystemMap[allKey][system]
                        systemCounts = alleleSystemOtherMap[alleleSystemKey]

                        if isSystemRow:
                            structureCounts = systemCounts
                        else:
                            structureCounts = alleleStructureMap[allKey][system][structure]

                        # The same result will appear once for every system the structure belongs to.
                        # When we tally the counts, only tally for the struture once.
                        key = (resultKey, structure)
                        seen = key in tallied
                        tallied.add(key)
                        
                        # tally the counts. 
                        # first decide which bin(s) the result tallies under

                        binIndexes = []
                        if age == "postnatal":
                            binIndexes = [7]
                        elif age == "postnatal newborn":
                            binIndexes = [3]
                        elif age == "postnatal adult":
                            binIndexes = [6]
                        else:
                            # check each age bin (except 'Postnatal') for overlap
                            for binIndex, ageBin in enumerate(ageBins[:-1]):
                                binMin = ageBin['agemin']
                                binMax = ageBin['agemax']
                                if ageMin <= binMax and ageMax >= binMin:
                                    binIndexes.append(binIndex)
                                
                        # Now do the tally in each bin
                        for binIndex in binIndexes:
                            cellData = structureCounts['cellData'][binIndex]
                            systemCellData = systemCounts['cellData'][binIndex]
                            # Add to cell counts for this structure (rollup happens below)
                            if ambiguous:
                                if not seen: cellData['amb'] += 1
                                if not isSystemRow: systemCellData['ndd'] += 1
                            elif expressed:
                                if not seen: cellData['d'] += 1
                                if not isSystemRow: systemCellData['d'] += 1
                            else:
                                if not seen: cellData['nd'] += 1
                                if not isSystemRow: systemCellData['ndd'] += 1

                        #
                        # affected
                        # unaffected
                        #
                        # dictionaries to track affected/unaffected (expression)
                        # by allele_key:allele_system_key:system
                        # for use in other functions
                        #
                        # assumes that "expressed = 1" order comes before "expressed = 0"
                        #

                        triple = (allKey, alleleSystemKey, system)
                        if expressed == 0 and not triple in affected:
                                if not triple in unaffected:
                                        unaffected.append (triple)
                        else:
                                if not triple in affected:
                                        affected.append (triple)
                                        
                        #
                        # alleleData: maps allele key to allele (accID, symbol) 
                        # for use in other functions
                        #
                        if allKey not in alleleData:
                                alleleData[allKey] = (row[idCol], row[symCol])

                #
                # all data has been read into dictionaries
                #
                # write all of the alleleSystemOther information into the output file
                #
                for aSysKey,aSys in alleleSystemOtherMap.items():
                    out.append ( (
                        aSysKey,
                        aSys['row'][keyCol],
                        aSys['row'][idCol],
                        aSys['row'][systemCol],
                        json.dumps(aSys['cellData']),
                    ) )

                columns = [ 'alleleSystemKey', 'alleleKey', 'alleleID', 'system', 'cell_data']

                logger.debug ('Found %d allele/system pairs' % alleleSystemKey)
                logger.debug ('Found %d alleles' % len(alleleData))

                #
                # note that other dictionaries that were generated by this
                # processing are returned and used by other functions
                #

                return alleleSystemMap, alleleStructureMap, \
                        alleleData, columns, out, \
                        affectedCols, affected, unaffectedCols, unaffected

        def findOtherSystems (self, alleleSystemMap, alleleData,
                        affectedCols, affected):
                # Purpose: processes 'alleleSystemMap' to find other systems
                #       involved with the allele for each allele/system pair
                #       (does not touch query results in self.results)
                # Returns: columns, rows

                # for quick access, get a dictionary of allele/system keys
                # where expression was 1

                asKey = affectedCols.index('alleleSystemKey')
                positives = {}
                for row in affected:
                        positives[row[asKey]] = 1

                # we need to generate the data set for
                # the recombinase_other_system table (for each allele/system
                # pair, list the other systems associated with each allele)

                out = []
                i = 0
                alleles = list(alleleSystemMap.keys())
                for allele in alleles:
                        systems = list(alleleSystemMap[allele].keys())
                        systems.sort()

                        alleleID = alleleData[allele][0]

                        for (system, otherSystems) in explode(systems):
                            # allele/system key for the current a/s pair
                            alleleSystemKey = alleleSystemMap[allele][system]

                            # Now walk through the other systems for this
                            # allele.  If any were expressed, then they need
                            # to go in the 'out' bin as other systems which
                            # showed expression for this allele.

                            for otherSystem in otherSystems:
                                if otherSystem:
                                    k = alleleSystemMap[allele][otherSystem]
                                    if k in positives:
                                        i = i + 1
                                        out.append ( (i, alleleSystemKey,
                                                alleleID, otherSystem) )

                columns = [ 'uniqueKey', 'alleleSystemKey', 'alleleID', 'system' ]
                logger.debug ('Found %d other systems' % i)

                return columns, out

        def findOtherAlleles (self, alleleSystemMap, alleleData, affectedCols, affected):
                # Purpose: processes 'alleleSystemMap' to find other alleles
                #       involved with the system for each allele/system pair
                #       (does not touch query results in self.results)
                # Returns: columns, rows

                # for quick access, get a dictionary of allele/system keys
                # where expression was 1

                asKey = affectedCols.index('alleleSystemKey')
                positives = {}
                for row in affected:
                        positives[row[asKey]] = 1

                # we will generate the data set for the
                # recombinase_other_allele table (for each allele/system pair,
                # list the other alleles associated with each system)

                bySystem = {}           # bySystem[system] = list of alleles
                alleles = list(alleleSystemMap.keys())
                for allele in alleles:
                        systems = list(alleleSystemMap[allele].keys())
                        for system in systems:
                                asKey = alleleSystemMap[allele][system]

                                # if this allele/system did not have expressed
                                # = 1, then skip it

                                if asKey not in positives:
                                        continue

                                if system not in bySystem:
                                        bySystem[system] = [ allele ]
                                else:
                                        bySystem[system].append (allele)

                out = []
                i = 0

                for system in list(bySystem.keys()):
                        alleles = bySystem[system]

                        for (allele, otherAlleles) in explode(alleles):
                                alleleSystemKey = alleleSystemMap[allele][system]

                                for other in otherAlleles:      
                                        i = i + 1
                                        out.append ( (
                                                i,
                                                alleleSystemKey,
                                                other,
                                                alleleData[other][0],
                                                alleleData[other][1]
                                                ) )

                columns = [ 'uniqueKey', 'alleleSystemKey', 'alleleKey', 'alleleID', 'alleleSymbol', ]
                logger.debug ('Found %d other alleles' % i)

                return columns, out

        def findGenotypes (self):
                # extract allelic composition and background strain from
                # query 1

                keyCol = Gatherer.columnNumber (self.results[1][0], '_Genotype_key')
                noteCol = Gatherer.columnNumber (self.results[1][0], 'note')
                strainCol = Gatherer.columnNumber (self.results[1][0], 'strain')

                alleleComp = {}
                strain = {}

                for r in self.results[1][1]:
                        key = r[keyCol]
                        if key in alleleComp:
                                alleleComp[key] = alleleComp[key] + r[noteCol]
                        else:
                                alleleComp[key] = r[noteCol]
                                strain[key] = r[strainCol]

                # trim trailing whitespace from allelic composition notes

                for key in list(alleleComp.keys()):
                        alleleComp[key] = TagConverter.convert ( alleleComp[key].rstrip() )

                logger.debug ('Found %d genotypes' % len(strain))
                return alleleComp, strain

        def findAssayNotes (self):
                # extract assay note from query 2

                keyCol = Gatherer.columnNumber (self.results[2][0], '_Assay_key')
                noteCol = Gatherer.columnNumber (self.results[2][0], 'assayNote')

                assayNote = {}

                for r in self.results[2][1]:
                        key = r[keyCol]
                        assayNote[key] = r[noteCol]

                logger.debug ('Found %d assay notes' % len(assayNote))
                return assayNote

        def findNamesIDs (self, results):
                ids = {}
                names = {}

                columns, rows = results
                prepCol = Gatherer.columnNumber (columns, '_ProbePrep_key')
                nameCol = Gatherer.columnNumber (columns, 'name')
                idCol = Gatherer.columnNumber (columns, 'accID')
                
                for r in rows:
                        key = r[prepCol]

                        # only keep the first ID and name found for
                        # each probe/antibody

                        if key not in ids:
                                ids[key] = r[idCol]
                                names[key] = r[nameCol] 
                return ids, names

        def findDistinctStructures (self, alleleStructureMap):
                # Purpose: processes 'alleleStructureMap' to build 
                #       'recombinase_system_structure' columns/rows
                # Returns: columns, rows

                sCols = ['alleleSystemKey','structure','structureSeq', 'cell_data']

                sRows = []
                count = 0

                for allele in alleleStructureMap:
                        for system in alleleStructureMap[allele]:
                                sorted_list = list(alleleStructureMap[allele][system])
                                sorted_list.sort(key=lambda s : s)
                                count = 0
                                for structure in sorted_list:
                                        count += 1
                                        # if system is embryo-other or mouse-other, "zero out" cells that aren't valid 
                                        cellData = alleleStructureMap[allele][system][structure]['cellData']
                                        if system in ['embryo-other', 'mouse-other']:
                                            # make a deep copy
                                            cellData = list(map(lambda d: d.copy(), cellData))
                                            # zero out cells that don't apply
                                            rng = [0,2] if system == "mouse-other" else [3,5]
                                            for i in range(rng[0], rng[1]+1):
                                                cellData[i]['d'] = cellData[i]['nd'] = cellData[i]['ndd'] = cellData[i]['amb']  = 0
                                        #
                                        sRows.append((alleleStructureMap[allele][system][structure]['alleleSystemKey'],
                                                alleleStructureMap[allele][system][structure]['printName'],
                                                count,
                                                json.dumps(cellData)
                                                ))

                logger.debug ('Found %d recombinase_system_structures' % len(sRows))
                return sCols,sRows 

        def findAssayResults (self, alleleSystemMap):

                # preliminary extractions from queries 1-4

                alleleComp, strain = self.findGenotypes()
                assayNotes = self.findAssayNotes()
                probeIDs, probeNames = self.findNamesIDs (self.results[3])
                antibodyIDs, antibodyNames = self.findNamesIDs ( self.results[4])

                # detailed assay results from query 5

                cols = self.results[5][0]

                alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
                emapsKeyCol = Gatherer.columnNumber (cols, '_emaps_key')
                structureCol = Gatherer.columnNumber (cols, 'structure')
                stageCol = Gatherer.columnNumber (cols, '_stage_key')
                assayTypeCol = Gatherer.columnNumber (cols, '_AssayType_key')
                reporterCol = Gatherer.columnNumber (cols, '_ReporterGene_key')
                assayCol = Gatherer.columnNumber (cols, '_Assay_key')
                ageCol = Gatherer.columnNumber (cols, 'age')
                sexCol = Gatherer.columnNumber (cols, 'sex')
                specimenNoteCol = Gatherer.columnNumber (cols, 'specimenNote')
                genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
                resultNoteCol = Gatherer.columnNumber (cols, 'resultNote')
                strengthCol = Gatherer.columnNumber (cols, '_Strength_key')
                patternCol = Gatherer.columnNumber (cols, '_Pattern_key')
                jnumCol = Gatherer.columnNumber (cols, 'jnumID')
                pPrepCol = Gatherer.columnNumber (cols, '_ProbePrep_key')
                aPrepCol = Gatherer.columnNumber (cols, '_AntibodyPrep_key')
                dbResultCol = Gatherer.columnNumber (cols, '_Result_key')
                systemCol = Gatherer.columnNumber (cols, 'cresystemlabel')
                cellTypeCol = Gatherer.columnNumber (cols, 'cell_type')
                cellTypeKeyCol = Gatherer.columnNumber (cols, 'cell_type_key')

                out = []
                columns = [ 'resultKey', 'alleleSystemKey', 'structureKey', 'structure',
                        'age', 'sex', 'jnumID', 'resultNote', 'specimenNote',
                        'level', 'pattern', 'assayType', 'reporterGene',
                        'detectionMethod', 'allelicComposition', 'strain',
                        'assayNote', 'probeID', 'probeName', 'antibodyID',
                        'antibodyName', 'cell_type', 'cell_type_key',
                        ]

                # Note that cell types are now part of defining a unique result.
                # (We cannot show multiple cell types per result, so each must
                # be a unique result, even if everything else is identical.)
                
                # newResultKeys[db result key] = [ (new result key, new
                #       allele/system key, cell type), ... ]
                newResultKeys = {}

                i = 0
                for r in self.results[5][1]:
                        system = r[systemCol]
                        cellType = r[cellTypeCol]
                        cellTypeKey = r[cellTypeKeyCol]
                        alleleSystemKey = alleleSystemMap[r[alleleCol]][system]

                        i = i + 1

                        # map the old result key (from the database) to the
                        # new result key(s) and the allele/system keys being
                        # generated

                        info = (i, alleleSystemKey, cellType)
                        dbResultKey = r[dbResultCol]
                        if dbResultKey in newResultKeys:
                                newResultKeys[dbResultKey].append (info)
                        else:
                                newResultKeys[dbResultKey] = [ info ]

                        emapsKey = r[emapsKeyCol]
                        structure = r[structureCol]

                        row = [ i, alleleSystemKey, emapsKey, structure, r[ageCol], r[sexCol], r[jnumCol], r[resultNoteCol], r[specimenNoteCol], ]
                        row.append (Gatherer.resolve (r[strengthCol], 'VOC_Term', '_Term_key', 'term'))
                        row.append (Gatherer.resolve (r[patternCol], 'VOC_Term', '_Term_key', 'term'))
                        row.append (Gatherer.resolve (r[assayTypeCol], 'GXD_AssayType', '_AssayType_key', 'assayType'))
                        row.append (Gatherer.resolve (r[reporterCol]))

                        pPrep = r[pPrepCol]
                        aPrep = r[aPrepCol]

                        if pPrep in probeIDs:
                                row.append ('probe')
                        elif aPrep in antibodyIDs:
                                row.append ('antibody')
                        else:
                                row.append ('direct')

                        genotype = r[genotypeCol]

                        if genotype in alleleComp:
                                row.append (alleleComp[genotype])
                        else:
                                row.append (None)

                        if genotype in strain:
                                row.append (strain[genotype])
                        else:
                                row.append (None)

                        if r[assayCol] in assayNotes:
                                row.append (assayNotes[r[assayCol]])
                        else:
                                row.append (None)

                        toDo = [ (pPrep, (probeIDs, probeNames) ), (aPrep, (antibodyIDs, antibodyNames) ) ]

                        for (prep, dicts) in toDo:
                                for dict in dicts:
                                        if prep in dict:
                                                row.append (dict[prep])
                                        else:
                                                row.append (None)

                        row.append(cellType)
                        row.append(cellTypeKey)
                        out.append (row)

                logger.debug ('Found %d assay results' % len(out))
                logger.debug ('Mapped %d assay results' % len(newResultKeys))
                return columns, out, newResultKeys

        def findSortValues (self, cols, rows):
                keyCol = Gatherer.columnNumber (cols, 'resultKey')

                byFields = [ 'structure', 'age', 'level', 'pattern', 'jnumID',
                        'assayType', 'reporterGene', 'detectionMethod',
                        'assayNote', 'allelicComposition', 'sex',
                        'specimenNote', 'resultNote' ]

                sorted = {}             # sorted[key] = [ field1, ... ]
                for row in rows:
                        sorted[row[keyCol]] = [ row[keyCol] ]

                for field in byFields:
                        nullValued = []
                        sortable = []

                        myCol = Gatherer.columnNumber (cols, field)
                        for row in rows:
                                myVal = row[myCol]
                                if myVal == None:
                                        nullValued.append (row[keyCol])
                                else:
                                        sortable.append ((myVal.lower(), row[keyCol]))
                        sortable.sort()

                        i = 1
                        for (myValue, key) in sortable:
                                sorted[key].append (i)
                                i = i + 1

                        for key in nullValued:
                                sorted[key].append (i)

                        logger.debug ('Sorted %d rows by %s' % (i - 1, field))

                allRows = list(sorted.values())
                allRows.sort()
                logger.debug ('Sorted output rows prior to load')

                return [ 'resultKey' ] + byFields, allRows

        def findResultImages (self,
                resultMap       # maps from old result key to (new result key,
                                # new allele/system key, cell type)
                ):

                # associations between results and image panes is in query 6

                columns, rows = self.results[6]

                resultCol = Gatherer.columnNumber (columns, '_Result_key')
                labelCol = Gatherer.columnNumber (columns, 'paneLabel')
                imageCol = Gatherer.columnNumber (columns, '_Image_key')

                # columns for assay result image panes
                arColumns = [ 'uniqueKey', 'resultKey', 'imageKey', 'sequenceNum', 'paneLabel', ]

                # columns for allele system images
                asColumns = [ 'uniqueKey', 'alleleSystemKey', 'imageKey', 'sequenceNum', ]

                arRows = []     # rows for assay result image panes
                asRows = []     # rows for allele system image panes

                arNum = 0       # sequence number for assay result image panes
                asNum = 0       # sequence number for allele system img panes

                done = {}       # done[(allele/system key, image key)] = 1

                lastResultKey = None
                lastSystemKey = None

                arMax = 0
                asMax = 0

                for row in rows:
                    if row[resultCol] not in resultMap:
                        logger.debug ('Unknown result key: %s' % row[resultCol])
                        continue
                    for (resultKey, alleleSysKey, cellType) in resultMap[row[resultCol]]:
                        imageKey = row[imageCol]
                        label = row[labelCol]

                        if resultKey == lastResultKey:
                                arNum = arNum + 1
                        else:
                                arNum = 1
                                lastResultKey = resultKey

                        arMax = arMax + 1
                        arRows.append ( (arMax, resultKey, imageKey, arNum, label) )

                        pair = (alleleSysKey, imageKey)
                        if pair not in done:

                                if alleleSysKey != lastSystemKey:
                                        asNum = asNum + 1
                                else:
                                        asNum = 1
                                        lastSystemKey = alleleSysKey

                                asMax = asMax + 1
                                asRows.append ( (asMax, alleleSysKey, imageKey, asNum) )
                                done[pair] = 1

                return arColumns, arRows, asColumns, asRows

        def collateResults (self):

                # step 1 -- recombinase_allele_system table

                alleleSystemMap, alleleStructureMap, \
                        alleleData, columns, rows, \
                        affectedCols, affected, unaffectedCols, unaffected, \
                         = self.findAlleleSystemPairs()
                self.output.append ( (columns, rows) )

                # step 2 -- recombinase_other_system table

                columns, rows = self.findOtherSystems (alleleSystemMap, alleleData, affectedCols, affected)
                self.output.append ( (columns, rows) )

                # step 3 -- recombinase_other_allele table

                columns, rows = self.findOtherAlleles (alleleSystemMap, alleleData, affectedCols, affected)
                self.output.append ( (columns, rows) )

                # step 4 -- recombinase_assay_result table

                columns, rows, resultMap = self.findAssayResults (alleleSystemMap)
                self.output.append ( (columns, rows) )

                # step 5 -- recombinase_assay_result_sequence_num table

                columns, rows = self.findSortValues (columns, rows)
                self.output.append ( (columns, rows) )

                # step 6 - recombinase_assay_result_imagepane and
                #       recombinase_allele_system_imagepane tables
                arColumns, arRows, asColumns, asRows = self.findResultImages ( resultMap)
                self.output.append ( (asColumns, asRows) )
                self.output.append ( (arColumns, arRows) )

                # step 7 - allele_recombinase_affected_system and
                #       allele_recombinase_unaffected_system tables

                self.output.append ( (affectedCols, affected) )
                self.output.append ( (unaffectedCols, unaffected) )

                # step 8 - recombinase_system_structure
                columns, rows = self.findDistinctStructures(alleleStructureMap)
                self.output.append((columns,rows))

                return

###--- globals ---###

# SQL commands to be executed against the source database
cmds = [
        # 0
        # all allele / system / structure / age information
        # make sure 'expression' is ordered descending so that the '1' are encountered first
        #
        '''select c._Allele_key, c.accID,
                c._stage_key, vte._term_key as _emaps_key,
                c._emapa_term_key, c.emapaterm as structure,
                c.symbol, c.age, c.ageMin, c.ageMax, c.expressed, c.strength,
                c.hasImage, c.cresystemlabel, c._assay_key, r._result_key, s.specimenlabel
        from all_cre_cache c
                join voc_term_emaps vte on
                        c._emapa_term_key = vte._emapa_term_key
                        and c._stage_key = vte._stage_key
                join gxd_assay a on
                    c._assay_key = a._assay_key
                join gxd_specimen s on
                    a._assay_key = s._assay_key
                    and s.age = c.age
                join gxd_insituresult r on
                    s._specimen_key = r._specimen_key
                join gxd_iSresultstructure rs on
                    r._result_key = rs._result_key
                    and rs._emapa_term_key = c._emapa_term_key
                    and rs._stage_key = c._stage_key
                join voc_term gxs on
                    r._strength_key = gxs._term_key
                    and c.strength = gxs.term
        where c.cresystemlabel is not null
            and c.ageMin is not null
            and c.ageMax is not null
        order by c._Allele_key, c.cresystemlabel, c.expressed desc''',

        # 1
        # genetic strain/background info by genotype
        #
        '''select distinct s._Genotype_key, mn.note, t.strain
        from all_cre_cache c,
                gxd_specimen s,
                mgi_note mn,
                gxd_genotype g,
                prb_strain t
        where c._Assay_key = s._assay_key
                and s._Genotype_key = mn._Object_key
                and mn._NoteType_key = 1018
                and s._Genotype_key = g._Genotype_key
                and g._Strain_key = t._Strain_key''',

        # 2
        # assay notes by assay key
        #
        '''select distinct a._Assay_key, a.assayNote
        from gxd_assaynote a,
                all_cre_cache c
        where a._Assay_key = c._Assay_key
        order by a._Assay_key''',

        # 3
        # probes for each Cre probe prep key
        #
        '''select distinct g._ProbePrep_key,
                p.name,
                acc.accID
        from all_cre_cache c,
                gxd_assay a,
                gxd_probeprep g,
                prb_probe p,
                acc_accession acc
        where c._Assay_key = a._Assay_key
                and a._ProbePrep_key = g._ProbePrep_key
                and g._Probe_key = p._Probe_key
                and g._Probe_key = acc._Object_key
                and acc._LogicalDB_key = 1
                and acc._MGIType_key = 3
                and acc.preferred = 1
                and acc.prefixPart = 'MGI:'
                ''',

        # 4
        # antibodies for each Cre antibody key
        #
        '''select distinct g._AntibodyPrep_key as _ProbePrep_key,
                p.antibodyName as name,
                acc.accID
        from all_cre_cache c,
                gxd_assay a,
                gxd_antibodyprep g,
                gxd_antibody p,
                acc_accession acc
        where c._Assay_key = a._Assay_key
                and a._AntibodyPrep_key = g._AntibodyPrep_key
                and g._Antibody_key = p._Antibody_key
                and g._Antibody_key = acc._Object_key
                and acc._LogicalDB_key = 1
                and acc._MGIType_key = 6
                and acc.preferred = 1
                and acc.prefixPart = 'MGI:'
                ''',

        # 5
        # main cre assay result data
        #
        '''with result_cell_types as (
            select distinct ct._Result_key, t.term as cell_type, t._term_key as cell_type_key
            from gxd_isresultcelltype ct, voc_term t
            where ct._CellType_Term_key = t._Term_key
        )
        select distinct c._Allele_key, c._stage_key, vte._term_key as _emaps_key, c.emapaterm as structure,
                a._AssayType_key, a._ReporterGene_key, a._Refs_key,
                a._Assay_key, a._ProbePrep_key, a._AntibodyPrep_key,
                s.age, s.sex, s.specimenNote, s._Genotype_key,
                r.resultNote, r._Strength_key, r._Pattern_key, r._Result_key,
                racc.accid as jnumID, c.cresystemlabel, rct.cell_type, rct.cell_type_key
        from  all_cre_cache c
        join gxd_assay a on
            a._assay_key = c._assay_key
        join gxd_specimen s on
            s._assay_key = a._assay_key
        join gxd_insituresult r on
            r._specimen_key = s._specimen_key
        join gxd_iSresultstructure rs on
            rs._result_key = r._result_key
            and rs._emapa_term_key = c._emapa_term_key
            and rs._stage_key = c._stage_key
        join acc_accession racc on
            racc._object_key = a._refs_key
            and racc._mgitype_key = 1
            and racc._logicaldb_key = 1
            and racc.preferred = 1
            and prefixpart = 'J:'
        join voc_term_emaps vte on
            c._emapa_term_key = vte._emapa_term_key
            and c._stage_key = vte._stage_key
        left outer join result_cell_types rct on (r._result_key = rct._result_key)
        where c.cresystemlabel is not null
        order by c._Allele_key, r._Result_key, rct.cell_type
        ''',

        # 6
        # image panes associated with recombinase assay results
        #
        '''select distinct g._Result_key, i.paneLabel, i._Image_key
        from img_imagepane i,
                gxd_insituresultimage g,
                gxd_insituresult r,
                gxd_specimen s,
                all_cre_cache c
        where g._ImagePane_key = i._ImagePane_key
                and g._Result_key = r._Result_key
                and r._Specimen_key = s._Specimen_key
                and s._Assay_key = c._Assay_key''',

        # 7
        # Need a lookup table of names/_emapa_term_keys for all recombinase systems.
        #
        '''
        select 
            sm._object_key as _emapa_term_key,
            case when sm.label is not null then sm.label else ea.term end as label
        from
            mgi_setmember sm,
            voc_term ea
        where sm._set_key = 1047
        and sm._object_key = ea._term_key
        ''',

        # 8
        # In order to roll up annotation counts, need EMAP dag closure info.
        # Need mapping from EMAPS key to the EMAPA keys of all ancestors.
        # Follow path: EMAPS term -> EMAPS ancestors -> corresp EMAPA terms
        '''
        select
            es._term_key as _emaps_term_key,
            esa._emapa_term_key
        from
            voc_term_emaps es,
            dag_closure dc,
            voc_term_emaps esa
        where
            es._term_key = dc._descendentobject_key
            and dc._ancestorobject_key = esa._term_key
        ''',

        # 9
        # Need age range for every emapa term (so we can draw those little open circles)
        #
        '''
        select
            vta._term_key as _emapa_term_key,
            ts1.dpcmin,
            ts2.dpcmax
        from
            voc_term_emapa vta,
            gxd_theilerstage ts1,
            gxd_theilerstage ts2
        where
            vta.startstage = ts1._stage_key
            and vta.endstage = ts2._stage_key
        ''',
        ]

# data about files to be written; for each:  (filename prefix, list of field
#       names in order to be written, name of table to be loaded)
files = [
        ('recombinase_allele_system',
                [ 'alleleSystemKey', 'alleleKey', 'alleleID', 'system', 'cell_data'],
                'recombinase_allele_system'),

        ('recombinase_other_system',
                [ 'uniqueKey', 'alleleSystemKey', 'alleleID', 'system' ],
                'recombinase_other_system'),

        ('recombinase_other_allele',
                [ 'uniqueKey', 'alleleSystemKey', 'alleleKey', 'alleleID',
                        'alleleSymbol' ],
                'recombinase_other_allele'),

        ('recombinase_assay_result',
                [ 'resultKey', 'alleleSystemKey', 'structureKey', 'structure', 'age', 'level',
                        'pattern', 'jnumID', 'assayType', 'reporterGene',
                        'detectionMethod', 'sex', 'allelicComposition',
                        'strain', 'assayNote', 'resultNote',
                        'specimenNote', 'probeID', 'probeName', 'antibodyID',
                        'antibodyName', 'cell_type', 'cell_type_key' ],
                'recombinase_assay_result'),

        ('recombinase_assay_result_sequence_num',
                [ 'resultKey', 'structure', 'age', 'level', 'pattern',
                        'jnumID', 'assayType', 'reporterGene',
                        'detectionMethod', 'assayNote', 'allelicComposition',
                        'sex', 'specimenNote', 'resultNote' ],
                'recombinase_assay_result_sequence_num'),

        ('recombinase_allele_system_imagepane',
                [ 'uniqueKey', 'alleleSystemKey', 'imageKey', 'sequenceNum' ],
                'recombinase_allele_system_to_image'),

        ('recombinase_assay_result_imagepane',
                [ 'uniqueKey', 'resultKey', 'imageKey', 'sequenceNum',
                        'paneLabel', ],
                'recombinase_assay_result_imagepane'),

        ('recombinase_affected_system',
                [ Gatherer.AUTO, 'alleleKey', 'alleleSystemKey', 'system' ],
                'recombinase_affected_system'),

        ('recombinase_unaffected_system',
                [ Gatherer.AUTO, 'alleleKey', 'alleleSystemKey', 'system' ],
                'recombinase_unaffected_system'),

        ('recombinase_system_structure',
                [ Gatherer.AUTO, 'alleleSystemKey', 'structure','structureSeq',
                        'cell_data' ],
                'recombinase_system_structure'),

        ]

# global instance of a RecombinaseGatherer
gatherer = RecombinaseGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
