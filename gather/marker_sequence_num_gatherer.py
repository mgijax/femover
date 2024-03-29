#!./python
# 
# gathers data for the 'markerSequenceNum' table in the front-end database
#
# 07/02/2012    lec
#       GXD scrum/TR10269
#       - added sort by 'name'
#       - added sort by 'symbol' to byLocation
#

import Gatherer
import logger
import symbolsort
import config
import utils

###--- Classes ---###

SYMBOL = 'bySymbol'
NAME = 'byName'
TYPE = 'byMarkerType'
SUBTYPE = 'byMarkerSubType'
ORGANISM = 'byOrganism'
ID = 'byPrimaryID'
LOCATION = 'byLocation'

class MarkerSequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the markerSequenceNum table
        # Has: queries to execute against the source database
        # Does: queries the source database for ordering data for markers,
        #       collates results, writes tab-delimited text file

        def initSubTypeOrders (self):
                self.subTypeOrder = {}          # subTypeOrder[marker key] = seq num
                i = 0
                prevType=''
                for row in self.results[5][1]:
                        subtype = row[1]
                        if prevType != subtype:
                                prevType = subtype
                                i += 1
                        self.subTypeOrder[row[0]] = i
                self.maxSubTypeOrder = i+1

                logger.debug ('Ordered the %d marker sub types' % self.maxSubTypeOrder)

        def getSubTypeOrder (self,markerKey):
                if markerKey in self.subTypeOrder:
                        return self.subTypeOrder[markerKey]
                return self.maxSubTypeOrder     

        def collateResults (self):

                # compute and cache the ordering for marker types

                typeOrder = {}          # typeOrder[type key] = seq num
                i = 0
                keyCol = Gatherer.columnNumber (self.results[1][0],
                        '_Marker_Type_key')
                for row in self.results[1][1]:
                        i = i + 1
                        typeOrder[row[keyCol]] = i

                logger.debug ('Ordered the %d marker types' % len(typeOrder))

                self.initSubTypeOrders()

                # compute and cache the ordering for organisms

                organismOrder = {}      # organismOrder[org key] = seq num
                i = 0
                orgCol = Gatherer.columnNumber (self.results[2][0],
                        '_Organism_key')
                for row in self.results[2][1]:
                        i = i + 1
                        organismOrder[row[orgCol]] = i

                logger.debug ('Ordered the %d organisms' % len(organismOrder))

                # compute and cache the ordering for name

                toSort = []     # for sorting
                nameOrder = {}  # nameOrder[marker key] = seq num
                keyCol = Gatherer.columnNumber (self.results[0][0], '_Marker_key')
                nameCol = Gatherer.columnNumber (self.results[0][0], 'name')
                for row in self.results[0][1]:
                        toSort.append( (row[nameCol].lower(), row[keyCol]) )
                toSort.sort()
                logger.debug ('Ordered the names: %d' % len(toSort))

                i = 0
                for (name, key) in toSort:
                        i = i + 1
                        if key not in nameOrder:
                                nameOrder[key] = i
                logger.debug ('Assigned seq nums to name')

                # start collecting the actual data to be sorted...

                # dict[marker key] = [ marker key, sort 1, ... sort n ]
                dict = {}

                # marker symbol (assumes all markers are in this query)

                symbols = []    # list of (lowercase symbol, lowercase name, 
                                # marker key, marker type key, organism key)

                keyCol = Gatherer.columnNumber (self.results[0][0],
                        '_Marker_key')
                typeCol = Gatherer.columnNumber (self.results[0][0],
                        '_Marker_Type_key')
                orgCol = Gatherer.columnNumber (self.results[0][0],
                        '_Organism_key')
                symCol = Gatherer.columnNumber (self.results[0][0], 'symbol')
                nameCol = Gatherer.columnNumber (self.results[0][0], 'name')

                for row in self.results[0][1]:
                        # symbol sorting will be case-insensitive
                        symbols.append ( (row[symCol].lower(), row[keyCol], row[keyCol],
                                row[typeCol], row[orgCol]) )

                # sort the tuples by comparing the nomenclature using our
                # special nomen-comparison function (and ignoring the other
                # tuple items)
                symbols.sort (key=lambda a : symbolsort.splitter(a[0]))

                i = 1
                for (symbol, n, markerKey, t, o) in symbols:
                        dict[markerKey] = [ markerKey, i, nameOrder[n], typeOrder[t],
                                self.getSubTypeOrder(markerKey), organismOrder[o] ]
                        i = i + 1

                self.finalColumns = [ '_Marker_key', SYMBOL, NAME, TYPE, SUBTYPE, ORGANISM ]

                logger.debug ('Sorted %d by symbol' % len(dict))

                # primary ID (assumes sorted results with earlier rows taking
                # precedence)

                allKeys = {}
                for key in list(dict.keys()):
                        allKeys[key] = 1

                i = 0
                keyCol = Gatherer.columnNumber (self.results[3][0],
                        '_Marker_key')
                for row in self.results[3][1]:
                        key = row[keyCol]
                        if key in allKeys:
                                i = i + 1
                                del allKeys[key]
                                dict[key].append (i)

                logger.debug ('Sorted %d by ID' % i)

                # if any markers did not have an ID, then sort to the bottom
                if allKeys:
                        i = i + 1
                        for key in list(allKeys.keys()):
                                dict[key].append (i)

                self.finalColumns.append (ID)

                logger.debug ('Handled %d markers with no ID' % len(allKeys))

                # location (assume we first sort markers with coordinates,
                # then cM offsets, then cytobands.  within coordinates, we
                # sort by chromosome then start coordinate.  likewise for
                # cM offset and cytoband.)

                maxOffset = 99999999    # bigger than any cM offset
                maxCytoband = 'ZZZZ'    # bigger than any cytoband

                locations = []          # list of (chrom seq num, start coord,
                                        # cM offset, cytoband, symbol, marker key)

                columns = self.results[4][0]
                keyCol = Gatherer.columnNumber (columns, '_Marker_key')
                startCol = Gatherer.columnNumber (columns, 'startCoordinate')
                cmCol = Gatherer.columnNumber (columns, 'cmOffset')
                cytoCol = Gatherer.columnNumber (columns, 'cytogeneticOffset')
                chrCol = Gatherer.columnNumber (columns, 'sequenceNum')
                symbolCol = Gatherer.columnNumber (columns, 'symbol')
                genChrCol = Gatherer.columnNumber (columns, 'genomicchromosome')

                def sortKey(a):
                        # return a sort key for 'a' with appropriate handling for None values
                        chrom, startCoord, cM, cytoband, symbol, markerKey, genChrCol = a

                        # special handling for PAR markers; 
                        if (chrom == 22) and (genChrCol == "Y"):
                                chrom = 21
                        if (chrom == 22) and (genChrCol == "X"):
                                chrom = 20


                        chromSeqNum = utils.intSortKey(chrom)
                        symbol = utils.stringSortKey(symbol)
                        startCoord = utils.floatSortKey(startCoord)
                        cytoband = utils.stringSortKey(cytoband, noneFirst=True)

                        return (chromSeqNum, startCoord, cM, cytoband, symbol, markerKey)

                for row in self.results[4][1]:
                        cmOffset = row[cmCol]
                        if (cmOffset == None) or (cmOffset <= 0):
                                cmOffset = maxOffset

                        locations.append ( (row[chrCol], row[startCol],
                                cmOffset, row[cytoCol], row[symbolCol].lower(), row[keyCol],row[genChrCol]) )
                locations.sort(key=sortKey)

                allKeys = {}
                for key in list(dict.keys()):
                        allKeys[key] = 1

                i = 0
                for (a,b,c,d,e,markerKey,f) in locations:
                        if markerKey in allKeys:
                                i = i + 1
                                del allKeys[markerKey]
                                dict[markerKey].append (i)

                logger.debug ('Sorted %d by location' % i)

                # if any markers did not have a location, then sort to the
                # bottom
                if allKeys:
                        i = i + 1
                        for key in list(allKeys.keys()):
                                dict[key].append (i)

                logger.debug ('Handled %d without locations' % len(allKeys))

                self.finalColumns.append (LOCATION)
                self.finalResults = list(dict.values()) 
                return

###--- globals ---###

#
# result 0: markers
#       this statement is selecting all markers from all organisms (not just mouse)
#       includes withdrawn as well as official/current nomenclature
#
#
# idea:  
# 1) read the list of marker key/symbol that do not contain mrk_location_cache rows 
#    sorty by symbol
#    and store in a list
#    this can be used later to help sort the main "dict" dictionary
#

cmds = [
        '''select _Marker_key, symbol, name, _Marker_Type_key,
                        _Organism_key
                from mrk_marker
                ''',

        '''select _Marker_Type_key, name
                from mrk_types
                order by name''',

        '''select _Organism_key, commonName
                from mgi_organism
                order by commonName''',

        '''select m._Marker_key, a._LogicalDB_key, a.prefixPart, a.numericPart
                from acc_accession a, acc_logicaldb ldb, mrk_marker m
                where a._MGIType_key = 2
                        and a._LogicalDB_key = ldb._LogicalDB_key
                        and a.preferred = 1
                        and m._Marker_key = a._Object_key
                        and ldb._Organism_key = m._Organism_key
                order by m._Marker_key, a._LogicalDB_key, a.prefixPart,
                        a.numericPart''',

        '''select c._Marker_key, c.genomicchromosome, c.startCoordinate,
                        c.cmOffset, c.cytogeneticOffset, c.sequenceNum, m.symbol
                from mrk_location_cache c, mrk_marker m
                where c._Marker_key = m._Marker_key
                ''',
        '''
        select distinct mcc._marker_key,ct.term,d._Parent_key,d.sequencenum
        from mrk_mcv_cache mcc, DAG_Edge d, DAG_Node p, DAG_Node c, VOC_Term ct
        where d._DAG_key = 9 
                and d._Parent_key = p._Node_key 
                and d._Child_key = c._Node_key 
                and c._Object_key = ct._Term_key 
                and ct.term=mcc.directterms
        order by d._Parent_key, d.sequenceNum
        ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Marker_key', SYMBOL, NAME, TYPE, SUBTYPE, ORGANISM, ID, LOCATION,
        ]

# prefix for the filename of the output file
filenamePrefix = 'marker_sequence_num'

# global instance of a MarkerSequenceNumGatherer
gatherer = MarkerSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
