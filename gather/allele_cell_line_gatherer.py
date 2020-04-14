#!./python
# 
# gathers data for the 'allele_cell_line' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Classes ---###

def sortKey(t):
        # return a sort key for tuple t, which has: (allele key, is mutant?, ID/name, cell line key)
        return (t[0], t[1], symbolsort.splitter(t[2]), t[3])

class AlleleCellLineGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the allele_cell_line table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for cell lines
        #       associated to alleles, collates results, writes tab-delimited
        #       text file

        def collateResults (self):

                # list of allele/cell line relationships to be sorted, each
                # being: (allele key, is mutant?, ID/name, cell line key)
                toSort = []

                # data for each cell line:
                # cell line key : (is mutant, cell line type key, name,
                #       acc ID, logical db key, strain, vector, vector type,
                #       creator)
                cellLines = {}

                # process parent cell lines

                cols, rows = self.results[0]
                alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
                nameCol = Gatherer.columnNumber (cols, 'cellLine')
                clKeyCol = Gatherer.columnNumber (cols, '_CellLine_key')
                strainCol = Gatherer.columnNumber (cols, 'strain')
                clTypeCol = Gatherer.columnNumber (cols, '_CellLine_Type_key')
                idCol = Gatherer.columnNumber (cols, 'accID')
                ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')

                for row in rows:
                        alleleKey = row[alleleKeyCol]
                        name = row[nameCol]
                        accID = row[idCol]
                        cellLineKey = row[clKeyCol]

                        sortBy = name
                        if accID:
                                sortBy = accID

                        toSort.append ( (alleleKey, 0, sortBy, cellLineKey) )

                        if cellLineKey not in cellLines:
                                cellLines[cellLineKey] = (0, row[clTypeCol],
                                        name, accID, row[ldbKeyCol], 
                                        row[strainCol], None, None, None)

                pclCount = len(cellLines)
                logger.debug ('Found %d parent cell lines' % pclCount)

                # process mutant cell lines

                cols, rows = self.results[1]
                alleleKeyCol = Gatherer.columnNumber (cols, '_Allele_key')
                nameCol = Gatherer.columnNumber (cols, 'cellLine')
                clKeyCol = Gatherer.columnNumber (cols, '_CellLine_key')
                strainCol = Gatherer.columnNumber (cols, 'strain')
                clTypeCol = Gatherer.columnNumber (cols, '_CellLine_Type_key')
                idCol = Gatherer.columnNumber (cols, 'accID')
                ldbKeyCol = Gatherer.columnNumber (cols, '_LogicalDB_key')
                creatorCol = Gatherer.columnNumber (cols, 'creator')
                vectorCol = Gatherer.columnNumber (cols, '_Vector_key')
                vectorTypeCol = Gatherer.columnNumber (cols, '_VectorType_key')

                for row in rows:
                        alleleKey = row[alleleKeyCol]
                        name = row[nameCol]
                        accID = row[idCol]
                        cellLineKey = row[clKeyCol]

                        sortBy = name
                        if accID:
                                sortBy = accID

                        toSort.append ( (alleleKey, 1, sortBy, cellLineKey) )

                        if cellLineKey not in cellLines:
                                cellLines[cellLineKey] = (1, row[clTypeCol],
                                        name, accID, row[ldbKeyCol], 
                                        row[strainCol], row[vectorCol],
                                        row[vectorTypeCol], row[creatorCol])

                logger.debug ('Found %d mutant cell lines' % (
                        len(cellLines) - pclCount) )

                # sort the allele/cell line relationships

                toSort.sort (key=sortKey)
                logger.debug ('Sorted %d allele/cell line pairs' % len(toSort))

                # now compile the list of results 

                self.finalColumns = [ 'allele_key', 'is_parent', 'is_mutant',
                        'mgd_cellline_key', 'cellline_type', 'cellline_name',
                        'primary_id', 'logical_db', 'background_strain',
                        'vector', 'vector_type', 'creator', 'sequence_num' ]
                self.finalResults = []

                seqNum = 0

                for (alleleKey, isMutant, sortBy, cellLineKey) in toSort:

                        (isMutant, cellLineTypeKey, name, accID, logicalDBKey,
                                strain, vectorKey, vectorTypeKey, creator) = \
                                        cellLines[cellLineKey]

                        isParent = 1
                        if isMutant:
                                isParent = 0

                        seqNum = seqNum + 1

                        logicalDB = None
                        if logicalDBKey != None:
                                logicalDB = Gatherer.resolve (logicalDBKey,
                                        'acc_logicaldb',
                                        '_LogicalDB_key', 'name')

                        cellLineType = Gatherer.resolve(cellLineTypeKey)

                        vector = None
                        if vectorKey != None:
                                vector = Gatherer.resolve (vectorKey)

                        vectorType = None
                        if vectorTypeKey != None:
                                vectorType = Gatherer.resolve (vectorTypeKey)

                        self.finalResults.append ( [ alleleKey, isParent,
                                isMutant, cellLineKey, cellLineType,
                                name, accID, logicalDB, strain,
                                vector, vectorType, creator, seqNum ] )

                logger.debug ('Compiled %d data rows' % seqNum)
                return

###--- globals ---###

cmds = [
        # 0. parent cell lines for each allele
        '''select distinct a._Allele_key,
                p.cellLine,
                p._CellLine_key,
                s.strain,
                s._Strain_key,
                p._CellLine_Type_key,
                ac.accID,
                ac._LogicalDB_key
        from all_allele_cellline a
        inner join all_cellline c on (a._MutantCellLine_key = c._CellLine_key)
        inner join all_cellline_derivation d on (
                c._Derivation_key = d._Derivation_key)
        inner join all_cellline p on (
                d._ParentCellLine_key = p._CellLine_key
                and p._CellLine_key != -2)
        inner join prb_strain s on (
                p._Strain_key = s._Strain_key
                and s._Strain_key != -2)
        left outer join acc_accession ac on (
                p._CellLine_key = ac._Object_key
                and ac._MGIType_key = 28)
        order by a._Allele_key, p.cellLine''',

        # 1. mutant cell lines for each allele
        '''select a._Allele_key,
                mc._CellLine_key,
                mc.cellLine,
                ac.accID,
                ac._LogicalDB_key,
                t.term as creator,
                mc._CellLine_Type_key,
                d._Vector_key,
                d._VectorType_key,
                s.strain,
                s._Strain_key
        from all_allele_cellLine a
        inner join all_cellline mc on (
                a._MutantCellLine_key = mc._CellLine_key
                and mc._CellLine_key not in (-1,-2,-4)
                and mc.isMutant = 1
                and mc.cellLine != 'Not Specified')
        inner join prb_strain s on (mc._Strain_key = s._Strain_key)
        left outer join all_cellline_derivation d on (
                mc._Derivation_key = d._Derivation_key)
        left outer join acc_accession ac on (
                a._MutantCellLine_key = ac._Object_key
                and ac._MGIType_key = 28)
        left outer join voc_term t on (d._Creator_key = t._Term_key)
        order by a._Allele_key, mc.cellLine, ac.accID''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        Gatherer.AUTO, 'allele_key', 'is_parent', 'is_mutant',
        'mgd_cellline_key', 'cellline_type', 'cellline_name', 'primary_id',
        'logical_db', 'background_strain', 'vector', 'vector_type',
        'creator', 'sequence_num',
        ]

# prefix for the filename of the output file
filenamePrefix = 'allele_cell_line'

# global instance of a AlleleCellLineGatherer
gatherer = AlleleCellLineGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
