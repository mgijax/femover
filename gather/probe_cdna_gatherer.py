#!./python
# 
# gathers data for the 'probe_cdna' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Constants ---###

tissueCol = None
ageCol = None
ageMinCol = None
ageMaxCol = None
cellLineCol = None
nameCol = None
idCol = None

NS = 'Not Specified'
NA = 'Not Applicable'

###--- Functions ---###

def cacheColumnIDs (columns):
        global tissueCol, ageCol, ageMaxCol, ageMinCol, cellLineCol, nameCol, idCol
        
        tissueCol = Gatherer.columnNumber(columns, 'tissue')
        ageCol = Gatherer.columnNumber(columns, 'age')
        ageMinCol = Gatherer.columnNumber(columns, 'ageMin')
        ageMaxCol = Gatherer.columnNumber(columns, 'ageMax')
        cellLineCol = Gatherer.columnNumber(columns, 'cell_line')
        nameCol = Gatherer.columnNumber(columns, 'name')
        idCol = Gatherer.columnNumber(columns, 'accID')
        return

def suppressNSNA(s):
        # push Not Specified value to the bottom, followed by Not Applicable, then anything else
        if type(s) != str:
                return 'zzzzzz'
        if s == NS:
                return 'zzzzzz' + s
        if s == NA:
                return 'zzzzzz' + s
        return s

def suppressNSAge(f):
        # push Not Specified (-1) ages to the bottom, other ages sorted by value
        if (type(f) != float) and (type(f) != int):
                return 999999999.9
        if f < 0:
                return 999999999.8
        return f

def cloneSortKey(a):
        # return sort key for clone a, ordering by tissue, age min, age max, cell line, name, acc ID.
        # does smart-alpha sorting on tissue, cell line, name, and acc ID.  Of particular note, Not Specified
        # tissues sink to the bottom.  Likewise, we prefer actual ages to Not Specified ages (-1 ageMin).

        tissue = suppressNSNA(a[tissueCol])
        age = suppressNSAge(a[ageCol])
        minAge = suppressNSAge(a[ageMinCol])
        maxAge = suppressNSAge(a[ageMaxCol])
        cellLine = symbolsort.splitter(a[cellLineCol])
        name = symbolsort.splitter(a[nameCol])
        accID = symbolsort.splitter(a[idCol])

        return (tissue, age, minAge, maxAge, cellLine, name, accID)

###--- Classes ---###

class ProbeCdnaGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the probe_cdna table
        # Has: queries to execute against the source database
        # Does: queries the source database for data specifically for cDNA probes,
        #       collates results, writes tab-delimited text file

        def postprocessResults(self):
                self.finalColumns.append('sequence_num')
                cacheColumnIDs(self.finalColumns)

                self.convertFinalResultsToList()
                self.finalResults.sort(key=cloneSortKey)

                i = 0
                for row in self.finalResults:
                        i = i + 1
                        row.append(i)
                logger.debug('Computed %d sequence numbers' % i)
                return

###--- globals ---###

cmds = [
        # Ordering in this query in the database is slower than just doing the Python side, side we'd need to 
        # at least do a final ordering in Python using the compareClones function.
        '''with myProbes as (select distinct p._Probe_key, p._Source_key, p.name
                        from prb_probe p, voc_term t, prb_marker pm
                        where p._SegmentType_key = t._Term_key
                                and t.term = 'cDNA'
                                and p._Probe_key = pm._Probe_key
                                and pm.relationship in ('E', 'P')
                        )
                select p._Probe_key, s.age, t.tissue, vt.term as cell_line, s.ageMin, s.ageMax, p.name, a.accID
                from myProbes p
                inner join prb_source s on (p._Source_key = s._Source_key and s._Organism_key = 1)
                inner join acc_accession a on (p._Probe_key = a._Object_key and a.preferred = 1 and a.private = 0
                        and a._MGIType_key = 3 and a._LogicalDB_key = 1)
                left outer join prb_tissue t on (s._Tissue_key = t._Tissue_key)
                left outer join voc_term vt on (s._CellLine_key = vt._Term_key)''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Probe_key', 'age', 'tissue', 'cell_line', 'sequence_num' ]

# prefix for the filename of the output file
filenamePrefix = 'probe_cdna'

# global instance of a ProbeCdnaGatherer
gatherer = ProbeCdnaGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
