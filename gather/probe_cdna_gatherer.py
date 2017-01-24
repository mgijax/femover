#!/usr/local/bin/python
# 
# gathers data for the 'probe_cdna' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Constants ---###

tissueCol = None
ageMinCol = None
ageMaxCol = None
cellLineCol = None
nameCol = None
idCol = None

###--- Functions ---###

def cacheColumnIDs (columns):
	global tissueCol, ageMaxCol, ageMinCol, cellLineCol, nameCol, idCol
	
	tissueCol = Gatherer.columnNumber(columns, 'tissue')
	ageMinCol = Gatherer.columnNumber(columns, 'ageMin')
	ageMaxCol = Gatherer.columnNumber(columns, 'ageMax')
	cellLineCol = Gatherer.columnNumber(columns, 'cell_line')
	nameCol	= Gatherer.columnNumber(columns, 'name')
	idCol = Gatherer.columnNumber(columns, 'accID')
	return

def compareClones (a, b):
	# comparator for two clones.  Ordering is by tissue, age min, age max, cell line, name, and acc ID.
	# does smart-alpha sorting on tissue, cell line, name, and acc ID.  Of particular note, Not Specified
	# tissues sink to the bottom.  Likewise, we prefer actual ages to Not Specified ages (-1 ageMin).
	
	if a[tissueCol] == 'Not Specified':
		if b[tissueCol] != 'Not Specified':
			return 1 
	elif b[tissueCol] == 'Not Specified':
		return -1

	t = symbolsort.nomenCompare(a[tissueCol], b[tissueCol])
	if t:
		return t
	
	n = cmp(a[ageMinCol], b[ageMinCol])
	if n:
		if a[ageMinCol] < 0:	# 'a' has Not Specified age
			return 1
		if b[ageMinCol] < 0:	# 'b' has Not Specified age
			return -1
		return n
	
	x = cmp(a[ageMaxCol], b[ageMaxCol])
	if x:
		return x
	
	c = symbolsort.nomenCompare(a[cellLineCol], b[cellLineCol])
	if c:
		return c

	n = symbolsort.nomenCompare(a[nameCol], b[nameCol])
	if n:
		return n
	
	return symbolsort.nomenCompare(a[idCol], b[idCol])

###--- Classes ---###

class ProbeCdnaGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probe_cdna table
	# Has: queries to execute against the source database
	# Does: queries the source database for data specifically for cDNA probes,
	#	collates results, writes tab-delimited text file

	def postprocessResults(self):
		self.finalColumns.append('sequence_num')
		cacheColumnIDs(self.finalColumns)

		self.convertFinalResultsToList()
		self.finalResults.sort(compareClones)

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
