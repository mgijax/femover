#!/usr/local/bin/python
# 
# gathers data for the 'strain_synonym' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Functions ---###

def compareRows(a, b):
	# assumes strain key is column 0, synonym is column 1, synonym type is column 2, and
	# J: number is column 3
	
	x = cmp(a[0], b[0])
	if x == 0:
		x = symbolsort.nomenCompare(a[1], b[1])
		if x == 0:
			x = symbolsort.nomenCompare(a[2], b[2])
			if x == 0:
				# J: number can be null
				if a[3]:
					if b[3]:
						x = symbolsort.nomenCompare(a[3], b[3])
					else:
						x = -1
				elif b[3]:
					x = 1
	return x

###--- Classes ---###

class StrainSynonymGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_synonym table
	# Has: queries to execute against the source database
	# Does: queries the source database for synonyms of strains,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		self.finalColumns, self.finalResults = self.results[0]
		self.finalResults.sort(compareRows)
		self.convertFinalResultsToList()
		self.finalColumns.append('sequence_num')
		
		i = 1
		for row in self.finalResults:
			row.append(i)
			i = i + 1

		logger.debug('built %d synonym rows' % len(self.finalResults))
		return
	
###--- globals ---###

cmds = [
	'''select s._Object_key, s.synonym, t.synonymType, c.jnumID
		from mgi_synonym s
		inner join mgi_synonymtype t on (s._SynonymType_key = t._SynonymType_key and t._MGIType_key = 10)
		inner join prb_strain ps on (s._Object_key = ps._Strain_key and ps.strain not ilike '%involves%')
		left outer join bib_citation_cache c on (s._Refs_key = c._Refs_key)
		order by 1, 2'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Object_key', 'synonym', 'synonymType', 'jnumID', 'sequence_num' ]

# prefix for the filename of the output file
filenamePrefix = 'strain_synonym'

# global instance of a StrainSynonymGatherer
gatherer = StrainSynonymGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
