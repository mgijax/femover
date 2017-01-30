#!/usr/local/bin/python
# 
# gathers data for the 'probe_polymorphism_details' table in the front-end database

import Gatherer
import symbolsort
import logger

###--- Globals ---###

keyCol = None
alleleCol = None
strainCol = None

###--- Functions ---###

def ppdCompare (a, b):
	# sort rows based on RFLV key, allele (smart-alpha), and strain (smart-alpha)
	
	k = cmp(a[keyCol], b[keyCol])
	if k:
		return k
	
	s = symbolsort.nomenCompare(a[alleleCol], b[alleleCol])
	if s:
		return s
	
	return symbolsort.nomenCompare(a[strainCol], b[strainCol])
	
###--- Classes ---###

class ProbePolymorhpismDetailsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probe_polymorphism_details table
	# Has: queries to execute against the source database
	# Does: queries the source database for details of probe polymorphisms,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		global keyCol, alleleCol, strainCol

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber(cols, '_RFLV_key')
		alleleCol = Gatherer.columnNumber(cols, 'allele')
		strainCol = Gatherer.columnNumber(cols, 'strain')
		fragmentCol = Gatherer.columnNumber(cols, 'fragments')
		
		rows.sort(ppdCompare)
		logger.debug('Sorted %d rows' % len(rows))
		
		strains = {}		# (RFLV key, allele) -> strains (comma-delimited string)
		for row in rows:
			pair = (row[keyCol], row[alleleCol])
			if pair not in strains:
				strains[pair] = row[strainCol]
			else:
				strains[pair] = '%s, %s' % (strains[pair], row[strainCol])
		
		logger.debug('Collated strains')
		
		byKey = {}			# (RFLV key, allele) -> [ key, allele, fragments, strains ]
		seqNum = 0
		
		for row in rows:
			pair = (row[keyCol], row[alleleCol])
			
			if pair not in byKey:
				seqNum = seqNum + 1
				pair = (row[keyCol], row[alleleCol])
				byKey[pair] = [ row[keyCol], row[alleleCol], row[fragmentCol], strains[pair], seqNum ]
				
		logger.debug('Collated %d rows down to %d probes' % (len(rows), len(byKey)) )
		
		self.finalColumns = [ '_RFLV_key', 'allele', 'fragments', 'strains', 'sequence_num' ]
		self.finalResults = byKey.values()
		return
	
###--- globals ---###

cmds = [
	'''select distinct r._RFLV_key, a.allele, a.fragments, s.strain
		from prb_rflv r, prb_allele a, prb_allele_strain pas, prb_strain s
		where r._RFLV_key = a._RFLV_key
			and a._Allele_key = pas._Allele_key
			and pas._Strain_key = s._Strain_key''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_RFLV_key', 'allele', 'fragments', 'strains', 'sequence_num', ]

# prefix for the filename of the output file
filenamePrefix = 'probe_polymorphism_details'

# global instance of a ProbePolymorphismDetailsGatherer
gatherer = ProbePolymorhpismDetailsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
