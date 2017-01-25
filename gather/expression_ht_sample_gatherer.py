#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample' table in the front-end database

import Gatherer
import logger
import VocabSorter
import AgeUtils
import symbolsort
from expression_ht import samples

###--- Globals ---###

expKeyCol = None		# column indexes populated by cacheColumns()
relevanceCol = None
ageMinCol = None
ageMaxCol = None
structureCol = None
tsCol = None
organismCol = None
nameCol = None

###--- Functions ---###

def cleanOrganism(organism):
	# switches ordering on organism, if a comma separates words
	# (eg- "mouse, laboratory" becomes "laboratory mouse")
	
	if not organism:
		return organism
	pieces = map(lambda x: x.strip(), organism.split(','))
	return ' '.join(pieces)
	
def cacheColumns(cols):
	# populate the global variables for which columns are where in the final results
	global expKeyCol, relevanceCol, ageMaxCol, ageMinCol, structureCol, tsCol, organismCol, nameCol
	
	expKeyCol = Gatherer.columnNumber(cols, '_Experiment_key')
	relevanceCol = Gatherer.columnNumber(cols, 'relevancy')
	ageMaxCol = Gatherer.columnNumber(cols, 'ageMax')
	ageMinCol = Gatherer.columnNumber(cols, 'ageMin')
	structureCol = Gatherer.columnNumber(cols, '_EMAPA_key')
	tsCol = Gatherer.columnNumber(cols, 'theiler_stage')
	organismCol = Gatherer.columnNumber(cols, 'organism')
	nameCol = Gatherer.columnNumber(cols, 'name')

	logger.debug('Cached 8 column indices')
	return

preferredOrganisms = {
	'mouse, laboratory' : 1,
	'laboratory mouse' : 1,
	}
def compareSamples(a, b):
	# sort samples by:
	#	0. experiment key, 1. relevance (yes first), 2. ageMin, 3. ageMax, 4. structure (topologically),
	#   5. Theiler stage, 6. organism (mouse then others alphabetically), 7. sample name
	# assumes: cacheColumns() was called previously
	
	byExpKey = cmp(a[expKeyCol], b[expKeyCol])				# experiment key
	if byExpKey != 0:
		return byExpKey

	byRelevance = cmp(a[relevanceCol], b[relevanceCol]) * -1	# relevance in descending order: yes, no, None
	if byRelevance != 0:
		return byRelevance

	# relevances match at this point; only check displayed fields for sorting
	if a[relevanceCol].lower() == 'yes':
		if a[ageMinCol] != None:						# ageMin
			if b[ageMinCol] != None:
				byAgeMin = cmp(a[ageMinCol], b[ageMinCol])
				if byAgeMin != 0:
					return byAgeMin 
			else:
				return -1
		elif b[ageMinCol] != None:
			return 1
		
		if a[ageMaxCol] != None:						# ageMax
			if b[ageMaxCol] != None:
				byAgeMax = cmp(a[ageMaxCol], b[ageMaxCol])
				if byAgeMax != 0:
					return byAgeMax
			else:
				return -1
		elif b[ageMaxCol] != None:
			return 1

		if a[tsCol]:								# Theiler Stage
			if b[tsCol]:
				byTS = cmp(int(a[tsCol]), int(b[tsCol]))
				if byTS != 0:
					return byTS
			else:
				return -1
		elif b[tsCol]:
			return 1
	
		if a[structureCol]:								# structure, sorted topologically
			if b[structureCol]:
				byStructure = cmp(VocabSorter.getSequenceNum(a[structureCol]), VocabSorter.getSequenceNum(b[structureCol]))
				if byStructure != 0:
					return byStructure
			else:
				return -1
		elif b[structureCol]:
			return 1
	
	if a[organismCol]:							# organism, preferred organisms above, others alphabetical
		if b[organismCol]:
			if a[organismCol] in preferredOrganisms:
				if b[organismCol] in preferredOrganisms:
					byOrg = cmp(preferredOrganisms[a[organismCol]], preferredOrganisms[b[organismCol]])
					if byOrg != 0:
						return byOrg
				else:
					return -1
			elif b[organismCol] in preferredOrganisms:
				return 1
			else:
				# neither a nor b has a preferred organism, so sort them alphabetically
				byOrg = cmp(a[organismCol].lower(), b[organismCol].lower())
				if byOrg != 0:
					return byOrg
		else:
			return -1
	elif b[organismCol]:
		return 1
	
	if a[nameCol]:								# sample name
		if b[nameCol]:
			return symbolsort.nomenCompare(str(a[nameCol]), str(b[nameCol]))
		else:
			return -1
	elif b[nameCol]:
		return 1

	return 0

###--- Classes ---###

class HTSampleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_sample table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for high-throughput expression
	#	samples, collates results, writes tab-delimited text file

	def cleanOrganisms(self):
		# go through the organism column and swap the pieces of any organisms containing a comma
		# (eg- "mouse, laboratory" becomes "laboratory mouse")

		organismCol = Gatherer.columnNumber(self.finalColumns, 'organism')
		for row in self.finalResults:
			row[organismCol] = cleanOrganism(row[organismCol])
		return
		
	def addAgeMinMax(self):
		# compute and add the ageMin and ageMax fields, adding them to the list of 'cols' and to each
		# sample in 'rows'
		
		self.finalColumns.append('ageMin')
		self.finalColumns.append('ageMax')
		ageCol = Gatherer.columnNumber(self.finalColumns, 'age')
		
		for row in self.finalResults:
			age, ageMin, ageMax = AgeUtils.getAgeMinMax(row[ageCol])
			row[ageCol] = age
			row.append(ageMin)
			row.append(ageMax)

		logger.debug('Computed ageMin/Max for %d samples' % len(self.finalResults))
		return
	
	def applySequenceNumbers(self):
		# re-order the given sample rows and append a sequence number for each

		cacheColumns(self.finalColumns)
		self.finalResults.sort(compareSamples)
		logger.debug('Sorted %d samples' % len(self.finalResults))

		i = 0
		self.finalColumns.append('sequence_num')
		for sample in self.finalResults:
			i = i + 1
			sample.append(i)
		logger.debug('Applied sequence numbers')

		return samples

	def collateResults(self):
		cols, rows = self.results[0]
		if not rows:
			cols, rows = self.getFakeData()
		
		self.finalColumns = cols
		self.finalResults = rows
		
		self.convertFinalResultsToList()
		self.cleanOrganisms()
		self.addAgeMinMax()
		self.applySequenceNumbers()
		return
		
###--- globals ---###

cmds = [
	'''select t._Experiment_key, r.term as relevancy, t._Sample_key, s.name, s._Genotype_key,
			o.commonName as organism, x.term as sex, s.age, s._Emapa_key, g.stage as theiler_stage
		from %s t
		inner join gxd_htsample s on (t._Sample_key = s._Sample_key)
		inner join voc_term r on (s._Relevance_key = r._Term_key)
		inner join mgi_organism o on (s._Organism_key = o._Organism_key)
		inner join voc_term x on (s._Sex_key = x._Term_key)
		left outer join gxd_theilerstage g on (s._Stage_key = g._Stage_key)
		order by t._Experiment_key, s.name''' % samples.getSampleTempTable()
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'_Sample_key', '_Experiment_key', 'name', '_Genotype_key', 'organism', 'sex', 'age',
	'ageMin', 'ageMax', '_EMAPA_key', 'theiler_stage', 'relevancy', 'sequence_num',
	]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_sample'

# global instance of a HTSampleGatherer
gatherer = HTSampleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
