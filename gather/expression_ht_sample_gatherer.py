#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample' table in the front-end database

import Gatherer
import logger
import VocabSorter
import AgeUtils
from expression_ht import samples

###--- Functions ---###
###--- TODO : eliminate this section once we have real sample data (It's a hack.) ---###

organismMap = {
	'mus musculus' : 'mouse',
	'mus musculus domesticus' : 'mouse',
	'mus musculus musculus' : 'mouse',
	'mouse' : 'mouse',
	'house mouse' : 'mouse',
	'c57bl/6 mouse' : 'mouse',
	'homo sapien' : 'human',
	'homo sapiens' : 'human',
	'rattus norvegicus' : 'rat',
	}

def cleanUpOrganism(organism):
	if not organism:
		return organism

	t = organism.strip().lower().replace('_', ' ').replace('.', ' ')
	if t in organismMap:
		return organismMap[t]
	return organism

def getCharacteristic(sample, name):
	if 'characteristic' in sample:
		lowerName = name.lower()
		characteristics = sample['characteristic']
		if type(characteristics) != type([]):
			characteristics = [ characteristics ]
			
		for characteristic in characteristics:
			if 'category' in characteristic:
				if str(characteristic['category']).lower() == lowerName:
					return characteristic['value'] 
	return None

#import re
#ageRegex = re.compile('^E([0-9]{1,2}\.?[0-9]?)$')
#def getAge(ageString):
#	# returns (age as a string, ageMin, ageMax)
#	if not ageString:
#		return None, 24, 28
#	
#	match = ageRegex.match(str(ageString))
#	if not match:
#		return ageString, 24, 28
#	
#	return ageString, float(match.group(1)), float(match.group(1))

###--- functions to keep ---###

expKeyCol = None
relevanceCol = None
ageMinCol = None
ageMaxCol = None
structureCol = None
tsCol = None
organismCol = None
nameCol = None
def cacheColumns(cols):
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
	'human' : 2,
	'rat'	: 3,
	}
def compareSamples(a, b):
	# sort samples by:
	#	0. experiment key, 1. relevance (yes first), 2. ageMin, 3. ageMax, 4. structure (topologically),
	#   5. Theiler stage, 6. organism (mouse, human, rat, others), 7. sample name
	# assumes: cacheColumns() was called previously
	
	byExpKey = cmp(a[expKeyCol], b[expKeyCol])				# experiment key
	if byExpKey != 0:
		return byExpKey

	byRelevance = cmp(a[relevanceCol], b[relevanceCol]) * -1	# relevance in descending order: yes, no, None
	if byRelevance != 0:
		return byRelevance

	if a[ageMinCol]:								# ageMin
		if b[ageMinCol]:
			byAgeMin = cmp(a[ageMinCol], b[ageMinCol])
			if byAgeMin != 0:
				return byAgeMin 
		else:
			return -1
	elif b[ageMinCol]:
		return 1
		
	if a[ageMaxCol]:								# ageMax
		if b[ageMaxCol]:
			byAgeMax = cmp(a[ageMaxCol], b[ageMaxCol])
			if byAgeMax != 0:
				return byAgeMax
		else:
			return -1
	elif b[ageMaxCol]:
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
	
	if a[tsCol]:								# Theiler Stage
		if b[tsCol]:
			byTS = cmp(int(a[tsCol]), int(b[tsCol]))
			if byTS != 0:
				return byTS
		else:
			return -1
	elif b[tsCol]:
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
			return -1
	elif b[organismCol]:
		return 1
	
	if a[nameCol]:								# sample name
		if b[nameCol]:
			return cmp(str(a[nameCol]).lower(), str(b[nameCol]).lower())
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

	def getFakeData(self):
		# TODO -- remove this method and go back to the superclass method; This is a hack until we have
		# curated sample data.
		
		cols = [ '_Sample_key', '_Experiment_key', 'name', '_Genotype_key', 'organism', 'sex', 'age',
			'_EMAPA_key', 'theiler_stage', 'relevancy', ]
		rows = []

		sampleKey = 0
		for experimentID in samples.getExperimentsWithSamples():
			experimentKey = samples.getExperimentKey(experimentID)
			if not experimentKey:
				# sample file has more experiments than we have in latest data load, so skip any
				# that aren't in the database
				continue
			
			for sample in samples.getSamples(experimentID):
				sampleKey = sampleKey + 1
				
				row = [ sampleKey, experimentKey ]
				
				name = 'Been through the desert on a sample with no name (%d)' % sampleKey
				if 'source' in sample:
					if 'name' in sample['source']:
						name = sample['source']['name']
				row.append(name)
				
				row.append(-1)		# genotype key: not specified

				row.append(cleanUpOrganism(getCharacteristic(sample, 'organism')))
				row.append(getCharacteristic(sample, 'sex'))

#				age, ageMin, ageMax = getAge(getCharacteristic(sample, 'age'))
#				row = row + [ age, ageMin, ageMax ]
				age = getCharacteristic(sample, 'age')
				row.append(age)

				emapaKey, startStage = samples.getEmapa(getCharacteristic(sample, 'organism part'))
				row.append(emapaKey)
				row.append(startStage)
				
				if str(row[4]).lower().find('mouse') >= 0:
					row.append("Yes")
				else:
					row.append("Non-mouse sample: no data stored")

				rows.append(row)
				
		logger.debug("Hacked in %d sample rows" % len(self.finalResults))
		return cols, rows

	def addAgeMinMax(self):
		# compute and add the ageMin and ageMax fields, adding them to the list of 'cols' and to each
		# sample in 'rows'
		
		self.finalColumns.append('ageMin')
		self.finalColumns.append('ageMax')
		ageCol = Gatherer.columnNumber(self.finalColumns, 'age')
		
		for row in self.finalResults:
#			age, ageMin, ageMax = getAge(row[ageCol])
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
		
		self.addAgeMinMax()
		self.applySequenceNumbers()
		return
		
###--- globals ---###

cmds = [
	'''select t._Experiment_key, r.term as relevancy, t._Sample_key, s.name, s._Genotype_key,
			o.commonName as organism, x.term as sex, s.age, s._Emapa_key, g.stage as theiler_stage
		from %s t, gxd_htsample s, voc_term r, mgi_organism o, voc_term x, gxd_theilerstage g
		where t._Sample_key = s._Sample_key
			and s._Organism_key = o._Organism_key
			and s._Sex_key::integer = x._Term_key
			and s._Stage_key = g._Stage_key
			and s._Relevance_key = r._Term_key
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
