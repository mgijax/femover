#!/usr/local/bin/python
# 
# gathers data for the 'expression_ht_sample' table in the front-end database

import Gatherer
import logger
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

import re
ageRegex = re.compile('^E([0-9]{1,2}\.?[0-9]?)$')
def getAge(ageString):
	# returns (age as a string, ageMin, ageMax)
	if not ageString:
		return None, 24, 28
	
	match = ageRegex.match(str(ageString))
	if not match:
		return ageString, 24, 28
	
	return ageString, float(match.group(1)), float(match.group(1))

###--- Classes ---###

class HTSampleGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the expression_ht_sample table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for high-throughput expression
	#	samples, collates results, writes tab-delimited text file

	def collateResults(self):
		# TODO -- remove this method and go back to the superclass method; This is a hack until we have
		# curated sample data.
		
		cols, rows = self.results[0]
		if rows:
			return Gatherer.Gatherer.collateResults(self)
		
		self.finalColumns = [
			'_Sample_key', '_Experiment_key', 'name', '_Genotype_key', 'organism', 'sex', 'age',
			'ageMin', 'ageMax', '_EMAPA_key', 'theiler_stage', 'relevancy', 'sequence_num',
			]
		self.finalResults = []
		
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

				age, ageMin, ageMax = getAge(getCharacteristic(sample, 'age'))
				row = row + [ age, ageMin, ageMax ]

				emapaKey, startStage = samples.getEmapa(getCharacteristic(sample, 'organism part'))
				row.append(emapaKey)
				row.append(startStage)
				
				if str(row[4]).lower().find('mouse') >= 0:
					row.append("Yes")
				else:
					row.append("Non-mouse sample: no data stored")

				row.append(sampleKey)		# sequence num
				self.finalResults.append(row)
				
		logger.debug("Hacked in %d sample rows" % len(self.finalResults))
		return
		
###--- globals ---###

cmds = [
	'''select t._Experiment_key, r.term as relevancy, t._Sample_key, s.name, s._Genotype_key,
			o.commonName as organism, x.term as sex, s.age, s.ageMin, s.ageMax, s._Emapa_key,
			g.stage as theiler_stage,
			row_number() over (order by t._Experiment_key, s.name) as sequence_num
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
