#!/usr/local/bin/python
# 
# gathers data for the 'statistic' and 'statistic_group' tables in fe
# front-end database
#
#

import dbAgnostic
import Gatherer
import logger

### Constants ###

# MGI_Statistic _mgitype_key
STATISTIC_TYPE_KEY = 34

### Classes ###

class StatisticGatherer (Gatherer.CachingMultiFileGatherer):
	
		
	### Queries ###
			
	def queryStatistics(self):
		"""
		Query for all MGI statistics objects
		"""
		pass
		
		
		return []
	
	### Building specific tables ###

	def buildStatistic(self):
		"""
		build the statistic table
		"""
		
		statistics = []
		
		cols, rows = self.results[0]
		
		statisticKeyCol = Gatherer.columnNumber (cols, '_statistic_key')
		nameCol = Gatherer.columnNumber (cols, 'name')
		abbrevCol = Gatherer.columnNumber (cols, 'abbreviation')
		valueCol = Gatherer.columnNumber (cols, 'intvalue')
		seqnumCol = Gatherer.columnNumber (cols, 'sequencenum')
		groupCol = Gatherer.columnNumber (cols, 'group')
		groupSeqnumCol = Gatherer.columnNumber (cols, 'group_sequencenum')
			
		for row in rows:
			
			statisticKey = row[statisticKeyCol]
			name = row[nameCol]
			abbreviation = row[abbrevCol]
			value = row[valueCol]
			seqnum = row[seqnumCol]
			group = row[groupCol]
			groupSeqnum = row[groupSeqnumCol]
			
			statistics.append([
							statisticKey,
							name,
							abbreviation,
							value,
							group,
							seqnum,
							groupSeqnum
							])
			
			
		self.addRows('statistic', statistics)
		
		
	
	def collateResults (self):
		"""
		Process the results of cmds queries
		
		Builds all the tables in files 
			e.g. statistic, statistic_group
		"""
		
		# Build the tables for each batch
		
		self.buildStatistic()
		
		

###--- globals ---###

cmds = [
	
	#
	# 0. Query all statistics
	#
	'''
	select s._statistic_key,
	s.name, 
	s.abbreviation,
	meas.intvalue,
	meas.timerecorded,
	mmem.sequencenum,
	mset.name as group,
	mset.sequencenum as group_sequencenum
	from mgi_statistic s 
	cross join lateral ( 		-- Get the latest measurement for each stat
		select _m.intvalue,
		_m.timerecorded
		from mgi_measurement _m 
		where _m._statistic_key = s._statistic_key 
		order by _m.timerecorded desc
		limit 1
	) meas
	join mgi_setmember mmem on  -- Get the statistic groups and sequencenums
		mmem._object_key = s._statistic_key
	join mgi_set mset on
		mset._set_key = mmem._set_key
		and mset._mgitype_key = %d
	''' % (STATISTIC_TYPE_KEY),
	
	]

# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('statistic',
		[  'statistic_key', 'name', 'abbreviation', 
			'value', 'group_name', 'sequencenum', 'group_sequencenum' ],
		[ Gatherer.AUTO, 'statistic_key', 'name', 'abbreviation', 
			'value', 'group_name', 'sequencenum', 'group_sequencenum' ],
	),
	]

# global instance of a StatisticGatherer
gatherer = StatisticGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
