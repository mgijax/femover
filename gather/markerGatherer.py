#!/usr/local/bin/python
# 
# gathers data for the 'marker' table in the front-end database

import Gatherer
import sybaseUtil

###--- Classes ---###

class MarkerGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for markers, collates results,
	#	writes tab-delimited text file

	def collateResults (self):
		# Purpose: we override this method because we need to
		#	concatenate two result sets into a single, final set
		#	of results

		self.finalResults = self.results[0] + self.results[1]
		return

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single marker, rather
		#	than for all markers

		if self.keyField == 'markerKey':
			return 'm._Marker_key = %s' % self.keyValue
		return ''

	def postprocessResults (self):
		# Purpose: override method to provide key-based lookups

		for r in self.finalResults:
			r['status'] = sybaseUtil.resolve (
				r['_Marker_Status_key'], 'MRK_Status',
				'_Marker_Status_key', 'status')
			r['logicalDB'] = sybaseUtil.resolve (
				r['_LogicalDB_key'], 'ACC_LogicalDB',
				'_LogicalDB_key', 'name')
			r['markerType'] = sybaseUtil.resolve (
				r['_Marker_Type_key'], 'MRK_Types',
				'_Marker_Type_key', 'name')
			r['organism'] = sybaseUtil.resolve (
				r['_Organism_key'], 'MGI_Organism',
				'_Organism_key', 'commonName')
		return

###--- globals ---###

cmds = [
	# Gather the list of valid mouse markers for this report
	'''select m._Marker_key,
		m.symbol,
		m.name, 
		m._Marker_Type_key,
		m._Organism_key,
		a.accID, 
		a._LogicalDB_key,
		"" as subtype,
		m._Marker_Status_key
	from MRK_Marker m,
		ACC_Accession a
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and a._LogicalDB_key = 1
		and a.preferred = 1
		and m._Organism_key = 1 %s''',

	# and non-mouse markers 
	'''select m._Marker_key,
		m.symbol,
		m.name, 
		m._Marker_Type_key,
		m._Organism_key,
		a.accID, 
		a._LogicalDB_key,
		"" as subtype,
		m._Marker_Status_key
	from MRK_Marker m,
		ACC_Accession a,
		ACC_LogicalDB ldb
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and m._Organism_key != 1
		and a.preferred = 1
		and a._LogicalDB_key = ldb._LogicalDB_key
		and ldb._Organism_key = m._Organism_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', 'symbol', 'name', 'markerType', 'subtype',
	'organism', 'accID', 'logicalDB', 'status' ]

# prefix for the filename of the output file
filenamePrefix = 'marker'

# global instance of a MarkerGatherer
gatherer = MarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
