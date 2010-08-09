#!/usr/local/bin/python
# 
# gathers data for the 'marker' table in the front-end database

import Gatherer

###--- Classes ---###

class MarkerGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for markers, collates results,
	#	writes tab-delimited text file

	def postprocessResults (self):
		# Purpose: override method to provide key-based lookups

		self.convertFinalResultsToList()

		statusCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_Status_key')
		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')
		typeCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_Type_key')
		orgCol = Gatherer.columnNumber (self.finalColumns,
			'_Organism_key')

		for r in self.finalResults:
			self.addColumn ('status', Gatherer.resolve (
				r[statusCol], 'mrk_status',
				'_Marker_Status_key', 'status'),
				r, self.finalColumns)
			self.addColumn ('logicalDB', Gatherer.resolve (
				r[ldbCol], 'acc_logicaldb', '_LogicalDB_key',
				'name'), r, self.finalColumns)
			self.addColumn ('markerType', Gatherer.resolve (
				r[typeCol], 'mrk_types', '_Marker_Type_key',
				'name'), r, self.finalColumns)
			self.addColumn ('organism', Gatherer.resolve (
				r[orgCol], 'mgi_organism', '_Organism_key',
				'commonName'), r, self.finalColumns)
		return

###--- globals ---###

cmds = [
	# Gather the list of valid mouse markers for this report
	# (mouse markers on the top of the union, non-mouse markers below)
	'''select m._Marker_key,
		m.symbol,
		m.name, 
		m._Marker_Type_key,
		m._Organism_key,
		a.accID, 
		a._LogicalDB_key,
		'None' as subtype,
		m._Marker_Status_key
	from mrk_marker m,
		acc_accession a
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and a._LogicalDB_key = 1
		and a.preferred = 1
		and m._Organism_key = 1
	union
	select m._Marker_key,
		m.symbol,
		m.name, 
		m._Marker_Type_key,
		m._Organism_key,
		a.accID, 
		a._LogicalDB_key,
		'None' as subtype,
		m._Marker_Status_key
	from mrk_marker m,
		acc_accession a,
		acc_logicaldb ldb
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and m._Organism_key != 1
		and a.preferred = 1
		and a._LogicalDB_key != 4
		and a._LogicalDB_key = ldb._LogicalDB_key
		and ldb._Organism_key = m._Organism_key''',
	# exclude RapMap from rat marker IDs (logical database 4)
	# There are 15 rat markers that will be excluded because of this.
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