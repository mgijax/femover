#!/usr/local/bin/python
# 
# gathers data for the 'marker' table in the front-end database

import Gatherer
import logger
import GOGraphs

###--- Classes ---###

class MarkerGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for markers, collates results,
	#	writes tab-delimited text file

	def collateResults (self):
		self.featureTypes = {}	# marker key -> [ feature types ]
		self.ids = {}		# marker key -> (id, logical db key)

		# feature types from MCV vocab are in query 0 results

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		termCol = Gatherer.columnNumber (cols, 'directTerms')

		for row in rows:
			key = row[keyCol]
			term = row[termCol]

			if self.featureTypes.has_key(key):
				self.featureTypes[key].append (term)
			else:
				self.featureTypes[key] = [ term ]

		logger.debug ('Found %d MCV terms for %d markers' % (
			len(rows), len(self.featureTypes) ) )

		# IDs are retrieved in query 1

		cols, rows = self.results[1]
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		idCol = Gatherer.columnNumber (cols, 'accid')
		ldbCol = Gatherer.columnNumber (cols, '_LogicalDB_key')

		for row in rows:
			self.ids[row[keyCol]] = (row[idCol], row[ldbCol])
	
		logger.debug ('Found %d primary IDs for markers' % \
			len(self.ids))

		# last query has the bulk of the data
		self.finalColumns = self.results[-1][0]
		self.finalResults = self.results[-1][1]

		logger.debug ('Found %d markers' % len(self.finalResults))
		return

	def postprocessResults (self):
		# Purpose: override method to provide key-based lookups

		self.convertFinalResultsToList()

		statusCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_Status_key')
		typeCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_Type_key')
		orgCol = Gatherer.columnNumber (self.finalColumns,
			'_Organism_key')
		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Marker_key')

		for r in self.finalResults:
			markerKey = r[keyCol]
			if self.featureTypes.has_key(markerKey):
				feature = ', '.join (
					self.featureTypes[markerKey])
			else:
				feature = None

			# look up cached ID and logical database
			if self.ids.has_key(markerKey):
				accid, ldb = self.ids[markerKey]
				ldbName = Gatherer.resolve (ldb,
					'acc_logicaldb', '_LogicalDB_key',
					'name')
			else:
				accid = None
				ldb = None
				ldbName = None

			self.addColumn ('accid', accid, r, self.finalColumns)
			self.addColumn ('subtype', feature, r,
				self.finalColumns)
			self.addColumn ('status', Gatherer.resolve (
				r[statusCol], 'mrk_status',
				'_Marker_Status_key', 'status'),
				r, self.finalColumns)
			self.addColumn ('logicalDB', ldbName, r,
				self.finalColumns)
			self.addColumn ('markerType', Gatherer.resolve (
				r[typeCol], 'mrk_types', '_Marker_Type_key',
				'name'), r, self.finalColumns)
			self.addColumn ('organism', Gatherer.resolve (
				r[orgCol], 'mgi_organism', '_Organism_key',
				'commonName'), r, self.finalColumns)
			self.addColumn ('hasGOGraph',
				GOGraphs.hasGOGraph(accid),
				r, self.finalColumns)
			self.addColumn ('hasGOOrthologyGraph',
				GOGraphs.hasGOOrthologyGraph(accid),
				r, self.finalColumns)
		return

###--- globals ---###

cmds = [
	# 0. get the MCV (feature type) annotations, from which we will build
	# the subtype for each marker
	'select distinct _Marker_key, directTerms from MRK_MCV_Cache',

	# 1. Gather the set of primary marker IDs
	# (mouse markers on the top of the union, non-mouse markers below)
	'''select m._Marker_key,
		a.accID, 
		a._LogicalDB_key
	from mrk_marker m,
		acc_accession a
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and a._LogicalDB_key = 1
		and a.preferred = 1
		and m._Organism_key = 1
	union
	select m._Marker_key,
		a.accID, 
		a._LogicalDB_key
	from mrk_marker m,
		acc_accession a,
		acc_logicaldb ldb
	where m._Marker_key = a._Object_key
		and a._MGIType_key = 2
		and m._Organism_key != 1
		and a.preferred = 1
		and a._LogicalDB_key = ldb._LogicalDB_key
		and ldb._Organism_key = m._Organism_key''',

	# all markers
	'''select _Marker_key, symbol, name, _Marker_Type_key, _Organism_key,
		_Marker_Status_key
	from mrk_marker''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', 'symbol', 'name', 'markerType', 'subtype',
	'organism', 'accID', 'logicalDB', 'status', 'hasGOGraph',
	'hasGOOrthologyGraph' ]

# prefix for the filename of the output file
filenamePrefix = 'marker'

# global instance of a MarkerGatherer
gatherer = MarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
