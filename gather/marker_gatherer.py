#!/usr/local/bin/python
# 
# gathers data for the 'marker' table in the front-end database

import Gatherer
import logger
import GOGraphs
import utils

# list of markers in mrk_location_cache
locationLookup = {}

coordinateDisplay = 'Chr%s:%s-%s (%s)'
locationDisplay1 = 'Chr%s %s cM'
locationDisplay2 = 'Chr%s %s'
locationDisplay3 = 'Chr%s syntenic'
locationDisplay4 = 'Chr%s QTL'
locationDisplay5 = 'Chr Unknown'

###--- Functions ---###

def getLocationDisplay (marker, organism):
	#
	# returns location display
	# returns coordinate display
	# returns build identifier (version)
	#

	if not locationLookup.has_key(marker):
		return '', '', ''

	gchromosome = locationLookup[marker][1]
	chromosome = locationLookup[marker][2]
	startCoordinate = locationLookup[marker][3]
	endCoordinate = locationLookup[marker][4]
	strand = locationLookup[marker][5]
	cmoffset = locationLookup[marker][6]
	cytooffset = locationLookup[marker][7]
	buildIdentifier = locationLookup[marker][8]
	markerType = locationLookup[marker][9]

	if organism == 1:
		if chromosome == 'UN' or cmoffset == -999:
			location = locationDisplay5
		elif markerType == 6:
			location = locationDisplay4 % (chromosome)
		elif cmoffset == -1:
			location = locationDisplay3 % (chromosome)
		else:
			location = locationDisplay1 % (chromosome, cmoffset)
	else:
		location = locationDisplay2 % (chromosome, cytooffset)

	if startCoordinate:
		coordinate = coordinateDisplay \
			% (gchromosome, startCoordinate, endCoordinate, strand)
	else:
		coordinate = ''

	return location, coordinate, buildIdentifier

###--- Classes ---###

class MarkerGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for markers, collates results,
	#	writes tab-delimited text file

	def collateResults (self):
		global locationLookup

		self.featureTypes = {}	# marker key -> [ feature types ]
		self.ids = {}		# marker key -> (id, logical db key)
		self.inRefGenome = {}	# marker key -> 0/1

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

		# reference genome flags are in query 2

		cols, rows = self.results[2]
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')
		flagCol = Gatherer.columnNumber (cols, 'isReferenceGene')

		for row in rows:
			self.inRefGenome[row[keyCol]] = row[flagCol]
	
		logger.debug ('Found %d reference genome flags' % \
			len(self.inRefGenome))

		
                # sql (3)
                (cols, rows) = self.results[3]

                # set of columns for common sql fields
		keyCol = Gatherer.columnNumber (cols, '_Marker_key')

                for row in rows:
                        locationLookup[row[keyCol]] = row

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
			organism = r[orgCol]

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

			if self.inRefGenome.has_key(markerKey):
				isInRefGenome = self.inRefGenome[markerKey]
			else:
				isInRefGenome = 0

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
			self.addColumn ('organism', utils.cleanupOrganism(
				Gatherer.resolve (r[orgCol], 'mgi_organism',
				'_Organism_key', 'commonName')),
				r, self.finalColumns)
			self.addColumn ('hasGOGraph',
				GOGraphs.hasGOGraph(accid),
				r, self.finalColumns)
			self.addColumn ('isInReferenceGenome', isInRefGenome,
				r, self.finalColumns)

			# location and coordinate information
		        location, coordinate, buildIdentifier = getLocationDisplay(markerKey, organism)
			self.addColumn ('location_display', location, r, self.finalColumns)
			self.addColumn ('coordinate_display', coordinate, r, self.finalColumns)
			self.addColumn ('build_identifier', buildIdentifier, r, self.finalColumns)

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

	# 2. markers in the reference genome project
	'select _Marker_key, isReferenceGene from GO_Tracking',

	# 3. all markers with mrk_location_cache
	'''select distinct _Marker_key, genomicchromosome, chromosome,
                startcoordinate::varchar, endcoordinate::varchar,
                strand, cmoffset, cytogeneticoffset, version,
                _marker_type_key
            from mrk_location_cache''',

	# 4. all markers
	'''select _Marker_key, symbol, name, _Marker_Type_key, _Organism_key,
		_Marker_Status_key
	from mrk_marker''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [ '_Marker_key', 'symbol', 'name', 'markerType', 'subtype',
	'organism', 'accID', 'logicalDB', 'status', 'hasGOGraph',
	'isInReferenceGenome', 
	'location_display', 'coordinate_display', 'build_identifier' ]

# prefix for the filename of the output file
filenamePrefix = 'marker'

# global instance of a MarkerGatherer
gatherer = MarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
