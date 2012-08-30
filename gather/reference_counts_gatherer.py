#!/usr/local/bin/python
# 
# gathers data for the 'referenceCounts' table in the front-end database

# NOTE: To add more counts:
#	1. add a fieldname for the count as a global (like MarkerCount)
#	2. add a new query to 'cmds' in the main program
#	3. add processing for the new query to collateResults(), to tie the
#		query results to the new fieldname in each reference's
#		dictionary
#	4. add the new fieldname to fieldOrder in the main program

import Gatherer
import logger

###--- Globals ---###

MarkerCount = 'markerCount'
ProbeCount = 'probeCount'
MappingCount = 'mappingCount'
GxdIndexCount = 'gxdIndexCount'
GxdResultCount = 'gxdResultCount'
GxdStructureCount = 'gxdStructureCount'
GxdAssayCount = 'gxdAssayCount'
AlleleCount = 'alleleCount'
SequenceCount = 'sequenceCount'

error = 'referenceCountsGatherer.error'

###--- Classes ---###

class ReferenceCountsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the referenceCounts table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for reference counts,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		# Purpose: to combine the results of the various queries into
		#	one single list of final results, with one row per
		#	reference

		# list of count types (like field names)
		counts = []

		# initialize dictionary for collecting data per reference
		#	d[reference key] = { count type : count }
		d = {}
		for row in self.results[0][1]:
			d[row[0]] = {}

		# counts to add in this order, with each tuple being:
		#	(set of results, count constant, count column)

		toAdd = [ (self.results[1], MarkerCount, 'numMarkers'),
			(self.results[2], ProbeCount, 'numProbes'),
			(self.results[3], MappingCount, 'numExperiments'),
			(self.results[4], GxdIndexCount, 'numIndex'),
			(self.results[5], GxdResultCount, 'numResults'),
			(self.results[6], GxdStructureCount, 'numStructures'),
			(self.results[7], GxdAssayCount, 'numAssays'),
			(self.results[8], AlleleCount, 'numAlleles'),
			(self.results[9], SequenceCount, 'numSequences'),
			]
		for (r, countName, colName) in toAdd:
			logger.debug ('Processing %s, %d rows' % (countName,
				len(r[1])) )
			counts.append (countName)
			refKeyCol = Gatherer.columnNumber (r[0], '_Refs_key')
			countCol = Gatherer.columnNumber (r[0], colName)

			for row in r[1]:
				refKey = row[refKeyCol]
				if d.has_key(refKey):
					d[row[refKeyCol]][countName] = \
						row[countCol]
				else:
					raise error, \
					'Unknown reference key: %d' % refKey

		# compile the list of collated counts in self.finalResults
		# and the list of columns in self.finalColumns

		self.finalResults = []
		referenceKeys = d.keys()
		referenceKeys.sort()

		self.finalColumns = [ '_Refs_key' ] + counts

		for referenceKey in referenceKeys:
			row = [ referenceKey ]
			for count in counts:
				if d[referenceKey].has_key(count):
					row.append (d[referenceKey][count])
				else:
					row.append (0)

			self.finalResults.append (row)
		return

###--- globals ---###

cmds = [
	# all references
	'select m._Refs_key from bib_refs m',

	# count of markers for each reference
	'''select m._Refs_key, count(1) as numMarkers
		from mrk_reference m, mrk_marker mm
		where m._Marker_key = mm._Marker_key
			and mm._Marker_Status_key in (1,3)
		group by m._Refs_key''',

	# count of probes
	'''select _Refs_key, count(1) as numProbes
		from prb_reference
		group by _Refs_key''',

	# count of mapping experiments
	'''select _Refs_key, count(1) as numExperiments
		from mld_expts
		group by _Refs_key''',

	# count of GXD literature index entries
	'''select _Refs_key, count(1) as numIndex
		from gxd_index
		group by _Refs_key''',

	# count of expression results
	'''select a._Refs_key, count(distinct e._Expression_key) as numResults
		from gxd_assay a, gxd_expression e
		where a._Assay_key = e._Assay_key
			and e.isForGXD = 1
		group by a._Refs_key''',

	# count of structures with expression results
	'''select _Refs_key, count(distinct _Structure_key) as numStructures
		from gxd_expression
		where isForGXD = 1
		group by _Refs_key''',

	# count of expression assays
	'''select a._Refs_key, count(distinct a._Assay_key) as numAssays
		from gxd_assay a, gxd_expression e
		where a._Assay_key = e._Assay_key
			and e.isForGXD = 1
		group by a._Refs_key''',

	# count of alleles
	'''select r._Refs_key as _Refs_key,
			count(distinct a._Allele_key) as numAlleles
		from mgi_reference_assoc r, all_allele a
		where r._Object_key = a._Allele_key
			and r._MGIType_key = 11
			and a.isWildType != 1
		group by r._Refs_key''',

	# count of sequences
	'''select _Refs_key as _Refs_key,
			count(distinct _Object_key) as numSequences
		from mgi_reference_assoc
		where _MGIType_key = 19
		group by _Refs_key''',
	]

# order of fields (from the query results) to be written to the output file
fieldOrder = [ '_Refs_key', MarkerCount, ProbeCount, MappingCount,
	GxdIndexCount, GxdResultCount, GxdStructureCount,
	GxdAssayCount, AlleleCount, SequenceCount ]

# prefix for the filename of the output file
filenamePrefix = 'reference_counts'

# global instance of a ReferenceCountsGatherer
gatherer = ReferenceCountsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
