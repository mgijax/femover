#!/usr/local/bin/python
# 
# gathers data for the homology_cluster, homology_cluster_organism, and
# homology_cluster_organism_to_marker tables in the front-end database

import Gatherer
import symbolsort
import utils
import logger

###--- Globals ---###

# which organisms should be given priority when ordering
preferredOrganisms = [
	# the big three
	'human', 'mouse', 'rat',		

	# primates, alphabetical
	'chimpanzee', 'rhesus macaque',

	# mammals, alphabetical
	'cattle', 'dog',

	# others, alphabetical
	'chicken', 'zebrafish',
	]

###--- Functions ---###

def organismCompare (a, b):
	if a in preferredOrganisms:
		if b in preferredOrganisms:
			# 'a' and 'b' are both preferred, so which is first
			# in the list?

			aIndex = preferredOrganisms.index(a)
			bIndex = preferredOrganisms.index(b)

			if aIndex < bIndex:
				return -1
			elif aIndex > bIndex:
				return 1
			else:
				return 0
		else:
			# only 'a' is preferred, so it goes first
			return -1

	elif b in preferredOrganisms:
		# only 'b' is preferred, so it goes first
		return 1

	# at this point, neither 'a' nor 'b' are preferred, so just compare
	# them against each other

	return cmp(a, b)

def markerCompare (a, b):
	return symbolsort.nomenCompare (a[0], b[0])

###--- Classes ---###

class Cluster:
	def __init__ (self,
		clusterKey, 
		accID, 
		clusterType, 
		source, 
		version, 
		date
		):

		self.key = clusterKey
		self.accID = accID
		self.clusterType = clusterType
		self.source = source
		self.version = version
		self.date = date 
		self.organisms = {}	# organism -> [ marker key 1, ... ]
		self.markers = {}	# marker key -> [ symbol, organism,
					#    qualifier, refsKey, markerKey ]
		return

	def addRow (self,
		markerKey, 
		symbol,
		seqNum, 
		organism, 
		qualifier, 
		refsKey
		):

		self.markers[markerKey] = [ symbol, organism, qualifier,
			refsKey, markerKey ]

		if not self.organisms.has_key(organism):
			self.organisms[organism] = []
		self.organisms[organism].append (markerKey)
		return 

	def getClusterData (self):
		return (self.key, self.accID, self.clusterType, self.source,
			self.version, self.date)

	def getOrganisms (self):
		organisms = self.organisms.keys()
		organisms.sort (organismCompare)
		return organisms

	def getMarkers (self, organism):
		# collect the markers for this organism and return list of
		# them, each as [ symbol, organism, qualifier, refsKey,
		# markerKey ]

		markers = []
		for markerKey in self.organisms[organism]:
			markers.append (self.markers[markerKey])

		# sort the markers for this organism

		markers.sort (markerCompare)
		return markers

class HomologyClusterGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the homology_cluster flower
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for the 
	#	homology cluster flower tables,	collates results, writes
	#	tab-delimited text file

	def collateResults (self):

		cols, rows = self.results[0]

		clusterKeyPos = Gatherer.columnNumber (cols, '_Cluster_key')
		typePos = Gatherer.columnNumber (cols, 'clusterType')
		sourcePos = Gatherer.columnNumber (cols, 'source')
		idPos = Gatherer.columnNumber (cols, 'clusterID')
		versionPos = Gatherer.columnNumber (cols, 'version')
		datePos = Gatherer.columnNumber (cols, 'cluster_date')
		markerPos = Gatherer.columnNumber (cols, '_Marker_key')
		symbolPos = Gatherer.columnNumber (cols, 'symbol')
		seqNumPos = Gatherer.columnNumber (cols, 'sequenceNum')
		organismPos = Gatherer.columnNumber (cols, 'organism')
		qualifierPos = Gatherer.columnNumber (cols, 'qualifier')
		refsPos = Gatherer.columnNumber (cols, '_Refs_key')

		clusters = {}	# cluster key -> Cluster object

		# Time to build Cluster objects using our data from the
		# database.  Note that no particular ordering of records from
		# the results is assumed.

		for row in rows:
			clusterKey = row[clusterKeyPos]

			if not clusters.has_key(clusterKey):
				# We've not yet seen this cluster, so make a
				# new object.

				clusterType = row[typePos]
				source = row[sourcePos]
				accID = row[idPos]
				version = row[versionPos]
				date = row[datePos]
				if date:
					date = str(date)[:10]

				cluster = Cluster (clusterKey, accID,
					clusterType, source, version, date)

				clusters[clusterKey] = cluster
			else:
				# Look up the cluster we already created.
				cluster = clusters[clusterKey]

			markerKey = row[markerPos]
			symbol = row[symbolPos]
			seqNum = row[seqNumPos]
			organism = row[organismPos]
			qualifier = row[qualifierPos]
			refsKey = row[refsPos]

			# cleanup of organism text

			organism = organism.replace(', laboratory', '')
			organism = organism.replace(', domestic', '')

			cluster.addRow (markerKey, symbol, seqNum, organism, 
				qualifier, refsKey)

		# Our data is all in memory now.  Time to collate and produce
		# our result sets.

		hcCols = [ 'clusterKey', 'clusterID', 'version',
			'cluster_date', 'source' ]
		hcRows = []

		hcoCols = [ 'clusterOrganismKey', 'clusterKey', 'organism',
			'seqNum' ]
		hcoRows = []
		coKey = 0

		hcotmCols = [ 'clusterOrganismKey', '_Marker_key',
			'_Refs_key', 'qualifier', 'seqNum' ]
		hcotmRows = []

		hccCols = [ 'clusterKey', 'mouseMarkerCount',
			'humanMarkerCount', 'ratMarkerCount',
			'cattleMarkerCount', 'chimpMarkerCount',
			'dogMarkerCount', ]
		hccRows = []

		clusterKeys = clusters.keys()
		clusterKeys.sort()

		logger.debug ('%d clusters' % len(clusterKeys))

		for clusterKey in clusterKeys:
			cluster = clusters[clusterKey]

			counts = {}

			(key, accID, clusterType, source, version, date) = \
				cluster.getClusterData()

			hcRows.append ( [key, accID, version, date, source] )
			coSeqNum = 0
			hcotmSeqNum = 0

			for organism in cluster.getOrganisms():
				coKey = coKey + 1
				coSeqNum = coSeqNum + 1

				hcoRows.append ( [coKey, key,
					utils.cleanupOrganism(organism),
					coSeqNum] )

				markers = cluster.getMarkers(organism)
				counts[organism] = len(markers)

				for [ symbol, organism, qualifier, refsKey,
					markerKey ] in markers:

					hcotmSeqNum = hcotmSeqNum + 1

					hcotmRows.append ( [coKey, markerKey,
						refsKey, qualifier,
						hcotmSeqNum] )

			hccRow = [ clusterKey ]
			for organism in [ 'mouse', 'human', 'rat', 'cattle',
					'chimpanzee', 'dog' ]:
				if counts.has_key(organism):
					hccRow.append (counts[organism])
				else:
					hccRow.append (0)
			hccRows.append (hccRow)

		logger.debug ('%d organisms for clusters' % len(hcoRows))
		logger.debug ('%d markers in clusters' % len(hcotmRows))

		self.output.append ( (hcCols, hcRows) )
		self.output.append ( (hcoCols, hcoRows) )
		self.output.append ( (hcotmCols, hcotmRows) )
		self.output.append ( (hccCols, hccRows) ) 
		return

###--- globals ---###

cmds = [
	'''select mc._Cluster_key,
		typ.term as clusterType,
		src.term as source,
		mc.clusterID,
		mc.version,
		mc.cluster_date,
		mcm._Marker_key,
		mm.symbol,
		mcm.sequenceNum,
		mo.commonName as organism,
		null as qualifier,
		null as _Refs_key
	from mrk_cluster mc, mrk_clustermember mcm, mrk_marker mm,
		voc_term src, voc_term typ, mgi_organism mo
	where mc._Cluster_key = mcm._Cluster_key
		and mcm._Marker_key = mm._Marker_key
		and mc._ClusterType_key = typ._Term_key
		and mm._Organism_key = mo._Organism_key
		and mc._ClusterSource_key = src._Term_key''',
	]

# data about files to be written; for each:  (filename prefix, list of field
#	names in order to be written, name of table to be loaded)
files = [
	('homology_cluster',
		[ 'clusterKey', 'clusterID', 'version', 'cluster_date',
			'source', ],
		'homology_cluster'),

	('homology_cluster_organism',
		[ 'clusterOrganismKey', 'clusterKey', 'organism', 'seqNum' ],
		'homology_cluster_organism'),

	('homology_cluster_organism_to_marker',
		[ Gatherer.AUTO, 'clusterOrganismKey', '_Marker_key',
			'_Refs_key', 'qualifier', 'seqNum' ],
		'homology_cluster_organism_to_marker'),

	('homology_cluster_counts',
		[ 'clusterKey', 'mouseMarkerCount', 'humanMarkerCount',
			'ratMarkerCount', 'cattleMarkerCount',
			'chimpMarkerCount', 'dogMarkerCount', ],
		'homology_cluster_counts'),
	]

# global instance of a HomologyClusterGatherer
gatherer = HomologyClusterGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
