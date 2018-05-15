#!/usr/local/bin/python
# 
# gathers data for the 'strain_marker' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class StrainMarkerGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_marker table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for strain markers,
	#	collates results, writes tab-delimited text file

	def populateStrainCaches(self):
		# populate the global STRAIN_CACHE and STRAIN_ORDER variables
		
		self.strainCache = {}		# strain ID : [ name, strain key, sequence num ]
		self.strainOrder = []		# list of strain IDs in order to be displayed
		
		cols, rows = self.results[-2]
		keyCol = Gatherer.columnNumber(cols, '_Strain_key')
		nameCol = Gatherer.columnNumber(cols, 'strain')
		idCol = Gatherer.columnNumber(cols, 'accID')
		seqNumCol = Gatherer.columnNumber(cols, 'sequence_num')
		
		for row in rows:
			self.strainOrder.append(row[idCol])
			self.strainCache[row[idCol]] = [ row[nameCol], row[keyCol], row[seqNumCol] ]
		logger.debug('Got info for %d strains' % len(self.strainOrder))
		return
	
	def findMissingStrainsPerMarker(self):
		# returns { marker key : list of strain IDs with no data }
		
		markerCol = Gatherer.columnNumber(self.finalColumns, 'canonical_marker_key')
		strainCol = Gatherer.columnNumber(self.finalColumns, 'strain_id')
		
		existing = {}
		for row in self.finalResults:
			markerKey = row[markerCol]
			strainID = row[strainCol]

			if markerKey not in existing:
				existing[markerKey] = set()
			existing[markerKey].add(strainID)
			
		markers = existing.keys()
		markers.sort()
		
		ct = 0
		missing = {}
		for markerKey in markers:
			for strainID in self.strainOrder:
				if strainID not in existing[markerKey]:
					if markerKey not in missing:
						missing[markerKey] = []
					missing[markerKey].append(strainID)
					ct = ct + 1

		logger.debug('Found %d missing rows for %d markers' % (ct, len(missing)))
		return missing

	def getMaxStrainMarkerKey(self):
		# find the highest strain-marker key in the results so far
		
		smKeyCol = Gatherer.columnNumber(self.finalColumns, 'strain_marker_key')
		maxKey = 0
		for row in self.finalResults:
			maxKey = max(maxKey, row[smKeyCol])
		logger.debug('Found max strain_marker_key = %d' % maxKey)
		return maxKey

	def generateMissingStrainMarkers(self):
		# generate strain/marker rows for strains where a marker has no data
		
		# For each marker, find the strains with no annotations.
		missing = self.findMissingStrainsPerMarker()
		markers = missing.keys()
		markers.sort()

		smKey = self.getMaxStrainMarkerKey()
		ct = 0
		
		for markerKey in markers:
			for strainID in missing[markerKey]:
				strain, strainKey, seqNum = self.strainCache[strainID]

				smKey = smKey + 1
				r = ( smKey, markerKey, strainKey, strain, strainID,
						None, None, None, None, None, None, seqNum )
				self.finalResults.append (r)
				ct = ct + 1

		logger.debug('Generated %d filler rows' % ct)
		return
	
	def collateResults(self):
		self.populateStrainCaches()
		
		self.finalColumns, self.finalResults = self.results[-1]
		logger.debug('Got %d rows from db' % len(self.finalResults))
		
		self.generateMissingStrainMarkers()
		logger.debug('Added rows for missing strains')
		return
	
###--- globals ---###

all_strains = 'all_strains'

cmds = [
	# 0. ordered set of strains involved in strain markers
	'''select distinct a.accID, s._Strain_key, s.strain, case
			when s.strain = 'C57BL/6J' then 0
			else row_number() over (order by s.strain)
			end sequence_num
		into temp table %s
		from prb_strain s, acc_accession a
		where s._Strain_key = a._Object_key
			and a._MGIType_key = 10
			and a.preferred = 1
			and a._LogicalDB_key = 1
			and exists (select 1 from mrk_strainmarker msm
				where s._Strain_key = msm._Strain_key)
		order by 4''' % all_strains,
		
	# 1-2. add indexes to temp table
	'create unique index %s_idx1 on %s (accID)' % (all_strains, all_strains),
	'create unique index %s_idx2 on %s (_Strain_key)' % (all_strains, all_strains),

	# 3. pull in the standard set of strains (already in order)
	'''select _Strain_key, strain, accID, sequence_num
		from %s
		order by sequence_num''' % all_strains,
	
	# 4. all strain-marker records from the database
	'''select msm._StrainMarker_key as strain_marker_key, m._Marker_key as canonical_marker_key,
			s._Strain_key, s.strain, s.accID as strain_id, 
			gm.rawBiotype as feature_type, ch.chromosome,
			mcf.startCoordinate::bigint as start_coordinate,
			mcf.endCoordinate::bigint as end_coordinate, mcf.strand,
			(abs(mcf.endCoordinate - mcf.startCoordinate) + 1)::bigint as length,
			s.sequence_num
		from mrk_strainmarker msm
		inner join mrk_marker m on (m._Marker_key = msm._Marker_key)
		inner join %s s on (msm._Strain_key = s._Strain_key)
		inner join acc_accession a on (msm._StrainMarker_key = a._Object_key and a._MGIType_key = 44)
		left outer join acc_accession a_seq on (a.accID = a_seq.accID and a_seq._MGIType_key = 19)
		left outer join seq_genemodel gm on (a_seq._Object_key = gm._Sequence_key)
		left outer join map_coord_feature mcf on (a_seq._Object_key = mcf._Object_key and mcf._MGIType_key = 19)
		left outer join map_coordinate mc on (mcf._Map_key = mc._Map_key and mc._MGIType_key = 27)
		left outer join map_coord_collection col on (mc._Collection_key = col._Collection_key and col.abbreviation = 'MGP')
		left outer join mrk_chromosome ch on (mc._Object_key = ch._Chromosome_key)
		order by m._Marker_key, s.sequence_num''' % all_strains,
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'strain_marker_key', 'canonical_marker_key', '_Strain_key', 'strain', 'strain_id',
	'feature_type', 'chromosome', 'start_coordinate', 'end_coordinate', 'strand', 'length', 'sequence_num',
	]

# prefix for the filename of the output file
filenamePrefix = 'strain_marker'

# global instance of a StrainMarkerGatherer
gatherer = StrainMarkerGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
