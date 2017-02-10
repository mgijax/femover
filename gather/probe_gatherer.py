#!/usr/local/bin/python
# 
# gathers data for the 'probe' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class ProbeGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the probe table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for probes,
	#	collates results, writes tab-delimited text file

	def collateResults (self):
		
		# produce a cache with the clone ID for each probe which has
		# one

		columns, rows = self.results[1]

		keyCol = Gatherer.columnNumber (columns, '_Probe_key')
		idCol = Gatherer.columnNumber (columns, 'cloneID')

		cloneIDs = {}
		for r in rows:
			cloneIDs[r[keyCol]] = r[idCol]

		logger.debug('Cached IDs for %d clones' % len(cloneIDs))

		# remember the cache for post-processing

		self.cloneIDs = cloneIDs

		# cache counts of GXD expression results
		
		columns, rows = self.results[2]
		
		keyCol = Gatherer.columnNumber (columns, '_Probe_key')
		countCol = Gatherer.columnNumber (columns, 'ct')

		self.gxdCounts = {}
		for r in rows:
			self.gxdCounts[r[keyCol]] = r[countCol]

		logger.debug('Cached %d GXD counts' % len(self.gxdCounts))
		
		# the first query contains the bulk of the data we need, with
		# the rest to come via post-processing

		self.finalColumns = self.results[0][0]
		self.finalResults = self.results[0][1]
		return

	def postprocessResults (self):
		# Purpose: override of standard method for key-based lookups

		self.convertFinalResultsToList()

		keyCol = Gatherer.columnNumber (self.finalColumns,
			'_Probe_key') 
		ldbCol = Gatherer.columnNumber (self.finalColumns,
			'_LogicalDB_key')

		for r in self.finalResults:
			self.addColumn ('logicalDB', Gatherer.resolve (
				r[ldbCol], 'acc_logicaldb', '_LogicalDB_key',
				'name'), r, self.finalColumns)

			probeKey = r[keyCol]
			
			cloneID = None
			if probeKey in self.cloneIDs:
				cloneID = self.cloneIDs[probeKey]
			self.addColumn ('cloneID', cloneID, r, self.finalColumns)
				
			gxdCount = 0
			if probeKey in self.gxdCounts:
				gxdCount = self.gxdCounts[probeKey]
			self.addColumn ('gxd_count', gxdCount, r, self.finalColumns)
		return

###--- globals ---###

cmds = [
	# 0. basic probe data
	'''select p._Probe_key,
		p.name,
		t.term as segmentType,
		a.accID as primaryID,
		a._LogicalDB_key,
		o.commonName as organism,
		s.age,
		x.term as sex,
		c.term as cell_line,
		v.term as vector,
		p.insertSite as insert_site,
		p.insertSize as insert_size,
		p.productSize as product_size,
		s.name as library,
		r.jnumID as library_jnum,
		pt.tissue,
		regionCovered as region_covered,
		st.strain
	from prb_probe p
	inner join voc_term t on (p._SegmentType_key = t._Term_key)
	inner join acc_accession a on (p._Probe_key = a._Object_key
		and a.preferred = 1
		and a._LogicalDB_key = 1
		and a._MGIType_key = 3)
	inner join prb_source s on (p._Source_key = s._Source_key)
	inner join mgi_organism o on (s._Organism_key = o._Organism_key)
	inner join voc_term x on (s._Gender_key = x._Term_key)
	inner join voc_term c on (s._CellLine_key = c._Term_key)
	inner join voc_term v on (p._Vector_key = v._Term_key)
	inner join prb_tissue pt on (s._Tissue_key = pt._Tissue_key)
	inner join prb_strain st on (s._Strain_key = st._Strain_key)
	left outer join bib_citation_cache r on (s._Refs_key = r._Refs_key)''',

	# 1. clone IDs
	'''select p._Probe_key, a.accID as cloneID
	from prb_probe p, acc_accession a, mgi_setmember msm, mgi_set ms
	where p._Probe_key = a._Object_key
		and a._MGIType_key = 3
		and a._LogicalDB_key = msm._Object_key
		and msm._Set_key = ms._Set_key
		and ms.name = 'Clone Collection (all)' ''',
		
	# 2. GXD expression result counts
	'''select _Probe_key, count(distinct _Expression_key) as ct
	from GXD_Expression e, GXD_Assay a, GXD_ProbePrep p
	where e._Assay_key = a._Assay_key
		and e.isForGXD = 1
		and a._ProbePrep_key = p._ProbePrep_key
	group by 1'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Probe_key', 'name', 'segmentType', 'primaryID', 'logicalDB', 'cloneID',
		'organism', 'age', 'sex', 'cell_line', 'vector', 'insert_site', 'insert_size',
		'product_size', 'library', 'library_jnum', 'tissue', 'region_covered', 'strain', 'gxd_count',
	]

# prefix for the filename of the output file
filenamePrefix = 'probe'

# global instance of a ProbeGatherer
gatherer = ProbeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
