#!/usr/local/bin/python
# 
# gathers data for the 'marker_flags' table in the front-end database

import Gatherer
import logger
import GXDUtils

###--- Classes ---###

class MarkerFlagsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the marker_flags table
	# Has: queries to execute against the source database
	# Does: queries the source database to identify certain flags for markers (where we
	#	neither need nor want counts), collates results, writes tab-delimited text file
	
	def getPhenotypeData(self):
		# returns a dictionary of marker keys for markers that have phenotype data, 
		# where those phenotype terms are mapped to anatomy terms (excluding phenotype
		# annotations with a 'normal' qualifier)
		
		cols, rows = self.results[0]
		pheno = {}
		for row in rows:
			pheno[row[0]] = 1
		logger.debug('Found %d markers with pheno data' % len(pheno))
		return pheno
	
	def getWildtypeData(self):
		# returns a dictionary of marker keys that have wild-type expression data
		wildtype = {}

		cols, rows = self.results[1]
		markerCol = Gatherer.columnNumber(cols, '_Marker_key')
		genotypeCol = Gatherer.columnNumber(cols, '_Genotype_key')
		assayCol = Gatherer.columnNumber(cols, '_Assay_key')
		
		for row in rows:
			markerKey = row[markerCol]
			if markerKey not in wildtype:
				if GXDUtils.isWildType(row[genotypeCol], row[assayCol]):
					wildtype[markerKey] = 1
		
		logger.debug('Found %d markers with wildtype GXD data' % len(wildtype))
		return wildtype
	
	def collateResults(self):
		pheno = self.getPhenotypeData()
		wildtype = self.getWildtypeData()
		
		self.finalColumns = self.fieldOrder
		self.finalResults = []
		
		for row in self.results[-1][1]:
			markerKey = row[0]

			hasPheno = 0
			if markerKey in pheno:
				hasPheno = 1

			hasWildtype = 0
			if markerKey in wildtype:
				hasWildtype = 1
			
			self.finalResults.append( [ markerKey, hasWildtype, hasPheno ])
			
		logger.debug('Built %d output rows' % len(self.finalResults))
		return

###--- globals ---###

cmds = [
	# 0. markers with rolled-up phenotypes that have mappings to anatomy terms (for
	# annotations without a 'normal' qualifier)
	'''with assays as (select va._Object_key, va._Qualifier_key
			from voc_annot va, mgi_relationship r
			where va._AnnotType_key = 1015
				and r._Category_key = 1007
				and va._Term_key = r._Object_key_1
		)
		select distinct va._Object_key as _Marker_key
		from assays va, voc_term q
		where va._Qualifier_key = q._Term_key
			and q.term is null''',
	
	# 1. markers with genotype/assay pairs to look for wild-type expression data
	'''select distinct _Marker_key, _Genotype_key, _Assay_key
		from gxd_expression
		where isForGXD = 1''',

	# 2. all markers (ensuring that our new marker_flags table also has all markers)
	'''select m._Marker_key
		from mrk_marker m''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	'marker_key', 'has_wildtype_expression_data', 'has_phenotypes_related_to_anatomy',
	]

# prefix for the filename of the output file
filenamePrefix = 'marker_flags'

# global instance of a MarkerFlagsGatherer
gatherer = MarkerFlagsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
