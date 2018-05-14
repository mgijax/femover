#!/usr/local/bin/python
# 
# gathers data for the 'strain_to_reference' table in the front-end database

import Gatherer
import logger
import StrainUtils

###--- Classes ---###

class StrainToReferenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_to_reference table
	# Has: queries to execute against the source database
	# Does: queries the source database for references for strains,
	#	collates results, writes tab-delimited text file

	def collateResults(self):
		self.finalColumns = [ '_Strain_key', '_Refs_key', 'qualifier' ]
		self.finalResults = []
		
		cols, rows = self.results[-1]
		
		strainCol = Gatherer.columnNumber(cols, '_Strain_key')
		refsCol = Gatherer.columnNumber(cols, '_Refs_key')
		
		lastStrain = None
		for row in rows:
			strain = row[strainCol]
			if strain == lastStrain:
				qualifier = None
			else:
				qualifier = 'earliest'
				lastStrain = strain
				
				# flag the last reference for the previous strain as its latest
				if self.finalResults:
					if self.finalResults[-1][-1] == None:
						self.finalResults[-1][-1] = 'latest'
				
			self.finalResults.append( [strain, row[refsCol], qualifier] ) 
				
		logger.debug('Collected %d strain/refs rows' % len(self.finalResults))
		return
	
###--- globals ---###

cmds = [
	# 0. collect (in a temp table) the list of references for each strain.  Largely adapted from
	#	PRB_getStrainReferences() stored procedure in pgmgddbschema product.
	'''select v._Strain_key, e._Refs_key
		into temp table strainReferences
		from mld_expts e, mld_insitu m, %s v
		where e._Expt_key = m._Expt_key
			and m._Strain_key = v._Strain_key
		union
		select v._Strain_key, e._Refs_key
		from mld_expts e, mld_fish m, %s v
		where e._Expt_key = m._Expt_key
			and m._Strain_key = v._Strain_key
		union
		select v._Strain_key, e._Refs_key
		from mld_expts e, mld_matrix m, crs_cross c, %s v
		where e._Expt_key = m._Expt_key
			and m._Cross_key = c._Cross_key
			and c._femaleStrain_key = v._Strain_key
		union
		select v._Strain_key, e._Refs_key
		from mld_expts e, mld_matrix m, crs_cross c, %s v
		where e._Expt_key = m._Expt_key
			and m._Cross_key = c._Cross_key
			and c._maleStrain_key = v._Strain_key
		union
		select v._Strain_key, e._Refs_key
		from mld_expts e, mld_matrix m, crs_cross c, %s v
		where e._Expt_key = m._Expt_key
			and m._Cross_key = c._Cross_key
			and c._StrainHO_key = v._Strain_key
		union
		select v._Strain_key, e._Refs_key
		from mld_expts e, mld_matrix m, crs_cross c, %s v
		where e._Expt_key = m._Expt_key
			and m._Cross_key = c._Cross_key
			and c._StrainHT_key = v._Strain_key
		union
		select v._Strain_key, x._Refs_key
		from gxd_genotype s, gxd_expression x, %s v
		where s._Strain_key = v._Strain_key
			and s._Genotype_key = x._Genotype_key
		union
		select t._Strain_key, r._Refs_key
		from PRB_Reference r, prb_rflv v, prb_allele a, prb_allele_strain s, %s t
		where r._Reference_key = v._Reference_key
			and v._RFLV_key = a._RFLV_key
			and a._Allele_key = s._Allele_key
			and s._Strain_key = t._Strain_key
		union
		select v._Strain_key, r._Refs_key
		from all_allele a, mgi_reference_assoc r, %s v
		where a._Allele_key = r._Object_key
			and r._MGIType_key = 11
			and a._Strain_key = v._Strain_key
		union
		select mra._Object_key, mra._Refs_key
		from mgi_reference_assoc mra, %s t
		where mra._RefAssocType_key in (1009, 1010)
			and mra._Object_key = t._Strain_key'''.replace('%s', StrainUtils.getStrainTempTable()),

	# 1-2. index the temp table
	'create index strainIndex1 on strainReferences (_Strain_key)',
	'create index strainIndex2 on strainReferences (_Refs_key)',
	
	# 3. actually pull out the ordered references for each strain
	'''select distinct sr._Strain_key, r.year, c.numericPart, sr._Refs_key
		from strainReferences sr, bib_citation_cache c, bib_refs r
		where sr._Refs_key = c._Refs_key
			and sr._Refs_key = r._Refs_key
		order by 1, 2, 3'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Strain_key', '_Refs_key', 'qualifier' ]

# prefix for the filename of the output file
filenamePrefix = 'strain_to_reference'

# global instance of a StrainToReferenceGatherer
gatherer = StrainToReferenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
