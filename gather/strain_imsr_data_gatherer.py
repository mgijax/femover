#!/usr/local/bin/python
# 
# gathers data for the 'strain_imsr_data' table in the front-end database

import Gatherer
from IMSRData import IMSRStrainData
import logger
import StrainUtils

###--- Globals ---###

mgiStrainName = {}			# strain name (mixed case) : [ strain keys ]
mgiStrainNameLower = {}		# strain name (lowercase) : [ strain keys ]
mgiSynonym = {}				# synonym (mixed case) : [ strain keys ]
mgiSynonymLower = {}		# synonym (lowercase) : [ strain keys ]
mgiAccID = {}				# ID : [ strain keys ]

ldbOkay = [ 'EMMA', 'OBS', 'MMRRC', 'RMRC-NLAC', 'ARC', 'MUGEN' ]
ldbNeedsPrefix = [ 'NCIMR', 'APB', 'CARD', 'ORNL', 'NIG', 'TAC' ]
ldbJax = 'JAX Registry'
ldbHarwell = 'Harwell'
ldbRiken = 'RIKEN BRC'

###--- Functions ---###

def getImsrLines():
	# returns a list of data lines, processed from IMSR.  Each line includes:
	#		(strain name, approved nomen flag (0/1), IMSR ID, repository, source URL)

	return IMSRStrainData().queryStrains()

def tweakID(accID, ldb):
	# adjust the 'accID' according to rules for the given 'ldb', since MGI and IMSR store
	# IDs from the same logical database differently
	
	if ldb == ldbJax:
		if not accID.startswith('JAX'):
			return 'JAX:%s' % accID

	elif ldb == ldbRiken:
		if not accID.startswith('RBRC'):
			return 'RBRC%s' % accID

	elif ldb == ldbHarwell:
		if accID.startswith('FESA:'):
			accID = accID[5:]
		return 'HAR:%s' % accID
		
	elif ldb in ldbNeedsPrefix:
		if not accID.startswith(ldb):
			return '%s:%s' % (ldb, accID)
		
	return accID

###--- Classes ---###

class StrainImsrDataGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the strain_imsr_data table
	# Has: queries to execute against the source database
	# Does: queries the source database -- and IMSR -- for IMSR data about strains,
	#	collates results, writes tab-delimited text file

	def processIDs(self):
		# process accession IDs from the database, populating mgiAccIDs
		
		global mgiAccID
		
		cols, rows = self.results[2]
		keyCol = Gatherer.columnNumber(cols, '_Object_key')
		idCol = Gatherer.columnNumber(cols, 'accID')
		ldbCol = Gatherer.columnNumber(cols, 'name')
		
		for row in rows:
			key = row[keyCol]
			accID = tweakID(row[idCol], row[ldbCol])
			
			if accID not in mgiAccID:
				mgiAccID[accID] = [ key ]
			else:
				mgiAccID[accID].append(key)
				
		logger.debug('Processed %d ID rows' % len(rows))
		logger.debug('Got %d IDs' % len(mgiAccID))
		return
				
	def processNames(self):
		# process strain names from the database, populating two global dictionaries
		
		global mgiStrainName, mgiStrainNameLower
		
		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber(cols, '_Strain_key')
		strainCol = Gatherer.columnNumber(cols, 'strain')
		
		for row in rows:
			strain = row[strainCol]
			key = row[keyCol]
			strainLower = strain.lower()
			
			if strain not in mgiStrainName:
				mgiStrainName[strain] = [ key ]
			else:
				mgiStrainName[strain].append(key)
				
			if strainLower not in mgiStrainNameLower:
				mgiStrainNameLower[strainLower] = [ key ]
			else:
				mgiStrainNameLower[strainLower].append(key)
				
		logger.debug('Processed %d name rows' % len(rows))
		logger.debug('Got %s strain names' % len(mgiStrainName))
		logger.debug('Got %s lowercase strain names' % len(mgiStrainNameLower))
		return 

	def processSynonyms(self):
		# process strain synonyms from the database, populating two global dictionaries
		
		global mgiSynonym, mgiSynonymLower
		
		cols, rows = self.results[1]
		keyCol = Gatherer.columnNumber(cols, '_Object_key')
		synonymCol = Gatherer.columnNumber(cols, 'synonym')
		
		for row in rows:
			synonym = row[synonymCol]
			key = row[keyCol]
			synonymLower = synonym.lower()
			
			if synonym not in mgiSynonym:
				mgiSynonym[synonym] = [ key ]
			else:
				mgiSynonym[synonym].append(key)
				
			if synonymLower not in mgiSynonymLower:
				mgiSynonymLower[synonymLower] = [ key ]
			else:
				mgiSynonymLower[synonymLower].append(key)
				
		logger.debug('Processed %d synonym rows' % len(rows))
		logger.debug('Got %s synonyms' % len(mgiSynonym))
		logger.debug('Got %s lowercase synonyms' % len(mgiSynonymLower))
		return 

	def collateResults(self):
		# In order to match up IMSR data with MGI strain data, we need to do so by the
		# strain name stored in IMSR.  This may be an MGI strain name, or a synonym, or
		# neither.
		
		imsrLines = getImsrLines()
		self.processNames()
		self.processSynonyms()
		self.processIDs()
				
		# Now, we need to go through the data lines from IMSR.  For each...
		# 0. If we can match by ID, use that strain key.
		# 1. If not #0, if the IMSR strain name matches an MGI strain name, use that strain key.
		# 2. If not #1, if the IMSR strain name matches an MGI synonym, use that strain key.
		# 3. If not #2, if the IMSR strain name matches (case-insensitive) an MGI strain name, use that strain key.
		# 4. If not #3, if the IMSR strain name matches (case-insensitive) an MGI synonym, use that strain key.

		self.finalColumns = [ 'strain_key', 'imsr_id', 'repository', 'source_url', 'match_type', 'imsr_strain' ]
		self.finalResults = []
		
		noMatchCt = 0
		
		matchType = None
		for (imsrStrain, approved, imsrID, repository, sourceURL) in imsrLines:
			strainKeys = []
			if imsrID in mgiAccID:
				strainKeys = mgiAccID[imsrID]
				matchType = 'exact match to ID'
			elif imsrStrain in mgiStrainName:
				strainKeys = mgiStrainName[imsrStrain]
				matchType = 'exact match to name'
			elif imsrStrain in mgiSynonym:
				strainKeys = mgiSynonym[imsrStrain]
				matchType = 'exact match to synonym'
			else:
				imsrStrainLower = imsrStrain.lower()
				if imsrStrainLower in mgiStrainNameLower:
					strainKeys = mgiStrainNameLower[imsrStrainLower]
					matchType = 'case-insensitive match to name'
				elif imsrStrainLower in mgiSynonymLower:
					strainKeys = mgiSynonymLower[imsrStrainLower]
					matchType = 'case-insensitive match to synonym'
				else:
					noMatchCt = noMatchCt + 1
					
			for strainKey in strainKeys:
				self.finalResults.append( [ strainKey, imsrID, repository, sourceURL, matchType, imsrStrain ] )

		logger.debug('%d IMSR strain names did not match' % noMatchCt)
		logger.info('Produced %d output rows' % len(self.finalResults))
		return
	
###--- globals ---###

cmds = [
	# 0. get the strain names themselves
	'''select s._Strain_key, s.strain
		from prb_strain s, %s t
		where s._Strain_key = t._Strain_key''' % StrainUtils.getStrainTempTable(),

	# 1. get the synonyms for the strains
	'''select s._Object_key, s.synonym
		from mgi_synonym s, %s z, mgi_synonymtype t
		where s._Object_key = z._Strain_key
			and s._SynonymType_key = t._SynonymType_key
			and t._MGIType_key = 10''' % StrainUtils.getStrainTempTable(),
			
	# 2. non-MGI strain IDs (skip MGI, because IMSR has no MGI strain IDs)
	'''select a._Object_key, ldb.name, a.accID
		from acc_accession a, acc_logicaldb ldb, %s t
		where ldb.name != 'MGI'
			and a._Object_key = t._Strain_key
			and a._LogicalDB_key = ldb._LogicalDB_key
			and a._MGIType_key = 10''' % StrainUtils.getStrainTempTable(),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'strain_key', 'imsr_id', 'repository', 'source_url', 'match_type', 'imsr_strain',
	]

# prefix for the filename of the output file
filenamePrefix = 'strain_imsr_data'

# global instance of a StrainImsrData Gatherer
gatherer = StrainImsrDataGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
