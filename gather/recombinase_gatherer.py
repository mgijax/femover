#!/usr/local/bin/python
# 
# gathers data for the recombinase tables in the front-end database.
#
# Because we are generating unique keys that only exist in the front-end
# database, and because we need those keys to drive relationships among the
# recombinase tables, we generate all those connected tables within this
# single script.  (This allows us to keep the keys in sync.)
#
# 04/24/2013	lec
#	- TR11248
#		: added alleleSystemOtherMap (added 'age', other info)
#		: added alleleStructureMap
#		: re-organized some data structures/logic
#
# An age row of data can match more than one translation,
# but a translation can only match one range of numbers.
#
# That is, "postnatal 3-24" can match p1, p2
# but p1 and p2 must have unique min/max values
#

import Gatherer
import logger
import re
import TagConverter

# ageTranslation : used to translate an all_cre_cache.age/ageMin/ageMax
# to the appropriate age/ageMin/ageMax bucket for recombinase display
ageTranslation = {'e1': [0.0, 8.9],
             'e2': [8.91, 13.9],
             'e3': [14.0, 21.0],
             'p1': [21.01, 42.01],
             'p2': [42.02, 63.01],
             'p3': [63.02, 1846.0],
	};

###--- Functions ---###

def explode (x):
	# expand list x into a new list such that for each element i of x, we
	# have a tuple (i, all elements of x other than i)

	out = []
	for i in x:
		no_i = []
		for j in x:
			if i == j:
				continue
			no_i.append (j)
		out.append ( (i, no_i) )
	return out

def convert (s):
	# convert any ampersands (&) in s to be ands (and)
	if not s:
		return s
	return re.sub ('&', 'and', s)

###--- Classes ---###

class RecombinaseGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for the recombinase tables
	# Has: queries to execute against the source database
	# Does: queries for primary data for recombinases,
	#	collates results, writes multiple tab-delimited text files

	def findAlleleSystemPairs (self):
		# Purpose: processes query 0 to identify allele/system/structure objects
		# Returns: alleleSystemMap, alleleStructureMap, \
		# 	alleleData, columns, out, \
		# 	affectedCols, affected, unaffectedCols, unaffected, \
		# 	systemKeys

		#
		# alleleSystemMap[allele key] = {system : allele_system_key}
		#
		alleleSystemMap = {}

		#
		# alleleSystemOtherMap[allele_system_key] = 
		#	{'e1': '', 'e2':''..., 'p3':'', 'image':''}
		# for a given age range (e1...p3), set the value
		# 	'' = null
		# 	0 = not affected/not expressed
		# 	1 = affected/expressed
		#
		# for a given image, set the value
		# 	0 = does not have image
		# 	1 = has image
		#
		# alleleSystemOtherMap should realy be merged with alleleSystemMap
		# however alleleSystemMap has many dependencies
		# and re-organizing this dictionary will require changes
		# to other functions.  so, if I have more time..I will do this
		#
		alleleSystemOtherMap = {}

		#
		#
		# Structure information for a given allele/system
		#
		# alleleStructureMap[key][system] = { structure : {
		# 	'e1':'', 'e2':'', 'e3':'',
		# 	'p1':'', 'p2':'', 'p3':'',
		# 	'alleleSystemKey': the allele/system key (see alleleSystemMap)
		# 	'image':0,
		# 	'printName':'',
		# 	'expressed':0,
		#
		alleleStructureMap = {}

		# query 0 - defines allele / system pairs.  We need to number
		# these and store them in data set 0

		cols, rows = self.results[0]
		keyCol = Gatherer.columnNumber (cols, '_Allele_key')
		idCol = Gatherer.columnNumber (cols, 'accID')
		emapsKeyCol = Gatherer.columnNumber (cols, '_emaps_key')
		structureCol = Gatherer.columnNumber (cols, 'structure')
		stageCol = Gatherer.columnNumber (cols, '_stage_key')
		symCol = Gatherer.columnNumber (cols, 'symbol')
		ageCol = Gatherer.columnNumber (cols, 'age')
		ageMinCol = Gatherer.columnNumber (cols, 'ageMin')
		ageMaxCol = Gatherer.columnNumber (cols, 'ageMax')
		expCol = Gatherer.columnNumber (cols, 'expressed')
		hasImageCol = Gatherer.columnNumber (cols, 'hasImage')

		#
		# alleleData: dictonary of unique allele id:symbol for use in other functions
		# (findOtherSystems, findOtherAlleles)
		#
		alleleData = {}

		#
		# dictionary of affected (expressed) allele : system
		# dictionary of unaffected (not expressed) allele : system
		# for use in other functions
		#
		affectedCols = [ 'alleleKey', 'alleleSystemKey', 'system' ]
		unaffectedCols = [ 'alleleKey', 'alleleSystemKey', 'system' ]
		affected = []
		unaffected = []

		#
		# systemKeys[system name] = system key
		#
		systemKeys = {}

		#
		# out: dictionary of output data
		#
		out = []

		#
		# allele_system_key
		#
		alleleSystemKey = 0

		#
		# iterate thru each row of allele/system/structure/age
		#
		for row in rows:

			key = row[keyCol]
			system = 'test system'
			structure = row[structureCol]
			emapsKey = row[emapsKeyCol]
			age = row[ageCol]
			ageMin = row[ageMinCol]
			ageMax = row[ageMaxCol]
			hasImage = row[hasImageCol]
			printName = structure
			expressed = row[expCol]

			#
			# alleleSystemMap
			#
			# if existing allele...
			#
                        if alleleSystemMap.has_key(key):

				# if no existing system...
                                if not alleleSystemMap[key].has_key(system):

					# update allele_system_key count
			                alleleSystemKey = alleleSystemKey + 1

					# add new system/new alele_system_key
                                	alleleSystemMap[key][system] = alleleSystemKey

					# add new allele_system_key/age values
                                	alleleSystemOtherMap[alleleSystemKey] = { 
						'row':row,
						'e1':'', 'e2':'', 'e3':'',
					  	'p1':'', 'p2':'', 'p3':'',
						'image':0}

			# if new allele, then add new allele + system
                        else:
				# update allele_system_key count
			        alleleSystemKey = alleleSystemKey + 1

				# new allele/new system
                                alleleSystemMap[key] = { system : alleleSystemKey }

				# new allele_system_key/age values
                                alleleSystemOtherMap[alleleSystemKey] = { 
					  'row':row,
					  'e1':'', 'e2':'', 'e3':'',
					  'p1':'', 'p2':'', 'p3':'',
					  'image':0}

			#
			# alleleStructureMap
			#
			# if existing allele...
			#
                        if alleleStructureMap.has_key(key):

				# if no existing system...
                        	if not alleleStructureMap[key].has_key(system):
                                      	alleleStructureMap[key][system] = { structure : {
			          	'e1':'', 'e2':'', 'e3':'',
			          	'p1':'', 'p2':'', 'p3':'',
					'isPostnatal':0,
					'isPostnatalOther':0,
				        'alleleSystemKey':alleleSystemKey,
				        'image':0,
					'printName':'',
					'expressed':0}}

				# if structure...
                        	elif not alleleStructureMap[key][system].has_key(structure):
                                       	alleleStructureMap[key][system][structure] = {
				          	'e1':'', 'e2':'', 'e3':'',
				          	'p1':'', 'p2':'', 'p3':'',
						'isPostnatal':0,
						'isPostnatalOther':0,
					        'alleleSystemKey':alleleSystemKey,
					        'image':0,
						'printName':'',
						'expressed':0}

			else:
				# new allele/new system/new structure/ages
                                alleleStructureMap[key] = { system : { structure : {
					  'e1':'', 'e2':'', 'e3':'',
					  'p1':'', 'p2':'', 'p3':'',
					  'isPostnatal':0,
					  'isPostnatalOther':0,
					  'alleleSystemKey':alleleSystemKey,
					  'image':0,
					  'printName':'',
					  'expressed':0}}}

			#
			# process age information
			#

			#
			# if age == 'postnatal adult', assign to 'p3' (Adult)
			# if age == 'postnatal', assign to 'p3' (Postnatal-not specified) 
			# else use translation
			#

			if age in ['postnatal adult', 'postnatal']:
				alleleStructureMap[key][system][structure]['p3'] = row[expCol]
				alleleStructureMap[key][system][structure]['isPostnatal'] = 1

			#
			# use translation
			#
			else:

                                # keep track of age if it contains 'postnatal %'
                                if age.find('postnatal') >= 0:
                                        alleleStructureMap[key][system][structure]['isPostnatalOther'] = 1

				# search the ageTranslation translation
				for ageName in ageTranslation:

			    		ageMinTrans = ageTranslation[ageName][0]
			    		ageMaxTrans = ageTranslation[ageName][1]

					#
			    		# ages may fall within an ageMin/ageMax area
					# if both are outside the box, continue
					# else process
					#
					# ageMin between ageMinTrans and ageMaxTrans
					# ageMax between ageMinTrans and ageMaxTrans
					#

			    		if (ageMin >= ageMinTrans and ageMin <= ageMaxTrans) \
						or \
			    		   (ageMax >= ageMinTrans and ageMax <= ageMaxTrans):

						# null or 0 : can always be turned on
						# 1 : cannot be turned off
						# that is, 1 trumps
		
						for ageVal in ['e1', 'e2', 'e3', 'p1', 'p2', 'p3']:
		        				if ageName == ageVal and \
						   	   alleleStructureMap[key][system][structure][ageVal] != 1:
			    					alleleStructureMap[key][system][structure][ageVal] = \
										row[expCol]

							#
							# if the age falls into the 'p3' category
							# then set isPostnatal = true
							#
							if ageName == ageVal and ageName == 'p3':
								alleleStructureMap[key][system][structure]['isPostnatal'] = 1

                        #
                        # if structure contains a 'postnatal %' but no 'postnatal' value
                        #       then turn off the 'p3'/postnatal-not specified value
                        #
                        # example:
                        #        "postnatal", "postnatal month 1-2" : p3 on
                        #        "postnatal" : p3 on
                        #        "postnatal month 1-2": p3 off
                        #
                        if alleleStructureMap[key][system][structure]['p3'] == 1 \
                           and alleleStructureMap[key][system][structure]['isPostnatalOther'] == 1 \
                           and alleleStructureMap[key][system][structure]['isPostnatal'] == 0:
                               alleleStructureMap[key][system][structure]['p3'] = ''

			#
			# set alleleSystemOtherMap (system) age-value to alleleStructureMap (structure) age-value
			# that is, push the structure age-values up-to the system level
			#
			# an existing 1 cannot be overriden
			# an existing 0 cannot be overriden
			# 1 trumps 0 which trumps ''
			# 
			for ageVal in ['e1', 'e2', 'e3', 'p1', 'p2', 'p3']:

				if alleleSystemOtherMap[alleleSystemKey][ageVal] != 1 \
				   and alleleStructureMap[key][system][structure][ageVal] != '':
					alleleSystemOtherMap[alleleSystemKey][ageVal] = \
						alleleStructureMap[key][system][structure][ageVal]

			#
			# set alleleSystemOtherMap:hasImage (system)
			#
			if hasImage == 1 and alleleSystemOtherMap[alleleSystemKey]['image'] == 0:
				alleleSystemOtherMap[alleleSystemKey]['image'] = hasImage

			#
			# set alleleStructureMap:hasImage (
			#
			if hasImage == 1 and alleleStructureMap[key][system][structure]['image'] == 0:
				alleleStructureMap[key][system][structure]['image'] = hasImage

			#
			# set structure/printName
			# this does not really need to be repeated...
			#
			alleleStructureMap[key][system][structure]['printName'] = printName

			#
			# affected
			# unaffected
			#
			# dictionaries to track affected/unaffected (expression)
			# by allele_key:allele_system_key:system
			# for use in other functions
			#
			# assumes that "expressed = 1" order comes before "expressed = 0"
			#

			triple = (key, alleleSystemKey, system)
			if expressed == 0 and not triple in affected:
				if not triple in unaffected:
					unaffected.append (triple)
			else:
				if not triple in affected:
					affected.append (triple)

			#
			# systemKeys : dictionary of unique system keys
			# for use in other functions
			#

			if system and (not systemKeys.has_key(system)):
				systemKeys[system] = 1

			#
			# alleleData: dictonary of unique allele id:symbol 
			# for use in other functions
			#
			if not alleleData.has_key(key):
				alleleData[key] = (row[idCol], row[symCol])

		#
		# all data has been read into dictionaries
                #
                # write all of the alleleSystemOther information into the output file
                #
                for key in alleleSystemOtherMap.keys():

                        out.append ( (key, alleleSystemOtherMap[key]['row'][keyCol],
                                alleleSystemOtherMap[key]['row'][idCol],
                                convert(alleleSystemOtherMap[key]['row'][sysCol]),
				1, # _system_key
                                alleleSystemOtherMap[key]['e1'],
                                alleleSystemOtherMap[key]['e2'],
                                alleleSystemOtherMap[key]['e3'],
                                alleleSystemOtherMap[key]['p1'],
                                alleleSystemOtherMap[key]['p2'],
                                alleleSystemOtherMap[key]['p3'],
                                alleleSystemOtherMap[key]['image']))

		columns = [ 'alleleSystemKey', 'alleleKey', 'alleleID',
			'system', 'systemKey', 
			'age_e1', 'age_e2', 'age_e3',
			'age_p1', 'age_p2', 'age_p3',
			'has_image']

		logger.debug ('Found %d allele/system pairs' % alleleSystemKey)
		logger.debug ('Found %d alleles' % len(alleleData))
		logger.debug ('Found %d systems' % len(systemKeys))

		#
		# note that other dictionaries that were generated by this
		# processing are returned and used by other functions
		#

		return alleleSystemMap, alleleStructureMap, \
			alleleData, columns, out, \
			affectedCols, affected, unaffectedCols, unaffected, \
			systemKeys

	def findOtherSystems (self, alleleSystemMap, alleleData, systemKeys,
			affectedCols, affected):
		# Purpose: processes 'alleleSystemMap' to find other systems
		#	involved with the allele for each allele/system pair
		#	(does not touch query results in self.results)
		# Returns: columns, rows

		# for quick access, get a dictionary of allele/system keys
		# where expression was 1

		asKey = affectedCols.index('alleleSystemKey')
		positives = {}
		for row in affected:
			positives[row[asKey]] = 1

		# we need to generate the data set for
		# the recombinase_other_system table (for each allele/system
		# pair, list the other systems associated with each allele)

		out = []
		i = 0
		alleles = alleleSystemMap.keys()
		for allele in alleles:
			systems = alleleSystemMap[allele].keys()
			systems.sort()

			alleleID = alleleData[allele][0]

			for (system, otherSystems) in explode(systems):
			    # allele/system key for the current a/s pair
			    alleleSystemKey = alleleSystemMap[allele][system]

			    # Now walk through the other systems for this
			    # allele.  If any were expressed, then they need
			    # to go in the 'out' bin as other systems which
			    # showed expression for this allele.

			    for otherSystem in otherSystems:
				if otherSystem:
				    k = alleleSystemMap[allele][otherSystem]
				    if positives.has_key(k):
					i = i + 1
					systemKey = systemKeys[otherSystem]
					out.append ( (i, alleleSystemKey,
						alleleID, otherSystem,
						systemKey) )

		columns = [ 'uniqueKey', 'alleleSystemKey', 'alleleID',
				'system', 'systemKey' ]

		logger.debug ('Found %d other systems' % i)

		return columns, out

	def findOtherAlleles (self, alleleSystemMap, alleleData, systemKeys,
			affectedCols, affected):
		# Purpose: processes 'alleleSystemMap' to find other alleles
		#	involved with the system for each allele/system pair
		#	(does not touch query results in self.results)
		# Returns: columns, rows

		# for quick access, get a dictionary of allele/system keys
		# where expression was 1

		asKey = affectedCols.index('alleleSystemKey')
		positives = {}
		for row in affected:
			positives[row[asKey]] = 1

		# we will generate the data set for the
		# recombinase_other_allele table (for each allele/system pair,
		# list the other alleles associated with each system)

		bySystem = {}		# bySystem[system] = list of alleles
					# with expressed = 1 for this system

		alleles = alleleSystemMap.keys()
		for allele in alleles:
			systems = alleleSystemMap[allele].keys()
			for system in systems:
				asKey = alleleSystemMap[allele][system]

				# if this allele/system did not have expressed
				# = 1, then skip it

				if not positives.has_key(asKey):
					continue

				if not bySystem.has_key (system):
					bySystem[system] = [ allele ]
				else:
					bySystem[system].append (allele)

		out = []
		i = 0

		for system in bySystem.keys():
			alleles = bySystem[system]
			if system:
				systemKey = systemKeys[system]
			else:
				systemKey = None

			for (allele, otherAlleles) in explode(alleles):
				alleleSystemKey = \
					alleleSystemMap[allele][system]

				for other in otherAlleles:	
					i = i + 1
					out.append ( (
						i,
						alleleSystemKey,
						other,
						alleleData[other][0],
						alleleData[other][1],
						systemKey
						) )

		columns = [ 'uniqueKey', 'alleleSystemKey', 'alleleKey',
				'alleleID', 'alleleSymbol', 'systemKey', ]

		logger.debug ('Found %d other alleles' % i)

		return columns, out

	def findGenotypes (self):
		# extract allelic composition and background strain from
		# query 1

		keyCol = Gatherer.columnNumber (self.results[1][0],
			'_Genotype_key')
		noteCol = Gatherer.columnNumber (self.results[1][0], 'note')
		strainCol = Gatherer.columnNumber (self.results[1][0],
			'strain')

		alleleComp = {}
		strain = {}

		for r in self.results[1][1]:
			key = r[keyCol]
			if alleleComp.has_key (key):
				alleleComp[key] = alleleComp[key] + r[noteCol]
			else:
				alleleComp[key] = r[noteCol]
				strain[key] = r[strainCol]

		# trim trailing whitespace from allelic composition notes

		for key in alleleComp.keys():
			alleleComp[key] = TagConverter.convert (
				alleleComp[key].rstrip() )

		logger.debug ('Found %d genotypes' % len(strain))
		return alleleComp, strain

	def findAssayNotes (self):
		# extract assay note from query 2

		keyCol = Gatherer.columnNumber (self.results[2][0],
			'_Assay_key')
		noteCol = Gatherer.columnNumber (self.results[2][0],
			'assayNote')

		assayNote = {}

		for r in self.results[2][1]:
			key = r[keyCol]
			if assayNote.has_key (key):
				assayNote[key] = assayNote[key] + r[noteCol]
			else:
				assayNote[key] = r[noteCol]

		logger.debug ('Found %d assay notes' % len(assayNote))
		return assayNote

	def findNamesIDs (self, results):
		ids = {}
		names = {}

		columns, rows = results
		prepCol = Gatherer.columnNumber (columns, '_ProbePrep_key')
		nameCol = Gatherer.columnNumber (columns, 'name')
		idCol = Gatherer.columnNumber (columns, 'accID')
		
		for r in rows:
			key = r[prepCol]

			# only keep the first ID and name found for
			# each probe/antibody

			if not ids.has_key(key):
				ids[key] = r[idCol]
				names[key] = r[nameCol] 
		return ids, names

	def findDistinctStructures (self, alleleStructureMap):
		# Purpose: processes 'alleleStructureMap' to build 
		#	'recombinase_system_structure' columns/rows
		# Returns: columns, rows

		sCols = ['alleleSystemKey','structure','structureSeq',
			 'age_e1', 'age_e2', 'age_e3',
			 'age_p1', 'age_p2', 'age_p3',
			 'has_image']

		sRows = []
		count = 0

		for allele in alleleStructureMap:
		 	for system in alleleStructureMap[allele]:
				sorted_list = list(alleleStructureMap[allele][system])
				sorted_list.sort(key=lambda s : s)
				count = 0
				for structure in sorted_list:
					count += 1
					sRows.append((alleleStructureMap[allele][system][structure]['alleleSystemKey'],
						alleleStructureMap[allele][system][structure]['printName'],
						count,
						alleleStructureMap[allele][system][structure]['e1'],
						alleleStructureMap[allele][system][structure]['e2'],
						alleleStructureMap[allele][system][structure]['e3'],
						alleleStructureMap[allele][system][structure]['p1'],
						alleleStructureMap[allele][system][structure]['p2'],
						alleleStructureMap[allele][system][structure]['p3'],
						alleleStructureMap[allele][system][structure]['image']))

		logger.debug ('Found %d recombinase_system_structures' % len(sRows))
		return sCols,sRows 

	def findAssayResults (self, alleleSystemMap):

		# preliminary extractions from queries 1-4

		alleleComp, strain = self.findGenotypes()
		assayNotes = self.findAssayNotes()
		probeIDs, probeNames = self.findNamesIDs (self.results[3])
		antibodyIDs, antibodyNames = self.findNamesIDs (
			self.results[4])

		# detailed assay results from query 5

		cols = self.results[5][0]

		alleleCol = Gatherer.columnNumber (cols, '_Allele_key')
		emapsKeyCol = Gatherer.columnNumber (cols, '_emaps_key')
		structureCol = Gatherer.columnNumber (cols, 'structure')
		stageCol = Gatherer.columnNumber (cols, '_stage_key')
		
		assayTypeCol = Gatherer.columnNumber (cols, '_AssayType_key')
		reporterCol = Gatherer.columnNumber (cols,
			'_ReporterGene_key')
		assayCol = Gatherer.columnNumber (cols, '_Assay_key')
		ageCol = Gatherer.columnNumber (cols, 'age')
		sexCol = Gatherer.columnNumber (cols, 'sex')
		specimenNoteCol = Gatherer.columnNumber (cols, 'specimenNote')
		genotypeCol = Gatherer.columnNumber (cols, '_Genotype_key')
		resultNoteCol = Gatherer.columnNumber (cols, 'resultNote')
		strengthCol = Gatherer.columnNumber (cols, '_Strength_key')
		patternCol = Gatherer.columnNumber (cols, '_Pattern_key')
		jnumCol = Gatherer.columnNumber (cols, 'jnumID')
		pPrepCol = Gatherer.columnNumber (cols, '_ProbePrep_key')
		aPrepCol = Gatherer.columnNumber (cols, '_AntibodyPrep_key')
		dbResultCol = Gatherer.columnNumber (cols, '_Result_key')

		out = []
		columns = [ 'resultKey', 'alleleSystemKey', 'structure',
			'age', 'sex', 'jnumID', 'resultNote', 'specimenNote',
			'level', 'pattern', 'assayType', 'reporterGene',
			'detectionMethod', 'allelicComposition', 'strain',
			'assayNote', 'probeID', 'probeName', 'antibodyID',
			'antibodyName',
			]

		# newResultKeys[db result key] = [ (new result key, new
		#	allele/system key), ... ]
		newResultKeys = {}

		i = 0
		for r in self.results[5][1]:
			system = 'test system'

			alleleSystemKey = \
				alleleSystemMap[r[alleleCol]][system]

			i = i + 1

			# map the old result key (from the database) to the
			# new result key(s) and the allele/system keys being
			# generated

			info = (i, alleleSystemKey)
			dbResultKey = r[dbResultCol]
			if newResultKeys.has_key(dbResultKey):
				newResultKeys[dbResultKey].append (info)
			else:
				newResultKeys[dbResultKey] = [ info ]

			emapsKey = r[emapsKeyCol]
			structure = r[structureCol]

			row = [ i, alleleSystemKey, structure,
				r[ageCol], r[sexCol], r[jnumCol],
				r[resultNoteCol], r[specimenNoteCol], ]
			row.append (Gatherer.resolve (r[strengthCol],
				'GXD_Strength', '_Strength_key', 'strength'))
			row.append (Gatherer.resolve (r[patternCol],
				'GXD_Pattern', '_Pattern_key', 'pattern'))
			row.append (Gatherer.resolve (r[assayTypeCol],
				'GXD_AssayType', '_AssayType_key',
				'assayType'))
			row.append (Gatherer.resolve (r[reporterCol]))

			pPrep = r[pPrepCol]
			aPrep = r[aPrepCol]

			if probeIDs.has_key(pPrep):
				row.append ('probe')
			elif antibodyIDs.has_key(aPrep):
				row.append ('antibody')
			else:
				row.append ('direct')

			genotype = r[genotypeCol]

			if alleleComp.has_key (genotype):
				row.append (alleleComp[genotype])
			else:
				row.append (None)

			if strain.has_key (genotype):
				row.append (strain[genotype])
			else:
				row.append (None)

			if assayNotes.has_key (r[assayCol]):
				row.append (assayNotes[r[assayCol]])
			else:
				row.append (None)

			toDo = [ (pPrep, (probeIDs, probeNames) ), 
				(aPrep, (antibodyIDs, antibodyNames) ) ]

			for (prep, dicts) in toDo:
				for dict in dicts:
					if dict.has_key(prep):
						row.append (dict[prep])
					else:
						row.append (None)

			out.append (row)

		logger.debug ('Found %d assay results' % len(out))
		logger.debug ('Mapped %d assay results' % len(newResultKeys))
		return columns, out, newResultKeys

	def findSortValues (self, cols, rows):
		keyCol = Gatherer.columnNumber (cols, 'resultKey')

		byFields = [ 'structure', 'age', 'level', 'pattern', 'jnumID',
			'assayType', 'reporterGene', 'detectionMethod',
			'assayNote', 'allelicComposition', 'sex',
			'specimenNote', 'resultNote' ]

		sorted = {}		# sorted[key] = [ field1, ... ]
		for row in rows:
			sorted[row[keyCol]] = [ row[keyCol] ]

		for field in byFields:
			nullValued = []
			sortable = []

			myCol = Gatherer.columnNumber (cols, field)
			for row in rows:
				myVal = row[myCol]
				if myVal == None:
					nullValued.append (row[keyCol])
				else:
					sortable.append ((myVal.lower(),
						row[keyCol]))
			sortable.sort()

			i = 1
			for (myValue, key) in sortable:
				sorted[key].append (i)
				i = i + 1

			for key in nullValued:
				sorted[key].append (i)

			logger.debug ('Sorted %d rows by %s' % (i - 1, field))

		allRows = sorted.values()
		allRows.sort()
		logger.debug ('Sorted output rows prior to load')

		return [ 'resultKey' ] + byFields, allRows

	def findResultImages (self,
		resultMap	# maps from old result key to (new result key,
				# new allele/system key)
		):

		# associations between results and image panes is in query 6

		columns, rows = self.results[6]

		resultCol = Gatherer.columnNumber (columns, '_Result_key')
		labelCol = Gatherer.columnNumber (columns, 'paneLabel')
		imageCol = Gatherer.columnNumber (columns, '_Image_key')

		# columns for assay result image panes
		arColumns = [ 'uniqueKey', 'resultKey', 'imageKey',
			'sequenceNum', 'paneLabel', ]

		# columns for allele system images
		asColumns = [ 'uniqueKey', 'alleleSystemKey', 'imageKey',
			'sequenceNum', ]

		arRows = []	# rows for assay result image panes
		asRows = []	# rows for allele system image panes

		arNum = 0	# sequence number for assay result image panes
		asNum = 0	# sequence number for allele system img panes

		done = {}	# done[(allele/system key, image key)] = 1

		lastResultKey = None
		lastSystemKey = None

		arMax = 0
		asMax = 0

		for row in rows:
		    if not resultMap.has_key(row[resultCol]):
			logger.debug ('Unknown result key: %s' % \
				row[resultCol])
			continue
		    for (resultKey, alleleSysKey) in resultMap[row[resultCol]]:
			imageKey = row[imageCol]
			label = row[labelCol]

			if resultKey == lastResultKey:
				arNum = arNum + 1
			else:
				arNum = 1
				lastResultKey = resultKey

			arMax = arMax + 1
			arRows.append ( (arMax, resultKey, imageKey, arNum,
				label) )

			pair = (alleleSysKey, imageKey)
			if not done.has_key (pair):

				if alleleSysKey != lastSystemKey:
					asNum = asNum + 1
				else:
					asNum = 1
					lastSystemKey = alleleSysKey

				asMax = asMax + 1
				asRows.append ( (asMax, alleleSysKey,
					imageKey, asNum) )
				done[pair] = 1

		return arColumns, arRows, asColumns, asRows

	def collateResults (self):

		# step 1 -- recombinase_allele_system table

		alleleSystemMap, alleleStructureMap, \
			alleleData, columns, rows, \
			affectedCols, affected, unaffectedCols, unaffected, \
			systemKeys = self.findAlleleSystemPairs()
		logger.debug ('Systems: %s' % ', '.join(systemKeys.keys()))
		self.output.append ( (columns, rows) )

		# step 2 -- recombinase_other_system table

		columns, rows = self.findOtherSystems (alleleSystemMap,
			alleleData, systemKeys, affectedCols, affected)
		self.output.append ( (columns, rows) )

		# step 3 -- recombinase_other_allele table

		columns, rows = self.findOtherAlleles (alleleSystemMap,
			alleleData, systemKeys, affectedCols, affected)
		self.output.append ( (columns, rows) )

		# step 4 -- recombinase_assay_result table

		columns, rows, resultMap = \
			self.findAssayResults (alleleSystemMap)
		self.output.append ( (columns, rows) )

		# step 5 -- recombinase_assay_result_sequence_num table

		columns, rows = self.findSortValues (columns, rows)
		self.output.append ( (columns, rows) )

		# step 6 - recombinase_assay_result_imagepane and
		#	recombinase_allele_system_imagepane tables
		arColumns, arRows, asColumns, asRows = self.findResultImages (
			resultMap)
		self.output.append ( (asColumns, asRows) )
		self.output.append ( (arColumns, arRows) )

		# step 7 - allele_recombinase_affected_system and
		#	allele_recombinase_unaffected_system tables

		self.output.append ( (affectedCols, affected) )
		self.output.append ( (unaffectedCols, unaffected) )

		# step 8 - recombinase_system_structure
		columns, rows = self.findDistinctStructures(alleleStructureMap)
		self.output.append((columns,rows))

		return

###--- globals ---###

# SQL commands to be executed against the source database
cmds = [
	#
	# all allele / system / structure / age information
	# make sure 'expression' is ordered descending so that the '1' are encountered first
	#
	'''select c._Allele_key, c.accID, c.system, vte._emaps_key, struct.term as structure,
		c.symbol, c.age, c.ageMin, c.ageMax, c.expressed, 
		c.hasImage
	from all_cre_cache c
		join voc_term_emaps vte on
			vte._emapa_term_key = c._emapa_term_key
			and vte._stage_key = c._stage_key
		joint voc_term struct on
			struct._term_key = vte._term_key
	order by c._Allele_key, c.system, c.expressed desc''',

	#
	# genetic strain/background info by genotype
	#
	'''select distinct s._Genotype_key, mnc.note, mnc.sequenceNum,
		t.strain
	from all_cre_cache c,
		gxd_specimen s,
		mgi_note mn,
		mgi_notechunk mnc,
		gxd_genotype g,
		prb_strain t
	where c._Assay_key = s._assay_key
		and s._Genotype_key = mn._Object_key
		and mn._NoteType_key = 1018
		and mn._Note_key = mnc._Note_key
		and s._Genotype_key = g._Genotype_key
		and g._Strain_key = t._Strain_key
	order by mnc.sequenceNum''',

	#
	# assay notes by assay key
	#
	'''select distinct a._Assay_key, a.sequenceNum, a.assayNote
	from gxd_assaynote a,
		all_cre_cache c
	where a._Assay_key = c._Assay_key
	order by a.sequenceNum''',

	#
	# probes for each Cre probe prep key
	#
	'''select distinct g._ProbePrep_key,
		p.name,
		acc.accID
	from all_cre_cache c,
		gxd_assay a,
		gxd_probeprep g,
		prb_probe p,
		acc_accession acc
	where c._Assay_key = a._Assay_key
		and a._ProbePrep_key = g._ProbePrep_key
		and g._Probe_key = p._Probe_key
		and g._Probe_key = acc._Object_key
		and acc._LogicalDB_key = 1
		and acc._MGIType_key = 3
		and acc.preferred = 1
		and acc.prefixPart = 'MGI:'
		''',

	#
	# antibodies for each Cre antibody key
	#
	'''select distinct g._AntibodyPrep_key as _ProbePrep_key,
		p.antibodyName as name,
		acc.accID
	from all_cre_cache c,
		gxd_assay a,
		gxd_antibodyprep g,
		gxd_antibody p,
		acc_accession acc
	where c._Assay_key = a._Assay_key
		and a._AntibodyPrep_key = g._AntibodyPrep_key
		and g._Antibody_key = p._Antibody_key
		and g._Antibody_key = acc._Object_key
		and acc._LogicalDB_key = 1
		and acc._MGIType_key = 6
		and acc.preferred = 1
		and acc.prefixPart = 'MGI:'
		''',

	#
	# main cre assay result data
	#
	'''select distinct c._Allele_key, vte._emaps_key, struct.term as structure,
		a._AssayType_key, a._ReporterGene_key, a._Refs_key,
		a._Assay_key, a._ProbePrep_key, a._AntibodyPrep_key,
		s.age, s.sex, s.specimenNote, s._Genotype_key,
		r.resultNote, r._Strength_key, r._Pattern_key, r._Result_key,
		b.jnumID
	from all_cre_cache c,
		gxd_assay a,
		gxd_specimen s,
		gxd_insituresult r,
		gxd_iSresultstructure rs,
		bib_citation_cache b,
		voc_term_emaps vte,
		voc_term struct
	where c._Assay_key = a._Assay_key
		and a._Assay_key = s._Assay_key
		and s._Specimen_key = r._Specimen_key
		and rs._Result_key = r._Result_key
		and a._Refs_key = b._Refs_key
		and vte._emapa_term_key = c._emapa_term_key
		and vte._stage_key = c._stage_key
		and vte._term_key = struct._term_key
	''',

	#
	# image panes associated with recombinase assay results
	#
	'''select distinct g._Result_key, i.paneLabel, i._Image_key
	from img_imagepane i,
		gxd_insituresultimage g,
		gxd_insituresult r,
		gxd_specimen s,
		all_cre_cache c
	where g._ImagePane_key = i._ImagePane_key
		and g._Result_key = r._Result_key
		and r._Specimen_key = s._Specimen_key
		and s._Assay_key = c._Assay_key''',
	]

# data about files to be written; for each:  (filename prefix, list of field
#	names in order to be written, name of table to be loaded)
files = [
	('recombinase_allele_system',
		[ 'alleleSystemKey', 'alleleKey', 'alleleID', 'system',
			'systemKey',
			'age_e1', 'age_e2', 'age_e3',
			'age_p1', 'age_p2', 'age_p3',
			'has_image'],
		'recombinase_allele_system'),

	('recombinase_other_system',
		[ 'uniqueKey', 'alleleSystemKey', 'alleleID', 'system',
			'systemKey', ],
		'recombinase_other_system'),

	('recombinase_other_allele',
		[ 'uniqueKey', 'alleleSystemKey', 'alleleKey', 'alleleID',
			'alleleSymbol', 'systemKey' ],
		'recombinase_other_allele'),

	('recombinase_assay_result',
		[ 'resultKey', 'alleleSystemKey', 'structure', 'age', 'level',
			'pattern', 'jnumID', 'assayType', 'reporterGene',
			'detectionMethod', 'sex', 'allelicComposition',
			'strain', 'assayNote', 'resultNote',
			'specimenNote', 'probeID', 'probeName', 'antibodyID',
			'antibodyName', ],
		'recombinase_assay_result'),

	('recombinase_assay_result_sequence_num',
		[ 'resultKey', 'structure', 'age', 'level', 'pattern',
			'jnumID', 'assayType', 'reporterGene',
			'detectionMethod', 'assayNote', 'allelicComposition',
			'sex', 'specimenNote', 'resultNote' ],
		'recombinase_assay_result_sequence_num'),

	('recombinase_allele_system_imagepane',
		[ 'uniqueKey', 'alleleSystemKey', 'imageKey', 'sequenceNum' ],
		'recombinase_allele_system_to_image'),

	('recombinase_assay_result_imagepane',
		[ 'uniqueKey', 'resultKey', 'imageKey', 'sequenceNum',
			'paneLabel', ],
		'recombinase_assay_result_imagepane'),

	('allele_recombinase_affected_system',
		[ Gatherer.AUTO, 'alleleKey', 'alleleSystemKey', 'system' ],
		'allele_recombinase_affected_system'),

	('allele_recombinase_unaffected_system',
		[ Gatherer.AUTO, 'alleleKey', 'alleleSystemKey', 'system' ],
		'allele_recombinase_unaffected_system'),

	('recombinase_system_structure',
		[ Gatherer.AUTO, 'alleleSystemKey', 'structure','structureSeq',
			'age_e1', 'age_e2', 'age_e3',
			'age_p1', 'age_p2', 'age_p3',
			'has_image' ],
		'recombinase_system_structure'),

	]

# global instance of a RecombinaseGatherer
gatherer = RecombinaseGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
