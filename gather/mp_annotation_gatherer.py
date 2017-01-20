#!/usr/local/bin/python
#
# This gatherer creates all the tables needed to group and display MP annotations on allele detail
# pages and the genotype popup pages
# all tables of the format MP_* are centered around a genotype
# all tables of the format Phenotable_* are centered around an allele
#
# These two data architectures share the same queries,functions, and dictionary lookups
# for populating the data
#
# Uses: GenotypeClassifier located in "/femover/lib/python/" for calculating genotype sequences
#
# Author: kstone - oct 2012
#

import Gatherer
import VocabSorter
import Lookup
import logger
import GenotypeClassifier
import MPSorter
import copy
import gc
import symbolsort

###--- Constants ---###

# exception raised in this gatherer
Error = 'mp_annotation_gatherer.error'

# the following are database keys in mgd
# MP Note Types
GENERAL_NOTETYPE = 1008
NORMAL_NOTETYPE = 1031
# background sensitivity
BS_NOTETYPE = 1015
MP_NOTETYPES = ["%s"%GENERAL_NOTETYPE,"%s"%NORMAL_NOTETYPE,"%s"%BS_NOTETYPE]
# evidence property _property_term_key
SEX_SPECIFICITY_KEY = "8836535"
# specific term keys
NO_PHENOTYPIC_ANALYSIS_KEY=293594
# the following are display related values
NORMAL_PHENOTYPE = "normal phenotype"
SANGER_JNUM = 'J:175295'
EUROPHENOME_JNUM = 'J:165965'
# provider names
WTSI='WTSI'
MGI='MGI'
EUROPHENOME='EuPh'
PROVIDER_SORT_ORDER=[MGI,WTSI,EUROPHENOME]
# sex values
NA='NA'
M='M'
F='F'
SEX_SORT_ORDER=[NA,F,M]
# Diseases
DISEASE_MODELS="Disease Models"

# "HTMP Phenotyping Centers" vocabulary key
PHENO_CENTER_VOCAB = 99

# "Data Interpretation Center" term key
INTERPRETATION_CENTER = 12798366

# "Phenotyping Center" term key
PHENOTYPING_CENTER = 12798367

# maximum term key from database
MAX_TERM_KEY = 0

# term key for a "normal" qualifier for MP annotations
NORMAL_TERM_KEY = 2181424

# annotation type key for MP/Genotype annotations
MP_ANNOT_TYPE = 1002

###--- Functions ---###
# resolve a jnumber into the source display value
# QUESTION: should we be using jnum ID to determine this?
#def getSource(jnum):
#	# TODO: this could be done in a more kosher way
#	if jnum==SANGER_JNUM:
#		return WTSI
#	if jnum==EUROPHENOME_JNUM:
#		return EUROPHENOME
#	return MGI
# the following functions merely define what order different groups of things should be sorted in
def getSexSeq(sex):
	return getSeq(sex,SEX_SORT_ORDER)
def getSexCallSeq(sex,call):
	sexSeq = getSexSeq(sex)
	callSeq = 1
	if call=='N':
		callSeq = 2
	# do the sex sorting first, then by call (check or N)
	seq = sexSeq*100 + callSeq
	return seq
#def getProviderSeq(provider):
#	return getSeq(provider,PROVIDER_SORT_ORDER)
def getSeq(item,sort_list):
	return item in sort_list and sort_list.index(item)+1 or len(sort_list)+1

###--- Classes ---###
class LabCache:
	# cache of all LabInfo objects, providing easy access by term key or
	# by name

	def __init__ (self):
		self.byName = {}
		self.byKey = {}
		return

	def add (self, lab):
		term = lab.getTerm()
		if self.byName.has_key(term):
			raise Error, 'Duplicate LabInfo name: %s' % term

		key = lab.getTermKey()
		if self.byKey.has_key(key):
			raise Error, 'Duplicate LabInfo term key: %d' % key

		self.byName[term] = lab
		self.byKey[key] = lab

		abbrev = lab.getAbbreviation()
		if abbrev:
			self.byName[abbrev] = lab
		return

	def getByKey (self, termKey):
		if self.byKey.has_key(termKey):
			return self.byKey[termKey]
		return None

	def getByName (self, name):
		if self.byName.has_key(name):
			return self.byName[name]
		return None

	def getMaxSequenceNum (self):
		mx = 0
		for lab in self.byName.values():
			mx = max(mx, lab.getSequenceNum())
		return mx

	def getMaxTermKey (self):
		return max(self.byKey.keys())

class LabInfo:
	# data for one phenotyping or data interpretation center

	def __init__ (self, termKey, term, sequenceNum, abbreviation):
		self.termKey = termKey
		self.sequenceNum = sequenceNum
		self.term = term
		self.abbreviation = abbreviation
		return

	def getTermKey (self):
		return self.termKey

	def getSequenceNum (self):
		return self.sequenceNum

	def getTerm (self):
		return self.term

	def getAbbreviation (self):
		return self.abbreviation

class AnnotationGatherer (Gatherer.MultiFileGatherer):
	# Is: a data gatherer for mp_annotation table
	# Has: queries to execute against the source database

	mpSorter = MPSorter.MPSorter()

	###--- Maps ---###
	headerTermMap = {}
	headerKeys = []
	headerKeyMap = {}
	noteMap = {}
	alleleMap = {}
	systemSeqMap = {}
	# sequence map keys by (allele_key,genotype_Key)
	genotypeSeqMap = {}
	genotypeIDMap = {}
	genotypeRowMap = {}
	genotypeSexMap = {}
	diseaseGenotypeMap = {}
	# special map that calculates information about the genotype headers for the phenotable
	genotypeStatisticsMap = {}
	genotypeDiseaseOnly = {}
	# special map to hold all the phenocell "calls" before aggregating them
	phenoCallMap = {}
	providerRowsDict = {}
	diseaseSeqMap = {}

	###--- Genotype centric functions ---###
	# iterates through all genotypes in MGI that have pheno data or disease
	# sets all the map(lookup) data to be used later
	def buildGenotypes(self):
		cols, rows = self.results[6]
		count = 0
		for row in rows:
			count += 1
			allele_key=row[0]
			genotype_key=row[1]
			genotype_id=row[2]
			# annot type can be either mp or do
			annot_type=row[3]
			seq = GenotypeClassifier.getSequenceNum (allele_key,genotype_key)
			disease_only = 0
			# add the list of genotypes with disease models
			if annot_type=='do':
				self.diseaseGenotypeMap.setdefault(allele_key,[]).append(genotype_key)
				#disease_only = 1
			#disease_only = self.registerGenotypeDiseaseOnly(genotype_key,disease_only)
			# NOTE: The disease_only method does not work very well in all cases, commenting it out for now
			# add a default phenotable_to_genotype row, to be modified later by registering sex annotations later on
			# set the default sex value to ""
			self.genotypeRowMap[(allele_key,genotype_key)] = [count,allele_key,genotype_key,seq,0,"",disease_only]
			self.genotypeSeqMap[(allele_key,genotype_key)] = seq
			self.genotypeSeqMap.setdefault(allele_key,{})[genotype_key] = seq
			self.genotypeIDMap[genotype_key] = genotype_id
	def buildDiseaseGenotypes(self):
		dGenotypeCols = ['diseasetable_genotype_key','allele_key','genotype_key','genotype_seq']
		dGenotypeRows = []
		count = 0
		for allele_key,genotype_keys in self.diseaseGenotypeMap.items():
			for genotype_key in genotype_keys:
				count+=1
				genotype_seq = self.genotypeSeqMap[(allele_key,genotype_key)]
				dGenotypeRows.append([count,allele_key,genotype_key,genotype_seq])
		return dGenotypeRows

	# keeps track of genotypes that have do annotations but no mp annotations
	# will return true until a 0 is passed in
	def registerGenotypeDiseaseOnly(self,genotype_key,has_disease):
		if genotype_key not in self.genotypeDiseaseOnly:
			self.genotypeDiseaseOnly[genotype_key] = has_disease
			return has_disease
		if not has_disease:
			return has_disease
		self.genotypeDiseaseOnly[genotype_key] = self.genotypeDiseaseOnly[genotype_key] and has_disease
		return self.genotypeDiseaseOnly[genotype_key]

	# update genotype values when sex information has been encountered
	def registerGenotypeSex(self,allele_key,genotype_key,sex):
		map_key = (allele_key,genotype_key)
		self.genotypeSexMap.setdefault(map_key,set([])).add(sex)
		if len(self.genotypeSexMap[map_key]) > 1:
			# display Both
			self.setGenotypeRowSplitSex(allele_key,genotype_key)
			sex = "Both"
		if sex in [M,F,"Both"]:
		    self.genotypeRowMap[map_key][5] = sex
	# used by above function to set the split_sex bit to true when necessary
	def setGenotypeRowSplitSex(self,allele_key,genotype_key):
		map_key = (allele_key,genotype_key)
		if map_key in self.genotypeRowMap:
			# set that piece of data to true
			self.genotypeRowMap[map_key][4]=1
		#else wtf are we doing here
	# useful to lookup the phenotable_to_genotype row keys
	def getGenotypeRowKey(self,allele_key,genotype_key):
		return self.genotypeRowMap[(allele_key,genotype_key)][0]
	# return all the phenotable_to_genotype rows when we are finally ready to save them
	def getGenotypeRows(self):
		# need to clone the list because the values might be updated later
		return copy.deepcopy(self.genotypeRowMap.values())

	###--- Phenotable Cell calculation functions---###
	def getProviderSeq(self, provider):
		# get a sequence number for the provider pair

		if self.providerSeqNum.has_key(provider):
			return self.providerSeqNum[provider]
		return 10000

	def resetPhenoGridMaps(self):
		self.genotypeStatisticsMap = {}
		self.phenoCallMap = {}
	# when we encounter annotations we need to register the cell data to calculate the header statistics
	# we also store all of the registered "calls" that we find in a map, to be replayed later once all the header stats are generated
	def registerCellCall(self,allele_key,genotype_key,provider,sex,call,termID):
		statsMap = self.genotypeStatisticsMap.setdefault(allele_key,{}).setdefault(genotype_key,{"providers":set([]),"sexes":set([])})
		statsMap["providers"].add(provider)
		statsMap["sexes"].add(sex)
		call_map_key = (allele_key,termID)
		self.phenoCallMap.setdefault(call_map_key,[]).append([genotype_key,provider,sex,call])
	# this can be called after all annotations have been registerd above.
	# It will return a map of the necessary information to create all the cell rows
	# used for both phenotable_term_cell and phenotable_system_cell by setting termID to either the system or a (system,termID) tuple
	def getRegisteredCellCalls(self,allele_key,termID):
		# init the pheno cells grid map
		pheno_genos = {}
		cell_seqs = []
		for gkey,statsMap in self.genotypeStatisticsMap[allele_key].items():
			g_seq = self.genotypeSeqMap[(allele_key,gkey)]
			if len(statsMap["sexes"])>1:
				statsMap["sexes"] = [M,F]
			sexes = statsMap["sexes"]
			providers = statsMap["providers"]
			for sex in sexes:
				s_seq = getSexSeq(sex)
				for provider in providers:
					p_seq = self.getProviderSeq(provider)
					# for now, define the cell sequence as a tuple. We will normalise them into integers later
					cell_seq = (g_seq,s_seq,p_seq)
					cell_map_key=(gkey,sex,provider)
					# here we are pre-generating the actual grid cells for this allele, termID combo
					pheno_genos[cell_map_key]={"call":NA,"seq":cell_seq,"genotype_key":gkey,"sex":sex}
					cell_seqs.append(cell_seq)

		# get the previously saved "calls"
		cellCalls = self.phenoCallMap[(allele_key,termID)]
		splitCellCalls = []
		# split any calls that we need to
		for row in cellCalls:
			gkey=row[0]
			sex = row[2]
			# do we split this into M and F?
			if sex==NA:
				statsMap = self.genotypeStatisticsMap[allele_key][gkey]
				if len(statsMap["sexes"])>1:
					#time to split sexes
					row1 = [row[0],row[1],M,row[3]]
					row2 = [row[0],row[1],F,row[3]]
					splitCellCalls.append(row1)
					splitCellCalls.append(row2)
					continue
			# ELSE
			splitCellCalls.append(row)

		# now replay all the calls and place them on the grid that was generated above
		for gkey,provider,sex,call in splitCellCalls:
			# set to Y or N
			call = call and "Y" or "N"

			# key = genotype_Key,sex,provider
			cell_map_key=(gkey,sex,provider)
			# set the call unless it already has been set to "Y', which is analogous to a check mark
			if pheno_genos[cell_map_key]["call"] != "Y":
				pheno_genos[cell_map_key]["call"] = call

		# normalise cell sequence nums
		# remember, we made cell_seq a tuple of genotype_seq,sex_seq,provider_seq) above
		cell_seqs.sort()
		cs_map = {}
		count =0
		for cell_seq in cell_seqs:
			count += 1
			cs_map[cell_seq] = count
		for item in pheno_genos.values():
			item["seq"] = cs_map[item["seq"]]

		return pheno_genos

	###--- System (header) term functions ---###
	# iterate the query for MP systems
	# create a lookup for term_key to system name
	def buildHeaderMap(self):
		if self.headerTermMap:
			return self.headerTermMap
		cols, rows = self.results[0]
		for row in rows:
			# add header term to map, keyed by child term key
			term_key = row[0]
			header_term = row[2]
			self.headerTermMap.setdefault(term_key,[]).append(header_term)

			# add header term keys to a list
			header_key = row[1]
			self.headerKeys.append(header_key)

			# add header term key for lookup
			self.headerKeyMap[header_term] = header_key
		return self.headerTermMap
	# is this term_key a header term_key
	def isHeaderTerm(self, termKey):
		return termKey in self.headerKeys
	# get the term_key for the system name passed in
	def getHeaderKey(self,headerTerm):
		if headerTerm in self.headerKeyMap:
			return self.headerKeyMap[headerTerm]
		return -1
	# Build the special ordering of systems that is pre-defined for each genotype
	# key each individual sequence map by term_key, grouped by genotype_key
	def buildSystemSeqMap(self):
		if self.systemSeqMap:
			return self.systemSeqMap
		cols, rows = self.results[3]
		for row in rows:
			gkey = row[0]
			term_key = row[1]
			seq = row[2]
			# add map for each genotype, then add term_key=>sequencenum
			self.systemSeqMap.setdefault(gkey,{})[term_key] = seq
		return self.systemSeqMap
	# get a system sequence ordering by genotype/system combo
	def getSystemSeq(self,genotype_key,header_term_key):
		if not self.systemSeqMap:
			self.buildSystemSeqMap()
		if genotype_key in self.systemSeqMap:
			headerSeqMap = self.systemSeqMap[genotype_key]
			if header_term_key in headerSeqMap:
				return headerSeqMap[header_term_key]
		return -1

	###--- Notes related functions ---###
	# load all the MP notes and "de-chunk" them
	# add any special processing, such as for "background sensitivity" notes
	# the notes get put into a map grouped by VOC_Evidence._evidence_key
	# ALGORITHM: the basic algorithm assumes that a pre-sorted list of note chunks has been queried.
	#  we then go through and concatenate each note chunk if the sequence is larger than the previous.
	#  if the sequence is <= the previous row, then we assume it's a new note, and save the last note that we had been de-chunking.
	def buildNotes(self):
		if self.noteMap:
			return self.noteMap
		cols, rows = self.results[2]
		cur_note_chunk = ''
		last_seq = 1
		last_evidence_key=-1

		# type of note we are building that has not yet been stored
		# in self.noteMap
		note_type = None

		for row in rows:
			evidence_key = row[1]
			note_chunk = row[2]
			seq = row[3]
			notetype_key = row[4]
			if seq <= last_seq:
				# we have a new note, we can stop adding chunks, save the previously built note, and start a new one.
				full_note = cur_note_chunk.strip()
				if full_note:
				    self.noteMap.setdefault(last_evidence_key,[]).append((full_note,note_type))
				cur_note_chunk = ''
				if notetype_key==BS_NOTETYPE:
					# append special label for background sensitivity notes
					cur_note_chunk = 'Background Sensitivity: '
			# combine note chunks, and set the list of notes by mgd evidence key
			cur_note_chunk = '%s%s'%(cur_note_chunk,note_chunk)
			last_seq = seq
			last_evidence_key=evidence_key
			note_type = notetype_key

		# end of rows, save the last note
		full_note = cur_note_chunk.strip()
		if full_note:
		    self.noteMap.setdefault(last_evidence_key,[]).append(
			(full_note, note_type))
		cur_note_chunk = ''

		return self.noteMap
	# returns a list of (note, note type) pairs for the given evidence key
	def getNotes(self,evidenceKey):
		if not self.noteMap:
			self.buildNotes()
		if evidenceKey in self.noteMap:
			return self.noteMap[evidenceKey]
		return []

	###--- "DAG"gy functions ---###

	# recurse through the MP dag to figure out how to sort (and indent) a subset of terms for a given system (header)
	# returns a map of termKey : {"seq": seq, "depth": depth}
	def calculateSortAndDepths(self,terms,rootKey):
		return self.mpSorter.calculateSortAndDepths(terms,rootKey)

	###--- Allele centric functions---###
	# builds a map of genotype key to n allele keys
	def buildAlleleMap(self):
		if self.alleleMap:
			return self.alleleMap
		cols, rows = self.results[5]
		for row in rows:
			genotypeKey = row[0]
			alleleKey = row[1]
			self.alleleMap.setdefault(genotypeKey,[]).append(alleleKey)
		return self.alleleMap
	def getAlleleKeys(self,genotypeKey):
		if not self.alleleMap:
			self.buildAlleleMap()
		if genotypeKey in self.alleleMap:
			return self.alleleMap[genotypeKey]
		return []

	# gather the set of annotation evidence keys which are for annotations
	# with a 'normal' qualifier
	def getNormalEvidenceKeys (self):
		logger.debug('gathering map of normal evidence keys')
		cols, rows = self.results[11]

		ek = {}
		for row in rows:
			ek[row[0]] = 1

		self.results[11] = [ cols,[] ]
		gc.collect()

		logger.debug('found %d evidence keys for normal annot' % \
			len(ek))
		return ek

	###--- MP table function---###
	# builds all the mp_* tables
	def buildMPRows (self ):
		# get mappings for header terms
		logger.debug("gathering map of header terms")
		self.headerTermMap = self.buildHeaderMap()

		systemRows = []
		termRows = []
		annotRows = []
		refRows = []
		noteRows = []

		cols, rows = self.results[1]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		gkeyCol = Gatherer.columnNumber (cols, 'genotype_key')
		gidCol = Gatherer.columnNumber (cols, 'genotype_id')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		termIDCol = Gatherer.columnNumber (cols, 'term_id')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		jnumCol = Gatherer.columnNumber (cols, 'jnum')
		seqCol = Gatherer.columnNumber (cols, 'sequencenum')
		evidenceKeyCol = Gatherer.columnNumber (cols, '_annotevidence_key')
		annotTypeKeyCol = Gatherer.columnNumber (cols,
			'_AnnotType_key')
		sexCol = Gatherer.columnNumber (cols, 'sex')

		# new annot key -> 1 (once done)
		done = {}

		logger.debug("looping through mp annotation rows")
		systemDict = {}
		systemSet = set([])
		for row in rows:
			# base values that we'll need later
			annotKey = row[annotCol]
			evidenceKey = row[evidenceKeyCol]
			gkey = row[gkeyCol]
			genotypeID = row[gidCol]
			term = row[termCol]
			termKey = row[termKeyCol]
			if termKey not in self.headerTermMap:
				logger.info("cannot find header term for %s "%term)
				continue
			qualifier = row[qualifierCol]
			# default call is abnormal
			call = 1
			# Special case to modify "no phenotypic analysis".
			# Per Janan on 1/11/2013 we are always displaying this term as not a Normal
			if qualifier and termKey != NO_PHENOTYPIC_ANALYSIS_KEY:
				call = 0
			jnum = row[jnumCol]
			sourceDisplay = self.getSource(evidenceKey, jnum)
			sex = row[sexCol]
			if sex not in SEX_SORT_ORDER:
				logger.info("invalid sex value found: %s,annotKey=%s,genotypeKey=%s"%(sex,annotKey,gkey))
				continue
			termID = row[termIDCol]
			termSeq = row[seqCol]
			systems = self.headerTermMap[termKey]
			for system in systems:
				# duplicate each annotation for every system the term maps to (could be more than one)
				systemSet.add(system)

				#aggregate terms under each system
				termObj = (term,termID,termKey,termSeq,evidenceKey,call,sex,jnum,sourceDisplay)
				sr_key = "%s%s"%(system,gkey)
				if sr_key in systemDict:
					systemObj = systemDict[sr_key]
					systemObj["terms"].append(termObj)
				else:
					systemObj = {"system":system,"terms":[termObj],
						"gkey":gkey}
					systemDict[sr_key] = systemObj

		# iterate the system groups and then create all the related tables that hang off of mp_system
		system_count = 0
		term_count = 0
		sex_count = 0
		annot_count = 0
		ref_count = 0
		note_count = 0

		lookup = Lookup.Lookup ('mgi_notetype', '_NoteType_key',
			'noteType')

		normalEvidenceKeys = self.getNormalEvidenceKeys()

		# use a map to keep track of jnum/term combos
		logger.debug("looping through %s mp/allele system groups to create all mp_* tables"%len(systemDict))
		for sr_key,systemObj in systemDict.items():
			gkey = systemObj["gkey"]
			system = systemObj["system"]
			system_count += 1
			headerKey = self.getHeaderKey(system)
			systemSeq = self.getSystemSeq(gkey,headerKey)
			systemRows.append([system_count,gkey,system,systemSeq])
			# unique the term set
			terms = systemObj["terms"]
			# calculate the relative depths for these terms
			# take the term keys and their sequence nums from VOC_Term and use them to calculate the sorts (and depths)
			termSeqMap = self.calculateSortAndDepths([[x[2],x[3]] for x in terms],headerKey)
			annotTermMap = {}
			sexAnnotMap = {}
			annotRefMap = {}
			# iterate terms, then group each child table by figuring out which data pieces define a unique key for that grouping
			# example: to define a unique annotation we need to know the term, sex, provider, and call
			for term,termID,termKey,termSeq,evidenceKey,call,sex,jnum,source in terms:
				if termID not in annotTermMap:
					term_count += 1
					annotTermMap[termID] = term_count
					# build annotation rows but only if it's a new jnum and term combo
					term_seq = termSeqMap[termKey]["seq"]
					termDepth = termSeqMap[termKey]["depth"]
					termRows.append([term_count,system_count,term,term_seq,termDepth,termID])
				mp_term_key = annotTermMap[termID]
				#build annot rows
				annot_map_key = (termID,sex,call)
				if annot_map_key not in annotRefMap:
					annot_count += 1
					annotRefMap[annot_map_key] = annot_count
					annotSeq = getSexCallSeq(sex,call)
					annotRows.append([annot_count,mp_term_key,call,sex,annotSeq])
				mp_annot_key = annotRefMap[annot_map_key]
				# build reference rows
				# references get attached to both an annotation and the term (to accomadate both displays in the genotype popup)
				ref_count += 1
				refRows.append([ref_count,mp_term_key,mp_annot_key,jnum,source[0],source[1]])
				# build notes
				for (note, note_type) in self.getNotes(evidenceKey):
					note_count += 1
					noteType = lookup.get(note_type)

					if normalEvidenceKeys.has_key(evidenceKey):
						normal = 1
					else:
						normal = 0

					noteRows.append([note_count,ref_count,note,noteType,normal])
			if system_count % 1000 == 0:
				pass
				#logger.info("processed %s systems"%system_count);

		logger.debug("done creating mp_* tables")
		return systemRows,termRows,annotRows,refRows,noteRows

	###--- Phenotable functions ---###
	# builds all the phenotable_* tables
	def buildPhenoTableRows(self):
		# Build all the tables needed for the phenotype summary table
		# get mappings for header terms
		logger.debug("gathering map of header terms")
		self.headerTermMap = self.buildHeaderMap()

		systemRows = []
		termRows = []
		termCellRows = []
		systemCellRows = []
		providerRows = set([])

		cols, rows = self.results[1]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		gkeyCol = Gatherer.columnNumber (cols, 'genotype_key')
		gidCol = Gatherer.columnNumber (cols, 'genotype_id')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		termIDCol = Gatherer.columnNumber (cols, 'term_id')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		jnumCol = Gatherer.columnNumber (cols, 'jnum')
		seqCol = Gatherer.columnNumber (cols, 'sequencenum')
		annotTypeKeyCol = Gatherer.columnNumber (cols,
			'_AnnotType_key')
		sexCol = Gatherer.columnNumber (cols, 'sex')
		evidKeyCol = Gatherer.columnNumber (cols, '_AnnotEvidence_key')

		# new annot key -> 1 (once done)
		done = {}

		logger.debug("looping through pheno_* annotation rows")
		systemDict = {}
		systemSet = set([])
		for row in rows:
			# base values that we'll need later
			annotKey = row[annotCol]
			gkey = row[gkeyCol]
			genotypeID = row[gidCol]
			term = row[termCol]
			termKey = row[termKeyCol]
			if termKey not in self.headerTermMap:
				logger.info("cannot find header term for %s for genotype %s"%(term,gkey))
				continue
			qualifier = row[qualifierCol]
			call = 1
			# Special case to modify "no phenotypic analysis".
			# Per Janan on 1/11/2013 we are always displaying this term as not a Normal
			if qualifier and termKey != NO_PHENOTYPIC_ANALYSIS_KEY:
				call = 0
			jnum = row[jnumCol]
			sourceDisplay = self.getSource(row[evidKeyCol], jnum)
			sex = row[sexCol]
			if sex not in SEX_SORT_ORDER:
				logger.info("invalid sex value found: %s,annotKey=%s,genotypeKey=%s"%(sex,annotKey,gkey))
				continue
			termID = row[termIDCol]
			termSeq = row[seqCol]
			systems = self.headerTermMap[termKey]
			for system in systems:
				# duplicate each annotation for every system the term maps to (could be more than one)
				systemSet.add(system)

				#aggregate terms under each system
				termObj = (term,termID,termKey,termSeq)
				allele_keys = self.getAlleleKeys(gkey)

				for allele_key in allele_keys:
					# duplicate each annotation for every allele the genotype maps to

					# Since this is the first pass through the annotation data, we register various information to calculate stuff for the grid
					# register the calls for the grid cells
					self.registerCellCall(allele_key,gkey,sourceDisplay,sex,call,(system,termID))
					# register the calls for the grid cells
					self.registerCellCall(allele_key,gkey,sourceDisplay,sex,call,system)
					# register the sex for the genotype cells
					self.registerGenotypeSex(allele_key,gkey,sex)
					# add provider row
					self.providerRowsDict.setdefault(allele_key,{}).setdefault("providers",set([])).add(sourceDisplay)
					self.providerRowsDict[allele_key].setdefault("provider_rows",set([])).add((gkey,sourceDisplay))

					# Done with cells / headers nonsense

					# group the systems for future processing
					sr_key = "%s%s"%(system,allele_key)
					if sr_key in systemDict:
						systemObj = systemDict[sr_key]
						systemObj["terms"].append(termObj)
					else:
						systemObj = {"system":system,"terms":[termObj],
							"allele_key":allele_key}
						systemDict[sr_key] = systemObj

		#calculate system ordinals
		systemSeqMap = {}
		systemSet = list(systemSet)
		# this is essentially an alphabetical sort
		systemSet.sort()
		seq = 0
		for system in systemSet:
			seq += 1
			systemSeqMap[system] = seq
		# special case for phenotable
		# put normal phenotype last in list
		if NORMAL_PHENOTYPE in systemSeqMap:
			systemSeqMap[NORMAL_PHENOTYPE] = seq+1

		# iterate the system groups and make all the connected tables (terms, cells, etc)
		system_count = 0
		term_count = 0
		term_cell_count = 0
		system_cell_count = 0
		logger.debug("looping through pheno/allele system groups")
		for sr_key,systemObj in systemDict.items():
			system = systemObj["system"]
			system_count += 1
			allele_key = systemObj["allele_key"]
			systemRows.append([system_count,allele_key,system,systemSeqMap[system]])
			# unique the term set
			termSet = set(systemObj["terms"])
			# calculate the relative depths for these terms
			headerKey = self.getHeaderKey(system)
			# take the term keys and their sequence nums from VOC_Term and use them to calculate the sorts (and depths)
			# cols 2 and 3 are the termID and termKey of the term object we created somewhere above
			termSeqMap = self.calculateSortAndDepths([[x[2],x[3]] for x in termSet],headerKey)
			# iterate each term object
			for term,termID,termKey,termSeq in termSet:
				term_count += 1
				term_seq = termSeqMap[termKey]["seq"]
				termDepth = termSeqMap[termKey]["depth"]
				termRows.append([term_count,system_count,term,termID,termDepth,term_seq,termKey])
				# get the (term) grid cells
				pheno_genos = self.getRegisteredCellCalls(allele_key,(system,termID))
				for item in pheno_genos.values():
					term_cell_count += 1
					call = item["call"]
					cell_seq = item["seq"]
					genotype_key = item["genotype_key"]
					genotypeID = self.genotypeIDMap[genotype_key]
					genotype_seq = self.genotypeSeqMap[(allele_key,genotype_key)]
					termCellRows.append([term_cell_count,term_count,call,item['sex'],genotypeID,cell_seq,genotype_seq])
			# get the system grid cells
			pheno_genos = self.getRegisteredCellCalls(allele_key,system)
			for item in pheno_genos.values():
				system_cell_count += 1
				call = item["call"]
				cell_seq = item["seq"]
				genotype_key= item["genotype_key"]
				genotypeID = self.genotypeIDMap[genotype_key]
				genotype_seq = self.genotypeSeqMap[(allele_key,genotype_key)]
				systemCellRows.append([system_cell_count,system_count,call,item['sex'],genotypeID,cell_seq,genotype_seq])

			if system_count % 1000 == 0:
				# this is for debugging
				pass
				#logger.info("processed %s systems"%system_count);

		# give keys to provider rows
		provider_count = 0
		pRows = []
		for allele_key,item in self.providerRowsDict.items():
			providers = item["providers"]
			# suppress case where only row would be 'MGI'
			suppress_providers = len(providers)==1 and list(providers)[0]==(None, None)
			if suppress_providers:
				continue
			for genotype_key,provider in item["provider_rows"]:
				provider_count+=1
				#if suppress_providers:
				#	provider=""

				p_seq = self.getProviderSeq(provider)

				pRows.append( (provider_count,
					self.getGenotypeRowKey(allele_key,
						genotype_key),
					provider[0],
					provider[1], p_seq) )

		logger.debug("done creating pheno_* tables")
		return systemRows, termRows, termCellRows,systemCellRows,pRows

	# inits the disease information into the phenogrid for calculating headers
	def initDiseaseRows(self):
		self.resetPhenoGridMaps()
		cols, rows = self.results[7]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		gkeyCol = Gatherer.columnNumber (cols, 'genotype_key')
		gidCol = Gatherer.columnNumber (cols, 'genotype_id')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		termIDCol = Gatherer.columnNumber (cols, 'term_id')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		jnumCol = Gatherer.columnNumber (cols, 'jnum')
		seqCol = Gatherer.columnNumber (cols, 'sequencenum')
		annotTypeKeyCol = Gatherer.columnNumber (cols, '_AnnotType_key')

		logger.debug("looping through DO annotation rows")
		systemDict = {}
		systemSet = set([])
		disease_set = set([])
		for row in rows:
			# base values that we'll need later
			annotKey = row[annotCol]
			gkey = row[gkeyCol]
			genotypeID = row[gidCol]
			qualifier = row[qualifierCol]
			call = 1
			if qualifier:
				call = 0
			sourceDisplay=MGI
			sex=NA
			term = row[termCol]
			disease_set.add(term)
			termKey = row[termKeyCol]
			termID = row[termIDCol]
			termSeq = row[seqCol]
			allele_keys = self.getAlleleKeys(gkey)

			for allele_key in allele_keys:
				# duplicate each annotation for every allele the genotype maps to

				# Since this is the first pass through the annotation data, we register various information to calculate stuff for the grid
				# register the calls for the grid cells
				self.registerCellCall(allele_key,gkey,sourceDisplay,sex,call,termID)
				# register a call for the rolled up "disease models" section
				#self.registerCellCall(allele_key,gkey,sourceDisplay,sex,call,DISEASE_MODELS)
				# register the sex for the genotype cells
				self.registerGenotypeSex(allele_key,gkey,sex)
				# Done with cells / headers nonsense

		# calculate the disease sorts (smart-alpha)
		disease_set = list(disease_set)
		disease_set.sort(symbolsort.nomenCompare)
		count = 0
		for disease in disease_set:
			count += 1
			self.diseaseSeqMap[disease] = count
		# "disease models" should be the first term
		self.diseaseSeqMap[DISEASE_MODELS] = 0

	# actually build the rows for disease cells
	def buildDiseaseRows(self):
		diseaseRows = []
		dCellRows = []

		cols, rows = self.results[7]

		annotCol = Gatherer.columnNumber (cols, '_Annot_key')
		gkeyCol = Gatherer.columnNumber (cols, 'genotype_key')
		gidCol = Gatherer.columnNumber (cols, 'genotype_id')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')
		termCol = Gatherer.columnNumber (cols, 'term')
		termIDCol = Gatherer.columnNumber (cols, 'term_id')
		vocabKeyCol = Gatherer.columnNumber (cols, '_Vocab_key')
		qualifierCol = Gatherer.columnNumber (cols, 'qualifier')
		jnumCol = Gatherer.columnNumber (cols, 'jnum')
		seqCol = Gatherer.columnNumber (cols, 'sequencenum')
		annotTypeKeyCol = Gatherer.columnNumber (cols,
			'_AnnotType_key')

		logger.debug("looping through DO annotation rows")
		systemDict = {}
		systemSet = set([])
		disease_count = 0
		disease_cell_count = 0
		disease_row_map = {}
		# loop through the DO annotations for the second time to gather all the data cells
		for row in rows:
			# base values that we'll need later
			annotKey = row[annotCol]
			gkey = row[gkeyCol]
			genotypeID = row[gidCol]
			term = row[termCol]
			termKey = row[termKeyCol]
			termID = row[termIDCol]
			termSeq = row[seqCol]
			allele_keys = self.getAlleleKeys(gkey)

			for allele_key in allele_keys:
				# duplicate each annotation for every allele the genotype maps to
				disease_row_key = (allele_key,termID)
				if disease_row_key not in disease_row_map:
					disease_count += 1
					disease_row_map[disease_row_key] = disease_count
					# add disease row
					diseaseRows.append([disease_count,allele_key,term,termKey,self.diseaseSeqMap[term],termID,0])
					# add all the cells
					pheno_genos = self.getRegisteredCellCalls(allele_key,termID)
					for item in pheno_genos.values():
						disease_cell_count += 1
						call = item["call"]
						cell_seq = item["seq"]
						genotype_key = item["genotype_key"]
						genotypeID = self.genotypeIDMap[genotype_key]
						genotype_seq = self.genotypeSeqMap[(allele_key,genotype_key)]
						dCellRows.append([disease_cell_count,disease_count,call,genotypeID,cell_seq,genotype_seq])
				# add the "disease models" header term (if it does not exist)
				"""
				disease_row_key = (allele_key,DISEASE_MODELS)
				if disease_row_key not in disease_row_map:
					disease_count += 1
					disease_row_map[disease_row_key] = disease_count
					term = DISEASE_MODELS
					termID = DISEASE_MODELS
					# add disease row
					diseaseRows.append([disease_count,allele_key,term,diseaseSeqMap[term],termID,1])
					# add all the cells
					pheno_genos = self.getRegisteredCellCalls(allele_key,termID)
					for item in pheno_genos.values():
						disease_cell_count += 1
						call = item["call"]
						cell_seq = item["seq"]
						genotype_key = item["genotype_key"]
						genotypeID = genotypeIDMap[genotype_key]
						genotype_seq = genotypeSeqMap[(allele_key,genotype_key)]
						dCellRows.append([disease_cell_count,disease_count,call,genotypeID,cell_seq,genotype_seq])
				"""

		return diseaseRows,dCellRows

	def buildCenters (self):
		# gather the phenotyping / data interpretation centers

		# prepare for fake terms

		mgiKey = MAX_TERM_KEY + 1
		europhenomeKey = MAX_TERM_KEY + 2
		fKey = MAX_TERM_KEY + 3
		mKey = MAX_TERM_KEY + 4
		mrcKey = MAX_TERM_KEY + 5
		naKey = MAX_TERM_KEY + 6

		# populate basic lab data

		cols1, rows = self.results[8]

		termKeyCol = Gatherer.columnNumber (cols1, '_Term_key')
		termCol = Gatherer.columnNumber (cols1, 'term')
		abbrevCol = Gatherer.columnNumber (cols1, 'abbreviation')
		sequenceNumCol = Gatherer.columnNumber (cols1, 'sequenceNum')

		fakeRows = [ (mgiKey, MGI, MGI, mgiKey),
			(europhenomeKey, 'EuroPhenome', EUROPHENOME,
				europhenomeKey),
			(fKey, 'F', 'F', fKey),
			(mKey, 'M', 'M', mKey),
			(mrcKey, 'MRCHarwell', 'MRCHarwell', mrcKey),
			(naKey, 'NA', 'NA', naKey),
			]

		for (key, center, abbrev, seqNum) in fakeRows:
			row = [ '', '', '', '' ]
			row[termKeyCol] = key
			row[sequenceNumCol] = seqNum
			row[termCol] = center
			row[abbrevCol] = abbrev
			rows.append(row)

		# Not only do we need to build a set of output rows for the
		# phenotable_center table, we also need to cache a set of
		# center data in this gatherer for future use.

		outRows = []
		self.labCache = LabCache()

		for row in rows:
			termKey = row[termKeyCol]
			term = row[termCol]
			seqNum = row[sequenceNumCol]
			abbrev = row[abbrevCol]

			self.labCache.add(LabInfo (termKey, term, seqNum,
				abbrev) )

			outRows.append ([ termKey, term, abbrev, seqNum ])

		self.results[8] = (cols1, [])

		del rows
		gc.collect()

		# generate sequence numbers for all possible pairings of
		# phenotyping and interpretation centers

		temp = []
		for (pcKey, a, b, pcSeqNum) in outRows:
			for (icKey, d, e, icSeqNum) in outRows:
				temp.append ((pcSeqNum, icSeqNum, pcKey, icKey))
		temp.sort()

		self.providerSeqNum = {}

		i = 0
		for (pcSeqNum, icSeqNum, pcKey, icKey) in temp:
			i = i + 1
			self.providerSeqNum[(pcKey, icKey)] = i

		return outRows

	def cacheCenterProperties (self):
		# Cache the phenotyping/interpretation center for each
		# evidence record, as we'll need these later.  Assumes that we
		# have already called buildCenters().

		cols, rows = self.results[9]

		keyCol = Gatherer.columnNumber (cols, '_AnnotEvidence_key')
		termCol = Gatherer.columnNumber (cols, '_PropertyTerm_key')
		centerCol = Gatherer.columnNumber (cols, 'center')

		# each maps from _AnnotEvidence_key to _Term_key for the center
		self.phenotypingCenters = {}
		self.interpretationCenters = {}

		for row in rows:
			centerType = row[termCol]
			evidenceKey = row[keyCol]
			center = row[centerCol]

			labInfo = self.labCache.getByName(center)

			if labInfo == None:
				raise Error, 'Unknown center name: %s' % center

			labKey = labInfo.getTermKey()

			if centerType == PHENOTYPING_CENTER:
				self.phenotypingCenters[evidenceKey] = labKey
			elif centerType == INTERPRETATION_CENTER:
				self.interpretationCenters[evidenceKey] = labKey
			else:
				raise Error, 'Unknown center type: %s' % \
					centerType

		logger.debug ('Cached centers for %d evidence rows' % \
			len(rows))

		self.results[9] = (cols, [])
		del rows
		gc.collect()
		return

	def getSource (self, evidenceKey, jnum):
		# return a tuple identifying the source for the given
		# _AnnotEvidence_key as:
		#    (term key of phenotyping center,
		#     term key of interpretation center)

		pc = None
		ic = None

		if self.phenotypingCenters.has_key(evidenceKey):
			pc = self.phenotypingCenters[evidenceKey]
		if self.interpretationCenters.has_key(evidenceKey):
			ic = self.interpretationCenters[evidenceKey]

		# fallback on old rules, if no properties

		if (ic == None) and (pc == None):
			if jnum == SANGER_JNUM:
				icText = WTSI
			elif jnum == EUROPHENOME_JNUM:
				icText = EUROPHENOME
			else:
				icText = MGI

			labInfo = self.labCache.getByName(icText)
			ic = labInfo.getTermKey()

		return (pc, ic)

	###--- Program Flow ---###

	# here is defined the high level script for building all these tables
	# by gatherer convention we create tuples of (cols,rows) for every table we want to create, and append them to self.output
	def buildRows (self ):
		# gather the genotype to allele relationships first. We only want genotypes with pheno data, or disease models
		genotypeCols = ['phenotable_genotype_key','allele_key','genotype_key','genotype_seq','split_sex','sex_display','disease_only']
		genotypeRows = []

		logger.debug("preparing to build list of genotypes with phenotypes or disease models")
		self.buildGenotypes()
		logger.debug("done collecting genotypes")

		# gather the provider data for HTMP Phenotyping Centers
		logger.debug('preparing to gather phenotyping center data')
		centerCols = [ 'center_key', 'name', 'abbreviation',
			'sequence_num' ]
		centerRows = self.buildCenters()
		logger.debug('built %d phenotyping centers' % len(centerRows))
		self.cacheCenterProperties()

		# mp_annotation table olumns and rows
		systemCols = ['mp_system_key','genotype_key','system','system_seq']
		systemRows = []
		termCols = [ 'mp_term_key', 'mp_system_key',
			'term','term_seq','indentation_depth',
			'term_id' ],
		termRows = []
		annotCols = [ 'mp_annotation_key', 'mp_term_key', 'call',
			 'sex','annotation_seq'],
		annotRows = []
		refCols = ['mp_reference_key','mp_term_key','mp_annotation_key','jnum_id','phenotyping_center_key', 'interpretation_center_key']
		refRows = []
		noteCols = ['mp_note_key','mp_annotation_key','note',
			'note_type', 'has_normal_qualifier']
		noteRows = []

		logger.debug("preparing to build list of mp annotations")

		systemRows,termRows,annotRows,refRows,noteRows = self.buildMPRows()

		logger.debug("done building list of mp annotations.")

		# phenotable columns and rows
		ptSystemCols = [ 'phenotable_system_key', 'allele_key', 'system', 'system_seq']
		ptSystemRows = []
		ptTermCols = [ 'phenotable_term_key','phenotable_system_key', 'term',
			'term_id','indentation_depth','term_seq','term_key']
		ptTermRows = []

		ptProviderCols = [ 'unique_key','phenotable_genotype_key','phenotyping_center_key', 'interpretation_center_key', 'sequence_num' ]
		ptProviderRows = []

		ptTermCellCols = ['phenotable_term_cell_key','phenotable_term_key','call','sex',
			'genotype_id','cell_seq','genotype_seq']
		ptTermCellRows = []
		ptSystemCellCols = ['phenotable_system_cell_key','phenotable_system_key','call','sex',
			'genotype_id','cell_seq','genotype_seq']
		ptSystemCellRows = []

		logger.debug("preparing to build list of phenotable data")
		ptSystemRows,ptTermRows,ptTermCellRows,ptSystemCellRows,ptProviderRows = self.buildPhenoTableRows()
		logger.debug("done building list of phenotable data.")

		logger.debug("building phenotable genotype rows")
		genotypeRows = self.getGenotypeRows()
		logger.debug("done building phenotable genotype rows")

		# diseasetable columns and rows
		dGenotypeCols = ['diseasetable_genotype_key','allele_key','genotype_key','genotype_seq']
		logger.debug("building disease genotypes")
		dGenotypeRows = self.buildDiseaseGenotypes()
		logger.debug("done building disease genotypes")

		diseaseCols = ['diseasetable_disease_key','allele_key','disease','term_key', 'disease_seq','disease_id','is_header']
		diseaseRows = []
		dCellCols = ['diseasetable_disease_cell_key','diseasetable_disease_key','call',
			'genotype_id','cell_seq','genotype_seq']
		dCellRows = []

		logger.debug("doing first pass of disease model information to build header statistics")
		self.initDiseaseRows()
		logger.debug("done with first pass of disease model information")
		logger.debug("building disease rows")
		diseaseRows,dCellRows = self.buildDiseaseRows()
		logger.debug("done with building diseases")


		logger.debug("done. outputing to file")

		self.output = [ (systemCols, systemRows),(termCols,termRows),(annotCols,annotRows),
			(refCols,refRows), (noteCols,noteRows),
			(ptSystemCols,ptSystemRows),(ptTermCols,ptTermRows),(genotypeCols,genotypeRows),
			(ptProviderCols,ptProviderRows),
			(ptTermCellCols,ptTermCellRows),(ptSystemCellCols,ptSystemCellRows),
			(dGenotypeCols,dGenotypeRows),(diseaseCols,diseaseRows),(dCellCols,dCellRows),
			(centerCols, centerRows), ]
		return

	# this is a function that gets called for every gatherer
	def collateResults (self):
		global MAX_TERM_KEY

		cols, rows = self.results[10]
		MAX_TERM_KEY = rows[0][0]
		logger.debug('found max term key: %d' % MAX_TERM_KEY)

		# process all queries
		self.buildRows()
		return


###--- MGD Query Definitions---###
# all of these queries get processed before collateResults() gets called
cmds = [
	# 0. get all header terms mapped to terms
	'''
	select distinct d._object_key term_key,
		dh._object_key header_term_key,
		h.synonym header_term
	from
		DAG_Node d,
		DAG_Closure dc,
		DAG_Node dh,
		MGI_Synonym h
	where
		d._DAG_key = 4
		and d._Node_key = dc._Descendent_key
		and dc._Ancestor_key = dh._Node_key
		and dh._Label_key = 3
		and dh._Object_key = h._object_key
		and h._synonymtype_key=1021
	union
	select distinct d._object_key term_key,
		dh._object_key header_term_key,
		h.synonym header_term
	from
		DAG_Node d,
		DAG_Closure dc,
		DAG_Node dh,
		MGI_Synonym h
	where
		d._DAG_key = 4
		and d._Node_key = dc._Descendent_key
		and dc._Descendent_key = dh._Node_key
		and dh._Label_key = 3
		and dh._Object_key = h._object_key
		and h._synonymtype_key=1021
	''',
	# 1. get  MP annotations made to genotypes, and pull a brief
	# set of info for them up through their alleles to their markers
	# (only null qualifiers, to avoid NOT and "normal" annotations).
	'''
	select va._Annot_key,
		va._object_key genotype_key,
		vt._Term_key,
		vt.term,
		aa.accID term_id,
		vt._Vocab_key,
		vt.sequencenum,
		va._AnnotType_key,
		vq.term qualifier,
		ar.accID jnum,
		ag.accID genotype_id,
		ve._AnnotEvidence_key,
		vep.value sex
	from
		voc_annot va,
		voc_term vt,
		voc_term vq,
		acc_accession aa,
		voc_evidence ve,
		voc_evidence_property vep,
		acc_accession ar,
		acc_accession ag
	where
		va._AnnotType_key in (1002)
		and va._Term_key = vt._Term_key
		and va._Qualifier_key = vq._Term_key
		and va._Term_key = aa._Object_key
		and aa._MGIType_key = 13
		and aa.preferred = 1
		and ve._annot_key=va._annot_key
		and ar._object_key=ve._refs_key
		and ar._mgitype_key=1
		and ar._logicaldb_key=1
		and ar.prefixpart = 'J:'
		and ag._object_key=va._object_key
		and ag._mgitype_key=12
	     	and ag._logicaldb_key=1
		and ag.preferred=1
		and ve._annotevidence_key=vep._annotevidence_key
		and vep._propertyterm_key=%s
	'''%SEX_SPECIFICITY_KEY,
	# 2. Get the annotation notes
	'''
	select ve._annot_key,
		ve._annotevidence_key,
		nc.note,
		sequenceNum,
		mn._notetype_key
	from voc_evidence ve,
		mgi_note mn,
		mgi_notechunk nc
	where mn._mgitype_key=25
		and mn._notetype_key in (%s)
		and ve._annotevidence_key=mn._object_key
		and nc._note_key=mn._note_key
	order by ve._annotevidence_key, nc._note_key,nc.sequenceNum
	'''%(",".join(MP_NOTETYPES)),
	# 3. get header term sequences by genotype
	'''
	select _object_key genotype_key, _term_key, sequencenum
		from voc_annotheader
		where _annottype_key=1002
	''',
	# 4. Get dag information for mp terms
	'''
	select n1._object_key parent_key,
	vt1.term parent,
	n2._object_key child_key,
	vt2.term child
	from dag_edge e,
	dag_node n1,
	dag_node n2,
	voc_term vt1,
	voc_term vt2
	where e._dag_key=4
		and e._child_key=n2._node_key
		and e._parent_key = n1._node_key
		and n1._object_key = vt1._term_key
		and n2._object_key = vt2._term_key
	order by e._parent_key, vt2.term
	''',
	# 5. Get the allele key to genotype mapping
	'''
	select _genotype_key, _allele_key
	from gxd_allelegenotype
	''',
	# 6. Get the unique allele -> genotype combos
	# but only genotypes with mp annotations and disease models
	'''
	WITH genotype_ids AS (
		select _object_key,accid genotype_id
		from acc_accession
		where _mgitype_key=12
		    and preferred=1
		    and _logicaldb_key=1
	)
	select ag._allele_key,ag._genotype_key,gid.genotype_id,'mp'
	from gxd_allelegenotype ag,genotype_ids gid
	where exists (select 1 from voc_annot va where va._object_key=ag._genotype_key and va._annottype_key=1002)
		and gid._object_key=ag._genotype_key
	UNION
	select ag._allele_key,ag._genotype_key,gid.genotype_id,'do'
	from gxd_allelegenotype ag,genotype_ids gid
	where exists (select 1 from mrk_do_cache o where o._genotype_key=ag._genotype_key)
		and exists (select 1 from voc_annot va where va._object_key=ag._genotype_key and va._annottype_key=1002)
		and gid._object_key=ag._genotype_key
	''',
	# 7. get DO annotations made to genotypes
	'''
	select va._Annot_key,
		va._object_key genotype_key,
		vt._Term_key,
		vt.term,
		aa.accID term_id,
		vt._Vocab_key,
		vt.sequencenum,
		va._AnnotType_key,
		vq.term qualifier,
		ar.accID jnum,
		ag.accID genotype_id,
		ve._annotevidence_key
	from
		voc_annot va,
		voc_term vt,
		voc_term vq,
		acc_accession aa,
		voc_evidence ve,
		acc_accession ar,
		acc_accession ag
	where
		va._AnnotType_key in (1020)
		and va._Term_key = vt._Term_key
		and va._Qualifier_key = vq._Term_key
		and va._Term_key = aa._Object_key
		and aa._MGIType_key = 13
		and aa.preferred = 1
		and ve._annot_key=va._annot_key
		and ar._object_key=ve._refs_key
		and ar._mgitype_key=1
		and ar._logicaldb_key=1
		and ar.prefixpart = 'J:'
		and ag._object_key=va._object_key
		and ag._mgitype_key=12
		and ag._logicaldb_key=1
		and ag.preferred=1
		and exists (select 1 from voc_annot va2 where va2._object_key=va._object_key and va2._annottype_key=1002)
	''',

	# 8. gather the phenotyping centers and data interpretation centers
	'''select _Term_key, term, abbreviation, sequenceNum
	from voc_term
	where _Vocab_key = %d
	order by sequenceNum''' % PHENO_CENTER_VOCAB,

	# 9. get phenotyping/interpretation centers for MP annotations made
	# to genotypes (only null qualifiers, to avoid NOT and "normal"
	# annotations).
	'''
	select ve._AnnotEvidence_key,
		vep._PropertyTerm_key,
		vep.value center
	from voc_annot va,
		voc_evidence ve,
		voc_evidence_property vep
	where va._AnnotType_key in (1002)
		and ve._annot_key=va._annot_key
		and ve._annotevidence_key=vep._annotevidence_key
		and vep._propertyterm_key in (%d, %d)
	''' % (INTERPRETATION_CENTER, PHENOTYPING_CENTER),

	# 10. get the maximum term key, so we know where to insert fake terms
	'select max(_Term_key) from voc_term',

	# 11. get the set of annotation evidence keys which have annotations
	# with a "normal" qualifier
	'''select distinct e._AnnotEvidence_key
	from voc_annot a,
		voc_evidence e
	where a._Annot_key = e._Annot_key
		and a._Qualifier_key = %d
		and a._AnnotType_key = %d''' % (NORMAL_TERM_KEY, MP_ANNOT_TYPE)
	]

###--- Table Definitions ---###
# definition of output files, each as:
#	(filename prefix, list of fieldnames, table name)
files = [
	('mp_system',
		['mp_system_key','genotype_key','system','system_seq'],
		'mp_system'),
	('mp_term',
		[ 'mp_term_key', 'mp_system_key',
			'term','term_seq','indentation_depth',
			'term_id'],
		'mp_term'),
	('mp_annot',
		[ 'mp_annotation_key', 'mp_term_key', 'call',
			 'sex','annotation_seq'],
		'mp_annot'),
	('mp_reference',
		['mp_reference_key','mp_term_key','mp_annotation_key','jnum_id','phenotyping_center_key', 'interpretation_center_key'],
		'mp_reference'),
	('mp_annotation_note',
		[ 'mp_note_key','mp_annotation_key', 'note', 'note_type',
			'has_normal_qualifier'],
		'mp_annotation_note'),
	('phenotable_system',
		[ 'phenotable_system_key', 'allele_key', 'system', 'system_seq'],
		'phenotable_system'),
	('phenotable_term',
		[ 'phenotable_term_key','phenotable_system_key', 'term',
			'term_id','indentation_depth','term_seq','term_key'],
		'phenotable_term'),
	('phenotable_to_genotype',
		['phenotable_genotype_key','allele_key','genotype_key','genotype_seq','split_sex','sex_display','disease_only'],
		'phenotable_to_genotype'),
	('phenotable_provider',
		[ 'unique_key', 'phenotable_genotype_key',
			'phenotyping_center_key',
			'interpretation_center_key', 'sequence_num' ],
		'phenotable_provider'),
	('phenotable_term_cell',
		['phenotable_term_cell_key','phenotable_term_key','call','sex',
			'genotype_id','cell_seq','genotype_seq'],
		'phenotable_term_cell'),
	('phenotable_system_cell',
		['phenotable_system_cell_key','phenotable_system_key','call','sex',
			'genotype_id','cell_seq','genotype_seq'],
		'phenotable_system_cell'),
	('diseasetable_to_genotype',
		['diseasetable_genotype_key','allele_key','genotype_key','genotype_seq'],
		'diseasetable_to_genotype'),
	('diseasetable_disease',
		['diseasetable_disease_key','allele_key','disease','term_key','disease_seq','disease_id','is_header'],
		'diseasetable_disease'),
	('diseasetable_disease_cell',
		['diseasetable_disease_cell_key','diseasetable_disease_key','call',
			'genotype_id','cell_seq','genotype_seq'],
		'diseasetable_disease_cell'),
	('phenotable_center',
		[ 'center_key', 'name', 'abbreviation', 'sequence_num' ],
		'phenotable_center'),
	]

# global instance of a AnnotationGatherer
gatherer = AnnotationGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)

