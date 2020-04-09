# Module: GenotypeClassifier.py
# Purpose: to provide an easy means to determine the classification (or type)
#	for a genotype (eg- conditional, hemizygous, etc.)

import dbAgnostic
import logger

###--- Globals ---###

# genotype key -> genotype type abbreviation (eg- hm, hz, etc.)
GENOTYPE_TYPES = {}

# (allele key, genotype key) -> integer sequence number to order the genotypes
# for an allele
SEQUENCE_NUM = {}

# (allele key, genotype key) -> string designation for a genotype for the
# allele (eg- hm3, ht4, etc.)
DESIGNATION = {}

# (allele key, genotype key) -> unique key for allele_to_genotype table
AG_KEY = {}

# genotype key -> 1 if is a disease model, missing if not
DISEASE_MODEL = {}

# genotype key -> 1 if has MP annotations, missing if not
HAS_PHENO_DATA = {}

# ordered abbreviations for type (for sorting preference)
TYPES = [ 'hm', 'ht', 'cn', 'cx', 'tg',  'ot' ]

###--- Private Functions ---###

def _specialSort (a, b):
	# a and b are both (allele key, sequence num, genotype key)
	# sorting rules:
	# 1. sort by allele key
	# 2. genotypes with MP or DO annotations come before those that
	#	don't
	# 3. prioritize by designation (order from TYPES)
	# 4. sort by sequence num
	# 5. (shouldn't happen) sort by genotype key

	if a[0] != b[0]:
		return cmp(a[0], b[0])

	genotypeA = a[2]
	genotypeB = b[2]

	if DISEASE_MODEL.has_key(genotypeA) or \
		HAS_PHENO_DATA.has_key(genotypeA):
			if not (DISEASE_MODEL.has_key(genotypeB) or \
				HAS_PHENO_DATA.has_key(genotypeB) ):
					return -1
	elif DISEASE_MODEL.has_key(genotypeB) or \
		HAS_PHENO_DATA.has_key(genotypeB):
			return 1

	# at this point, we have the same allele in both, and either both
	# have or both don't have MP/DO annotations.  sort based on
	# genotype type, preference in global TYPES.

	typeA = TYPES.index(GENOTYPE_TYPES[genotypeA])
	typeB = TYPES.index(GENOTYPE_TYPES[genotypeB])

	if typeA != typeB:
		return cmp(typeA, typeB) 

	# fall back on sequence num, then genotype key

	if a[1] != b[1]:
		return cmp(a[1], b[1])		# by sequence num
	return cmp(a[2], b[2])			# by genotype key
	
	

def _initialize():
	global GENOTYPE_TYPES, SEQUENCE_NUM, DESIGNATION, AG_KEY
	global DISEASE_MODEL, HAS_PHENO_DATA

	GENOTYPE_TYPES = {}

	conditionalQuery = '''select _Genotype_key
		from gxd_genotype
		where isConditional = 1'''

	# allele type "Transgeneic" = 847126
	# marker type Transgene = 12
	transgeneQuery = '''select distinct g._Genotype_key
                from gxd_allelegenotype g, 
				all_allele a,
				mrk_marker m
                where g._Allele_key = a._Allele_key
				and a._marker_key=m._marker_key
                	and a._Allele_Type_key in (847126)
				and m._marker_type_key=12
	'''

	complexQuery = '''select _Genotype_key, count(1)
		from gxd_allelepair
		group by _Genotype_key
		having count(1) > 1'''

	heterozygoteQuery = '''select _Genotype_key
		from gxd_allelepair
		where _Compound_key = 847167
			and _PairState_key = 847137'''

	homozygoteQuery = '''select _Genotype_key
		from gxd_allelepair
		where _Compound_key = 847167
			and _PairState_key = 847138'''
		
	allQuery = '''select _Genotype_key
		from gxd_genotype'''


	# queries and their respective abbreviations; later queries take
	# precedence when setting the type for a genotype
	queries = [ (allQuery, 'ot'),
			(homozygoteQuery, 'hm'),
			(heterozygoteQuery, 'ht'),
			(transgeneQuery, 'tg'),
			(complexQuery, 'cx'),
			(conditionalQuery, 'cn') ]

	for (query, abbrev) in queries:
		(cols, rows) = dbAgnostic.execute (query)

		genotypeCol = dbAgnostic.columnNumber (cols, '_Genotype_key')
		for row in rows:
			GENOTYPE_TYPES[row[genotypeCol]] = abbrev

		logger.debug ('Finished query %s with %d results' % (
			abbrev, len(rows)) ) 

	# determine which genotypes have disease and phenotype data

	DISEASE_MODEL = {}
	HAS_PHENO_DATA = {}

	diseaseQuery = '''select distinct _Object_key
		from voc_annot
		where _AnnotType_key = 1020'''

	phenoQuery = '''select distinct _Object_key
		from voc_annot
		where _AnnotType_key = 1002'''

	queries = [ (diseaseQuery, DISEASE_MODEL, 'DO'),
			(phenoQuery, HAS_PHENO_DATA, 'MP') ]

	for (query, d, vocab) in queries:
		(cols, rows) = dbAgnostic.execute (query)

		genotypeCol = dbAgnostic.columnNumber (cols, '_Object_key')
		for row in rows:
			d[row[genotypeCol]] = 1

		logger.debug ('Found %d genotypes with %s data' % (
			len(d), vocab) )

	# get the sequence number for each genotype for each allele, and build
	# the abbreviations at the same time.  note that those with MP or DO
	# annotations must come before those that don't.

	SEQUENCE_NUM = {}
	DESIGNATION = {}
	AG_KEY = {}

	orderingQuery = '''select distinct _Allele_key, sequenceNum,
				_Genotype_key
		from gxd_allelegenotype
		order by _Allele_key, sequenceNum'''

	(cols, rows) = dbAgnostic.execute (orderingQuery)

	alleleCol = dbAgnostic.columnNumber (cols, '_Allele_key')
	genotypeCol = dbAgnostic.columnNumber (cols, '_Genotype_key')
	seqNumCol = dbAgnostic.columnNumber (cols, 'sequenceNum')

	lastAllele = None	# allele key from previous record
	seqNumByAllele = None	# counter for genotypes of current allele
	alleleGenotypeKey = 0	# unique db key for allele/genotype pair

	tuples = []

	for row in rows:
		genotype = row[genotypeCol]
		allele = row[alleleCol]
		seqNum = row[seqNumCol]

		tuples.append ( ( allele, seqNum, genotype ) )

	tuples.sort (_specialSort)
	logger.debug ('Prioritized genotypes with MP/DO annotations')

	for (allele, seqNum, genotype) in tuples:
		abbrev = GENOTYPE_TYPES[genotype]

		alleleGenotypeKey = 1 + alleleGenotypeKey

		if allele == lastAllele:
			seqNumByAllele = 1 + seqNumByAllele
		else:
			seqNumByAllele = 1
			lastAllele = allele

		agTuple = (allele, genotype)

		SEQUENCE_NUM[agTuple] = seqNumByAllele
		DESIGNATION[agTuple] = '%s%d' % (abbrev, seqNumByAllele)
		AG_KEY[agTuple] = alleleGenotypeKey

	logger.debug ('Ordered %d genotype/allele pairs' % len(AG_KEY)) 
	return

###--- Functions ---###

def getClass (genotypeKey):
	# determine the classification (or type) abbreviation for the genotype
	# identified by 'genotypeKey'

	if not GENOTYPE_TYPES:
		_initialize()

	if GENOTYPE_TYPES.has_key(genotypeKey):
		return GENOTYPE_TYPES[genotypeKey]
	return None

def getDesignation (alleleKey, genotypeKey):
	if not DESIGNATION:
		_initialize()

	if DESIGNATION.has_key ( (alleleKey, genotypeKey) ):
		return DESIGNATION[ (alleleKey, genotypeKey) ]
	return None

def getSequenceNum (alleleKey, genotypeKey):
	if not SEQUENCE_NUM:
		_initialize()

	if SEQUENCE_NUM.has_key ( (alleleKey, genotypeKey) ):
		return SEQUENCE_NUM[ (alleleKey, genotypeKey) ]
	return None

def getAlleleGenotypeKey (alleleKey, genotypeKey):
	if not AG_KEY:
		_initialize()

	if AG_KEY.has_key ( (alleleKey, genotypeKey) ):
		return AG_KEY[ (alleleKey, genotypeKey) ]
	return None

def hasPhenoData (genotypeKey):
	if not HAS_PHENO_DATA:
		_initialize()

	if HAS_PHENO_DATA.has_key(genotypeKey):
		return HAS_PHENO_DATA[genotypeKey]
	return 0

def isDiseaseModel (genotypeKey):
	if not DISEASE_MODEL:
		_initialize()

	if DISEASE_MODEL.has_key(genotypeKey):
		return DISEASE_MODEL[genotypeKey]
	return 0

