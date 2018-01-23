#!/usr/local/bin/python
# 
# gathers data for the 'queryform_option' table in the front-end database

import Gatherer
import logger

###--- Constants ---###

# list of MCV terms that should be expanded by default on marker QF
expandedMcvOptions = [ 'all feature types', 'gene' ]

###--- Classes ---###

class QFOptionGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the queryform_option table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for query form
	#	options, collates results, writes tab-delimited text file

	def processBasicChromosomeQuery (self):
		# process query 0 : marker and allele QF chromosome field

		cols, rows = self.results[0]

		chromoCol = Gatherer.columnNumber (cols, 'chromosome')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')

		for row in rows:
			chromosome = row[chromoCol]
			seqNum = row[seqNumCol]

			displayChrom = chromosome
			if chromosome == 'XY':
				displayChrom = 'XY(pseudoautosomal)'
			elif chromosome == 'UN':
				displayChrom = 'Unknown'
			elif chromosome == 'MT':
				displayChrom = 'Mitochondrial'

			self.finalResults.append ( [ 'allele', 'chromosome',
				displayChrom, chromosome, None, seqNum, None,
				None, None, 0 ] )

			self.finalResults.append ( [ 'marker', 'chromosome',
				displayChrom, chromosome, None, seqNum, None,
				None, None, 0 ] )

		soFar = len(self.finalResults)
		logger.debug ('Added %d chromosome rows' % soFar)
		return

	def processFeatureTypeQueries (self):
		# process queries 3 (MCV definitions) and 1 (MCV terms)

		soFar = len(self.finalResults)
		cols, rows = self.results[3]

		termCol = Gatherer.columnNumber (cols, '_Term_key')
		noteCol = Gatherer.columnNumber (cols, 'note')

		defs = {}	# term key : note

		for row in rows:
			termKey = row[termCol]
			note = row[noteCol]
			defs[termKey] = note 

		# 1. marker QF : MCV (marker type) field

		cols, rows = self.results[1]

		parentCol = Gatherer.columnNumber (cols, 'parent')
		childCol = Gatherer.columnNumber (cols, 'child')
		countCol = Gatherer.columnNumber (cols, 'markerCount')
		termKeyCol = Gatherer.columnNumber (cols, '_Term_key')

		parents = []	# list of parents, in order
		children = {}	# children[parent] = list of parent's children
		termKey = {}	# termKey[term] = key for that term
		mrkCount = {}	# mrkCount[term] = count of markers for term

		for row in rows:
			parent = row[parentCol]
			child = row[childCol]
			count = row[countCol]
			key = row[termKeyCol]

			if parent not in parents:
				parents.append(parent)

			if children.has_key(parent):
				if child not in children[parent]:
					children[parent].append(child)
			else:
				children[parent] = [ child ]

			termKey[child] = key
			mrkCount[child] = count

		logger.debug ('Found children for %d MCV terms' % len(children))

		# reverse the lists of children, to ease processing later

		for parent in children.keys():
			children[parent].reverse()

		logger.debug ('Reversed lists of children')

		# now use a depth-first traversal to give the rows in order
		# we want them displayed, with proper indenting

		seqNum = 0
		indent = 0

		if len(parents) < 1:
			raise Gatherer.Error, 'Missing MCV vocabulary data'

		head = parents[0]

		# each tuple in toDo is (term, indent level)
		toDo = [ (head, 0) ]

		while toDo:
			# pop next to-do item from stack

			(nextTerm, indent) = toDo[0]
			toDo = toDo[1:]

			# add that item to the list of output (except the
			# 'head' item)

			if nextTerm != 'head':
				key = termKey[nextTerm]

				definition = ''
				if defs.has_key(key):
					definition = defs[key]

				expanded = 0
				if nextTerm in expandedMcvOptions:
					expanded = 1

				seqNum = seqNum + 1
				self.finalResults.append ( [ 'marker', 'mcv',
					nextTerm, key, definition, seqNum,
					indent, mrkCount[nextTerm], 'marker',
					expanded,
					] )

			# add children for that item to the to-do list

			if children.has_key(nextTerm):
				for child in children[nextTerm]:
					toDo.insert(0, (child, indent + 1) )

		increment = len(self.finalResults) - soFar
		logger.debug ('Added %s MCV term rows' % increment)
		return

	def processPhenotypePopupQuery (self):
		# process query 2 (phenotype popup on allele query form)

		soFar = len(self.finalResults)
		cols, rows = self.results[2]

		headerCol = Gatherer.columnNumber (cols, 'shortHeader')
		booleanCol = Gatherer.columnNumber (cols, 'searchExpression')
		seqNumCol = Gatherer.columnNumber (cols, 'sequenceNum')
		ct = 0

		for row in rows:
			term = row[headerCol]
			booleanExpression = row[booleanCol].strip()
			seqNum = row[seqNumCol]

			if seqNum == None:
				seqNum = len(rows) + ct
				ct = ct + 1

			self.finalResults.append ( [ 'allele', 'phenotype',
				term, booleanExpression, None, seqNum, None,
				None, None, 0 ] )

		increment = len(self.finalResults) - soFar
		logger.debug ('Added %s MP header rows' % increment) 
		return

	def processBuildNumberQuery (self):
		# process query 4 (genome build number for mouse markers)

		cols, rows = self.results[4]

		versionCol = Gatherer.columnNumber (cols, 'version')

		version = 'unknown build'

		if len(rows) > 0:
			version = rows[0][versionCol]

		self.finalResults.append ( [ 'marker', 'build_number',
			version, version, None, 1, None, None, None, 0 ] )
		logger.debug('Added row for genome build %s' % version)
		return

	def processSnpQueries (self):
		# process queries 5-8 for SNP QF options

		queries = [ (5, 'strain'), (6, 'chromosome'),
			(7, 'function class'), (8, 'variation class'),
			(9, 'build number') ]

		for (qry, field) in queries:
			cols, rows = self.results[qry]
			termCol = Gatherer.columnNumber(cols, 'term')
			seqNum = 0

			for row in rows:
				seqNum = seqNum + 1
				term = row[termCol]
				self.finalResults.append ( [ 'snp', field, 
					term, term, None, seqNum, None,
					None, None, 0 ] )

			logger.debug('Added %d rows for SNPs %s' % (
				seqNum, field))
		return

	def processSnpExtras (self):
		# a couple of extra option lists that aren't based in a db

		coordRanges = [
			('within the gene(s)', 0),
			('include 2 kb upstream and downstream of the gene(s)', 2000),
			('include 10 kb upstream and downstream of the gene(s)', 10000),
			]

		directions = [
			('EITHER', 'EITHER'),
			('upstream', 'upstream'),
			('downstream', 'downstream'),
			]

		lists = [ ('coordinate range', coordRanges),
			('direction', directions),
			]

		for (field, values) in lists:
			seqNum = 0
			for (display, submit) in values:
				seqNum = seqNum + 1
				self.finalResults.append ( [ 'snp', field,
					display, submit, None, seqNum, None,
					None, None, 0 ] )

			logger.debug('Added %d rows for SNPs %s' % (
				len(values), field))
		return

	def collateResults (self):
		self.finalColumns = [ 'formName', 'fieldName', 'displayValue',
			'submitValue', 'helpText', 'sequenceNum', 'indentLevel',
			'objectCount', 'objectType', 'showExpanded', ]
		self.finalResults = []

		self.processBasicChromosomeQuery()
		self.processFeatureTypeQueries()
		self.processPhenotypePopupQuery()
		self.processBuildNumberQuery()
		self.processSnpQueries() 
		self.processSnpExtras()
		return

###--- globals ---###

cmds = [
	# 0. marker QF and allele QF : chromosome field
	'''select chromosome,
		sequenceNum
	from mrk_chromosome
	where _Organism_key = 1
	order by sequenceNum''',

	# 1. marker QF : MCV (marker type) field
	'''select distinct 'head' as parent,
		0 as _Parent_key,
		t.term as child,
		p._Label_key,
		cc.markerCount,
		p._Object_key as _Term_key,
		1 as sequenceNum
	from dag_closure c, 
		voc_term t,
		mrk_mcv_count_cache cc,
		dag_node p,
		dag_edge d
	where c._DAG_key = 9
		and not exists (select 1
			from DAG_Closure c2
			where c2._DAG_key = 9
				and c2._Descendent_key = c._Ancestor_key)
		and c._AncestorObject_key = t._Term_key
		and c._AncestorObject_key = cc._MCVTerm_key
		and c._Ancestor_key = d._Parent_key
		and c._Ancestor_key = p._Node_key
	union
	select distinct pt.term as parent,
		d._Parent_key,
		ct.term as child,
		c._Label_key,
		cc.markerCount,
		ct._Term_key,
		d.sequenceNum
	from dag_edge d,
		dag_node p,
		dag_node c,
		voc_term pt,
		voc_term ct,
		mrk_mcv_count_cache pc,
		mrk_mcv_count_cache cc
	where d._DAG_key = 9
		and d._Parent_key = p._Node_key
		and p._Object_key = pt._Term_key
		and p._Object_key = pc._MCVTerm_key
		and d._Child_key = c._Node_key
		and c._Object_key = ct._Term_key
		and c._Object_key = cc._MCVTerm_key
	order by _Parent_key, sequenceNum''',

	# 2. allele QF : phenotype popup field
	'''select vt.term,
		ms.synonym AS shortHeader,
		mnc.note AS searchExpression,
		vt.sequenceNum
	from voc_term vt, 
		voc_vocab vv,
		mgi_note mn,
		mgi_notechunk mnc, 
		mgi_notetype mnt, 
		dag_dag dd, 
		dag_node dn, 
		dag_label dl, 
		mgi_synonym ms, 
		mgi_synonymtype mst
	where vv.name = 'Mammalian Phenotype' 
		and vv._Vocab_key = vt._Vocab_key 
		and vt._Term_key = dn._Object_key 
		and dn._DAG_key = dd._DAG_key 
		and dd.name = 'Mammalian Phenotype' 
		and dn._Label_key = dl._Label_key 
		and dl.label = 'Header' 
		and vt._Term_key = ms._Object_key 
		and ms._MGIType_key = 13 
		and ms._SynonymType_key = mst._SynonymType_key 
		and mst.synonymType = 'Synonym Type 1' 
		and vt._Term_key = mn._Object_key 
		and mn._Note_key = mnc._Note_key 
		and mn._MGIType_key = 13 
		and mn._NoteType_key = mnt._NoteType_key 
		and mnt.noteType = 'Note Type 1'
	order by vt.sequenceNum''',

	# 3. definitions for MCV terms on marker QF
	'''select t._Term_key, x.note
	from voc_vocab v,
		voc_term t,
		voc_text x
	where v._Vocab_key = t._Vocab_key
		and v.name = 'Marker Category'
		and t._Term_key = x._Term_key
	order by t._Term_key, x.note''',

	# 4. genome build number for mouse markers
	'''select distinct version
	from MRK_Location_Cache
	where _Organism_key = 1
		and version is not null
	order by version desc''', 

	# 5. SNP QF : distinct list of strains
	'''select distinct strain as term, sequenceNum
	from snp_strain
	order by sequenceNum''',

	# 6. SNP QF : chromosomes with SNP data
	'''select distinct chromosome as term, sequenceNum
	from snp_coord_cache
	order by sequenceNum''',

	# 7. SNP QF : dbSNP function classes (skip ones we added)
	'''select t.term
	from voc_term t
	where _Vocab_key = 49
		and t.term != 'within coordinates of'
		and t.term != 'within distance of'
		and exists (select 1 from snp_consensussnp_marker c
		where t._Term_key = c._Fxn_key)
	order by t.term''',

	# 8. SNP QF : variation classes
	'''select t.term, t.sequenceNum
	from voc_term t
	where _Vocab_key = 50
		and exists (select 1 from snp_consensussnp c
		where t._Term_key = c._VarClass_key)
	order by t.sequenceNum''',

	# 9. SNP QF : SNP build version
	'''select snp_data_version as term
	from snp.mgi_dbinfo''',
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'formName', 'fieldName', 'displayValue', 'submitValue',
	'helpText', 'sequenceNum', 'indentLevel', 'objectCount', 'objectType',
	'showExpanded',
	]

# prefix for the filename of the output file
filenamePrefix = 'queryform_option'

# global instance of a QFOptionGatherer
gatherer = QFOptionGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
