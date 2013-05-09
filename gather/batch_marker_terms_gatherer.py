#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_terms' table in the front-end database

import os
import Gatherer
import logger
import config

###--- Classes ---###

class BatchMarkerTermsGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the batch_marker_terms table
	# Has: queries to execute against the source database
	# Does: queries the source database for strings to be searched by the
	#	batch query interface for markers, collates results, writes
	#	tab-delimited text file

	def collateResults (self):
		self.finalColumns = [ 'term', 'term_type', 'marker_key' ]
		self.finalResults = []
		i = 0

		# nomenclature: for each marker key, a given term should only
		# appear once, with preference to the lowest priority values
		# (priority is handled by the order-by on the query)

		done = {}	# (marker key, term) -> 1
		validKeys = {}	# marker key -> 1

		cols, rows = self.results[0]

		termCol = Gatherer.columnNumber (cols, 'term')
		typeCol = Gatherer.columnNumber (cols, 'term_type')
		keyCol = Gatherer.columnNumber (cols, 'marker_key')

		for row in rows:
			pair = (row[keyCol], row[termCol].lower())
			if not done.has_key(pair):
				self.finalResults.append ( [ row[termCol],
					row[typeCol], row[keyCol] ] )
				done[pair] = 1
				validKeys[row[keyCol]] = 1

		logger.debug ('Kept %d labels from %d nomen rows' % (
			len(done), len(rows)) )

		# handle IDs in two queries

		# we need to exclude NCBI Gene Model (and evidence) IDs if we
		# encounter an Entrez Gene ID for that marker

		# marker key -> 1
		ncbiExcluded = {}

		for (cols, rows) in self.results[1:4]:
			termCol = Gatherer.columnNumber (cols, 'term')
			typeCol = Gatherer.columnNumber (cols, 'term_type')
			keyCol = Gatherer.columnNumber (cols, 'marker_key')

			for row in rows:
				term = row[termCol]
				termType = row[typeCol]
				marker = row[keyCol]

				if not validKeys.has_key(marker):
					continue

				triple = (term, termType, marker)
				if done.has_key(triple):
					continue

				# eliminate NCBI Gene Model (and evidence) if
				# it duplicates a Entrez Gene ID

				if termType.lower() == 'entrez gene':
					ncbiExcluded[marker] = 1

				if termType.lower().startswith (
						'ncbi gene model'):
					if ncbiExcluded.has_key(marker):
						continue

				# if we made it this far, add the term

				self.finalResults.append ( [ term, termType,
					marker ] )
				done[triple] = 1

			logger.debug ('Processed %d IDs from query %d' % (
				len(rows), i) )
			i = i + 1

		# gather GO IDs for each term key

		(cols, rows) = self.results[4]
		termCol = Gatherer.columnNumber (cols, '_Object_key')
		idCol = Gatherer.columnNumber (cols, 'accID')

		ids = {}	# ids[term key] = [list of IDs for that term]

		for row in rows:
			term = row[termCol]
			if ids.has_key(term):
				ids[term].append(row[idCol])
			else:
				ids[term] = [ row[idCol] ]

		logger.debug ('Collected %d GO IDs for %d terms' % (
			len(rows), len(ids) ))

		# gather ancestor term keys for each term key

		(cols, rows) = self.results[5]
		parentCol = Gatherer.columnNumber (cols,
			'_AncestorObject_key')
		childCol = Gatherer.columnNumber (cols,
			'_DescendentObject_key')

		ancestors = {}	# ancestors[term key] = [list of parent keys]

		for row in rows:
			parent = row[parentCol]
			child = row[childCol]
			if ancestors.has_key(child):
				if parent not in ancestors[child]:
					ancestors[child].append(parent)
			else:
				ancestors[child] = [ parent ]

		logger.debug (
			'Collected %d relationships for %d child GO terms' \
				% (len(rows), len(ancestors) ))

		# go through our marker/GO annotations and produce a row in
		# finalResults for each term and rows for its ancestors

		(cols, rows) = self.results[6]
		markerCol = Gatherer.columnNumber (cols, '_Object_key')
		termCol = Gatherer.columnNumber (cols, '_Term_key')

		label = 'Gene Ontology (GO)'

		noID = {}	# keys for terms without IDs, as we find them

		beforeCount = len(self.finalResults)

		# A marker may be annotated to multiple children from the same
		# parent node in the DAG.  We do not want to replicate the
		# marker / GO ID relationships, so we track those we have
		# already handled.

		done = {}	# done[marker key] = { term key : 1 }

		for row in rows:
			termKey = row[termCol]
			markerKey = row[markerCol]

			if not validKeys.has_key(markerKey):
				continue

			# include IDs for all of the term's IDs and all of the
			# IDs for its ancestors

			termKeys = [ termKey ]
			if ancestors.has_key(termKey):
				termKeys = termKeys + ancestors[termKey]

			for key in termKeys:

				# if we've already done this marker/term pair,
				# skip it
				if done.has_key (markerKey):
					if done[markerKey].has_key(key):
						continue

				# if this GO term has no IDs, skip it
				if not ids.has_key(key):
					if not noID.has_key(key):
					    logger.debug (
						'No ID for GO term key: %s' \
						% key)
					    noID[key] = 1
					continue
				
				for id in ids[key]:
					row = [ id, label, markerKey ]
					self.finalResults.append (row)

				# remember that we've done this marker/term
				# pair

				if done.has_key(markerKey):
					done[markerKey][key] = 1
				else:
					done[markerKey] = { key : 1 }

		logger.debug ('Processed %d GO annotations for %d rows (including ancestors)' % (len(rows), len(self.finalResults) - beforeCount) )

		logger.debug ('%d total rows for table' % \
			len(self.finalResults))
		return

###--- globals ---###

# sybase uses a different syntax at the end of its 'case' statement, so we
# need to account for that
if config.SOURCE_TYPE in [ 'postgres', 'mysql' ]:
	caseEnd = 'end as term_type'
elif config.SOURCE_TYPE == 'sybase':
	caseEnd = "end 'term_type'"

cmds = [
	# 0. nomenclature for current mouse markers, including symbol, name,
	# synonyms, old symbols, old names, human synonyms, related
	# synonyms, ortholog symbols, and ortholog names (not allele symbols,
	# not allele names)
	'''select ml.label as term,
			ml.labelTypeName as term_type,
			mm._Marker_key as marker_key,
			ml.priority
		from mrk_marker mm,
			mrk_label ml
		where mm._Organism_key = 1
			and mm._Marker_Status_key in (1,3)
			and mm._Marker_key = ml._Marker_key
			and ml.priority not in (3,4)
		order by mm._Marker_key, ml.priority''',

	# 1. all public accession IDs for current mouse markers (excluding
	# the sequence IDs picked up in a later query from the related
	# sequences).  Ordering is so that Entrez IDs will come before NCBI.
	'''select a.accID as term,
			l.name as term_type,
			a._Object_key as marker_key
		from acc_accession a,
			acc_logicaldb l,
			mrk_marker m
		where a._MGIType_key = 2
			and a.private = 0
			and a._Object_key = m._Marker_key
			and m._Marker_Status_key in (1,3)
			and m._Organism_key = 1
			and a._LogicalDB_key not in (9,13,27,41)
			and a._LogicalDB_key = l._LogicalDB_key
		order by a._Object_key, l.name''',

	# 2. Genbank (9), RefSeq (27), and Uniprot (13 and 41) IDs for
	# sequences associated to current mouse markers.
	'''select a.accID as term,
			case
				when l.name = 'Sequence DB' then 'GenBank'
				else l.name
			%s,
			m._Marker_key as marker_key
		from seq_marker_cache s,
			mrk_marker m,
			acc_accession a,
			acc_logicaldb l
		where s._Marker_key = m._Marker_key
			and m._Marker_Status_key in (1,3)
			and m._Organism_key = 1
			and s._Sequence_key = a._Object_key
			and a._MGIType_key = 19
			and a.private = 0
			and a._LogicalDB_key in (9, 13, 27, 41)
			and a._LogicalDB_key = l._LogicalDB_key''' % caseEnd,

	# 3. RefSNP IDs for RefSNPs that are directly associated with markers
	# (no SubSNPs, no distance-based associations in a region around a
	# marker -- just direct associations)
	'''select a.accid as term,
		ldb.name as term_type,
		m._Marker_key as marker_key
	from snp_consensussnp_marker m,
		snp_accession a,
		acc_logicaldb ldb
	where a._LogicalDB_key = ldb._LogicalDB_key
		and m._ConsensusSnp_key = a._Object_key
		and a._MGIType_key = 30''',

	# need to do GO IDs and their descendent terms, so a marker can be
	# retrieved for either its directly annotated terms or any of their
	# ancestor terms for its annotated terms
	
	# 4. get the list of GO term IDs and keys
	'''select _Object_key,
			accID
		from acc_accession
		where _LogicalDB_key = 31
			and private = 0
			and _MGIType_key = 13''',

	# 5. get the GO DAG, mapping a term to all of its subterms
	'''select c._AncestorObject_key,
			c._DescendentObject_key
		from dag_closure c,
			voc_term t
		where c._MGIType_key = 13
			and c._AncestorObject_key = t._Term_key
			and t._Vocab_key = 4''',

	# 6. get the marker/GO annotations
	'''select _Object_key,
			_Term_key
		from voc_annot
		where _Qualifier_key not in (1614151, 1614155)
			and _AnnotType_key = 1000''', 

	# need to do RefSNP IDs (will require resolving how to access the SNP
	# database or how to integrate SNP data into mgd)
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
	Gatherer.AUTO, 'term', 'term_type', 'marker_key'
	]

# prefix for the filename of the output file
filenamePrefix = 'batch_marker_terms'

# global instance of a BatchMarkerTermsGatherer
gatherer = BatchMarkerTermsGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
