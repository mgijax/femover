#!/usr/local/bin/python
# 
# gathers data for the 'batch_marker_terms' table in the front-end database

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

		for (cols, rows) in self.results[0:6]:
			termCol = Gatherer.columnNumber (cols, 'term')
			typeCol = Gatherer.columnNumber (cols, 'term_type')
			keyCol = Gatherer.columnNumber (cols, 'marker_key')

			for row in rows:
				self.finalResults.append ( [ row[termCol],
					row[typeCol], row[keyCol] ] )

			logger.debug ('Processed %d rows from query %d' % (
				len(rows), i) )
			i = i + 1

		# gather GO IDs for each term key

		(cols, rows) = self.results[6]
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

		(cols, rows) = self.results[7]
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

		(cols, rows) = self.results[8]
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

if config.SOURCE_TYPE in [ 'postgres', 'mysql' ]:
	caseEnd = 'end as term_type'
elif config.SOURCE_TYPE == 'sybase':
	caseEnd = "end 'term_type'"

cmds = [
	# 0. current symbol for mouse markers
	'''select symbol as term,
			'current symbol' as term_type,
			_Marker_key as marker_key
		from mrk_marker
		where _Organism_key = 1
			and _Marker_Status_key in (1,3)''',

	# 1. synonyms for current mouse markers
	'''select s.synonym as term,
			t.synonymType as term_type,
			s._Object_key as marker_key
		from mgi_synonym s,
			mrk_marker m,
			mgi_synonymtype t
		where s._SynonymType_key = t._SynonymType_key
			and s._Object_key = m._Marker_key
			and m._Marker_Status_key in (1,3)
			and m._Organism_key = 1
			and t._MGIType_key = 2''',

	# 2. ortholog symbols for current mouse markers
	'''select distinct om.symbol as term,
			o.commonName as term_type,
			mm._Marker_key as marker_key
		from mrk_marker mm,
			mrk_homology_cache mc,
			mrk_homology_cache oc,
			mrk_marker om,
			mgi_organism o
		where mm._Marker_Status_key in (1,3)
			and mm._Organism_key = 1
			and mm._Marker_key = mc._Marker_key
			and mc._Class_key = oc._Class_key
			and oc._Marker_key = om._Marker_key
			and oc._Organism_key != 1
			and om._Organism_key = o._Organism_key''',

	# 3. select accession IDs directly associated with current mouse
	# markers: MGI (1), Entrez Gene (55), Ensembl Gene Model (60), VEGA
	# Gene Model (85), UniGene (23), and miRBase (83)
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
			and a._LogicalDB_key in (1, 55, 60, 85, 23, 83)
			and a._LogicalDB_key = l._LogicalDB_key''',

	# 4. Affy IDs for current mouse markers
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
			and a._LogicalDB_key in (select sm._Object_key
				from mgi_setmember sm, mgi_set s
				where sm._Set_key = s._Set_key
					and s.name = 'MA Chip')
			and a._LogicalDB_key = l._LogicalDB_key''',
	
	# 5. Genbank (9), RefSeq (27), and Uniprot (13 and 41) IDs for
	# sequences associated to current mouse markers.  note that this
	# syntax for 'case' only works for postgres.
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
			and a.private = 0
			and a._LogicalDB_key in (9, 13, 27, 41)
			and a._LogicalDB_key = l._LogicalDB_key''' % caseEnd,

	# need to do GO IDs and their descendent terms, so a marker can be
	# retrieved for either its directly annotated terms or any of their
	# ancestor terms for its annotated terms
	
	# 6. get the list of GO term IDs and keys
	'''select _Object_key,
			accID
		from acc_accession
		where _LogicalDB_key = 31
			and private = 0
			and _MGIType_key = 13''',

	# 7. get the GO DAG, mapping a term to all of its subterms
	'''select c._AncestorObject_key,
			c._DescendentObject_key
		from dag_closure c,
			voc_term t
		where c._MGIType_key = 13
			and c._AncestorObject_key = t._Term_key
			and t._Vocab_key = 4''',

	# 8. get the marker/GO annotations
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
