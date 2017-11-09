#!/usr/local/bin/python
# 
# gathers data for the 'term_sibling' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class TermSiblingGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the term_sibling table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for siblings of terms,
	#	collates results, writes tab-delimited text file

	def getMinKeyQuery(self):
		return 'select min(_Vocab_key) from voc_vocab'
	
	def getMaxKeyQuery(self):
		return 'select max(_Vocab_key) from voc_vocab'
	
	def postprocessResults(self):
		# for any vocabularies without sequence numbers, compute and add them
		# also, we need to add edge numbers

		snCol = Gatherer.columnNumber(self.finalColumns, 'sequenceNum')
		termCol = Gatherer.columnNumber(self.finalColumns, 'term')
		
		positions = {}		# lowercase term : [ positions in self.finalResults of that term ]
		pos = 0
		maxSeqNum = 0
		
		for row in self.finalResults:
			if row[snCol] == None:
				lowerTerm = row[termCol].lower()
				if lowerTerm in positions:
					positions[lowerTerm].append(pos)
				else:
					positions[lowerTerm] = [ pos ]
			else:
				maxSeqNum = max(maxSeqNum, row[snCol])

			pos = pos + 1
		
		if len(positions) > 0:
			logger.debug('Found %d terms to sort' % len(positions))

			toSort = positions.keys()
			toSort.sort()
			logger.debug('Sorted terms')

			self.convertFinalResultsToList()
			seqNum = maxSeqNum + 1
			for term in toSort:
				for pos in positions[term]:
					self.finalResults[pos][snCol] = seqNum
					seqNum = seqNum + 1

			logger.debug("Assigned sequenceNum starting at %d" % (maxSeqNum + 1))
		return

###--- globals ---###

cmds = [
	'''with selected_vocabs as (select _Vocab_key
			from voc_vocab
			where _Vocab_key >= %d and _Vocab_key < %d
		), primary_id as (select a._Object_key as _Term_key, a.accID
			from voc_term t, acc_accession a, selected_vocabs v
			where t._Term_key = a._Object_key
				and a._MGIType_key = 13
				and a.preferred = 1
				and a.private = 0
				and t._Vocab_key = v._Vocab_key
				and a._LogicalDB_key = (select min(_LogicalDB_key)
					from acc_accession b
					where b._MGIType_key = 13
						and b.preferred = 1
						and b.private = 0
						and a._Object_key = b._Object_key)
		), leaves as (select t._Term_key, 1 as is_leaf
			from voc_term t, selected_vocabs v
			where t._Vocab_key = v._Vocab_key
				and not exists (select 1
					from dag_node dn, dag_edge de
					where t._Term_Key = dn._Object_key
						and dn._Node_key = de._Parent_key)
		)
		select t._Vocab_key, p._Object_key as parentKey, c._Object_key as childKey, s._Object_key as siblingKey,
			st.term, st.sequenceNum, l.label, id.accID,
			case
				when v.isSimple = 1 then 0
				when lv.is_leaf = 1 then 1
				else 0
			end as is_leaf
		from voc_term t
		inner join voc_vocab v on (t._Vocab_key = v._Vocab_key)
		inner join dag_node p on (t._Term_key = p._Object_key)
		inner join dag_edge e on (p._Node_key = e._Parent_key)
		inner join dag_node c on (e._Child_key = c._Node_key)
		inner join dag_edge es on (p._Node_key = es._Parent_key and es._Child_key != e._Child_key)
		inner join dag_node s on (es._Child_key = s._Node_key)
		inner join dag_label l on (es._Label_key = l._Label_key)
		inner join voc_term st on (s._Object_key = st._Term_key)
		inner join selected_vocabs vv on (t._Vocab_key = vv._Vocab_key)
		left outer join primary_id id on (st._Term_key = id._Term_key)
		left outer join leaves lv on (st._Term_key = lv._Term_key)'''
	]

# order of fields (from the query results) to be written to the output file
# (use the parentKey to identify the path number)
fieldOrder = [ Gatherer.AUTO, 'childKey', 'siblingKey', 'term', 'accID', 'sequenceNum',
	'is_leaf', 'label', 'parentKey' ]

# prefix for the filename of the output file
filenamePrefix = 'term_sibling'

# global instance of a TermSiblingGatherer
gatherer = TermSiblingGatherer (filenamePrefix, fieldOrder, cmds)
gatherer.setChunkSize(4)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
