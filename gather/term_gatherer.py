#!/usr/local/bin/python
# 
# gathers data for the 'term' table in the front-end database

import Gatherer
import VocabUtils
import logger

###--- Classes ---###

class TermGatherer (Gatherer.ChunkGatherer):
	# Is: a data gatherer for the term table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for vocabulary terms,
	#	collates results, writes tab-delimited text file

	def getMinKeyQuery(self):
		return 'select min(_Vocab_key) from voc_vocab'
	
	def getMaxKeyQuery(self):
		return 'select max(_Vocab_key) from voc_vocab'
	
	def postprocessResults(self):
		# for any vocabularies without sequence numbers, compute and add them

		snCol = Gatherer.columnNumber(self.finalColumns, 'sequenceNum')
		termCol = Gatherer.columnNumber(self.finalColumns, 'term')
		
		toSort = []			#  [ (lowercase term, row index), ... ]
		pos = 0
		
		for row in self.finalResults:
			if row[snCol] == None:
				toSort.append( (row[termCol].lower(), pos) )
			pos = pos + 1
		
		if len(toSort) > 0:
			self.convertFinalResultsToList()
			toSort.sort()
			seqNum = 1
			for (term, pos) in toSort:
				self.finalResults[pos][snCol] = seqNum
				seqNum = seqNum + 1

			logger.debug("Sorted %d terms" % len(toSort))
		return
	
###--- globals ---###

cmds = [
	'''with selected_vocabs as (select _Vocab_key
			from voc_vocab
			where _Vocab_key >= %%d and _Vocab_key < %%d
	), primary_ids as (select a._Object_key as _Term_key, a.accID
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
	), roots as (select t._Term_key, 1 as is_root
		from voc_term t, selected_vocabs v
		where t._Vocab_key = v._Vocab_key
			and not exists (select 1
				from dag_node dn, dag_edge de
				where t._Term_Key = dn._Object_key
					and dn._Node_key = de._Child_key)
			and exists (select 1
				from dag_node dn, dag_edge de
				where t._Term_key = dn._Object_key
					and dn._Node_key = de._Parent_key)
	), go_dags as (select distinct t._Term_key, 
			case
				when t.isObsolete = 1 then 'O'
				else d.abbreviation
			end as abbreviation
		from voc_term t, dag_node n, dag_dag d
		where t._Vocab_key = 4
			and t._Term_key = n._Object_key
			and n._DAG_key = d._DAG_key
	)
	select v.name as vocab, t._Term_key, t.term,
		case when h._Term_key is null then t.abbreviation
			when h.label is not null then h.label
			when t.abbreviation is not null then t.abbreviation
			else t.term
		end as abbreviation,
		t.note as definition, i.accID, t.sequenceNum, t.isObsolete,
		case
			when g.abbreviation = 'C' then 'Component'
			when g.abbreviation = 'F' then 'Function'
			when g.abbreviation = 'P' then 'Process'
			when g.abbreviation = 'O' then 'Obsolete'
			else v.name
		end as display_vocab,
		case
			when v.isSimple = 1 then 0
			when e.is_leaf = 1 then 1
			else 0
		end as is_leaf,
		case
			when v.isSimple = 1 then 0
			when r.is_root = 1 then 1
			else 0
		end as is_root
	from voc_term t
	inner join selected_vocabs vv on (t._Vocab_key = vv._Vocab_key)
	inner join voc_vocab v on (t._Vocab_key = v._Vocab_key)
	left outer join primary_ids i on (t._Term_key = i._Term_key)
	left outer join leaves e on (t._Term_Key = e._Term_key)
	left outer join roots r on (t._Term_key = r._Term_key)
	left outer join go_dags g on (t._Term_key = g._Term_key)
	left outer join %s h on (t._Term_key = h._Term_key)
	order by v.name, t.sequenceNum, t.isObsolete, t.term''' % VocabUtils.getHeaderTermTempTable(),
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Term_key', 'term', 'accID', 'vocab', 'display_vocab', 'abbreviation', 'definition',
	'sequenceNum', 'is_root', 'is_leaf', 'isObsolete' ]

# prefix for the filename of the output file
filenamePrefix = 'term'

# global instance of a TermGatherer
gatherer = TermGatherer (filenamePrefix, fieldOrder, cmds)
gatherer.setChunkSize(4)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
