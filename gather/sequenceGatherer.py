#!/usr/local/bin/python
# 
# gathers data for the 'sequence' table in the front-end database

import Gatherer

###--- Classes ---###

class SequenceGatherer (Gatherer.Gatherer):
	# Is: a data gatherer for the sequence table
	# Has: queries to execute against Sybase
	# Does: queries Sybase for primary data for sequences,
	#	collates results, writes tab-delimited text file

	def getKeyClause (self):
		# Purpose: we override this method to provide information
		#	about how to retrieve data for a single sequence,
		#	rather than for all sequences

		if self.keyField == 'sequenceKey':
			return 's._Sequence_key = %s' % self.keyValue
		return ''

###--- globals ---###

cmds = [
	'''select s._Sequence_key,
		t.term as sequenceType,
		q.term as quality,
		st.term as status,
		p.term as provider,
		o.commonName as organism,
		s.length,
		s.description,
		s.version,
		s.division,
		s.virtual as isVirtual,
		s.sequence_date,
		s.seqrecord_date,
		a.accID as primaryID,
		ldb.name as logicalDB,
		pst.strain,
		tis.term as tissue,
		gen.term as sex,
		ssr.rawLibrary,
		c.cellLine
	from SEQ_Sequence s,
		VOC_Term t,
		VOC_Term q,
		VOC_Term st,
		VOC_Term p,
		MGI_Organism o,
		ACC_Accession a,
		ACC_LogicalDB ldb,
		SEQ_Source_Assoc ssa,
		PRB_Source ps,
		PRB_Strain pst,
		VOC_Term tis,
		VOC_Term gen,
		SEQ_Sequence_Raw ssr,
		ALL_CellLine c
	where s._SequenceType_key = t._Term_key
		and s._SequenceQuality_key = t._Term_key
		and s._SequenceStatus_key = st._Term_key
		and s._SequenceProvider_key = p._Term_key
		and s._Organism_key = o._Organism_key
		and s._Sequence_key = ssa._Sequence_key
		and ssa._Source_key = ps._Source_key
		and ps._Strain_key = pst._Strain_key
		and ps._Tissue_key = tis._Term_key
		and ps._Gender_key = gen._Term_key
		and s._Sequence_key = a._Object_key
		and a._MGIType_key = 19
		and a.preferred = 1
		and a._LogicalDB_key = ldb._LogicalDB_key
		and s._Sequence_key = ssr._Sequence_key
		and ps._CellLine_key = c._CellLine_key %s''',
	]

# order of fields (from the Sybase query results) to be written to the
# output file
fieldOrder = [
	'_Sequence_key', 'sequenceType', 'quality', 'status', 'provider',
	'organism', 'length', 'description', 'version', 'division',
	'isVirtual', 'sequence_date', 'seqrecord_date', 'primaryID',
	'logicalDB', 'strain', 'tissue', 'age', 'sex', 'rawLibrary',
	'cellLine',
	]

# prefix for the filename of the output file
filenamePrefix = 'sequence'

# global instance of a sequenceGatherer
gatherer = SequenceGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
