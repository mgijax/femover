#!/usr/local/bin/python
# 
# gathers data for the 'hdp_annotation' table in the front-end database
#
# 07/19/2013	lec
#	- TR11423/Human Disease Portal
#

import Gatherer

###--- Classes ---###

HDPAnnotationGatherer = Gatherer.Gatherer
	# Is: a data gatherer for the actual_database table
	# Has: queries to execute against the source database
	# Does: queries the source database for primary data for actual dbs,
	#	collates results, writes tab-delimited text file

###--- globals ---###

cmds = [

	# mouse/genotypes annotated to OMIM terms (1005)
	# human/genes annotated to OMIM terms (1006)
	# make human/genotype key = null
	'''
	select distinct gg._Marker_key, v._Term_key, v._AnnotType_key, v._Object_key, t.term, a.accID, vv.name
	from VOC_Annot v , VOC_Term t, VOC_Vocab vv, GXD_AlleleGenotype gg, ACC_Accession a
	where v._AnnotType_key in (1005)
	and v._Term_key = t._Term_key
	and v._Object_key = gg._Genotype_key
	and v._Term_key = a._Object_key
	and a._MGIType_key = 13
	and a.private = 0
	and a.preferred = 1
	and t._Vocab_key = vv._Vocab_key
	union
	select distinct v._Object_key, v._Term_key, v._AnnotType_key, null::integer, t.term, a.accID, vv.name
	from VOC_Annot v , VOC_Term t, VOC_Vocab vv, ACC_Accession a
	where v._AnnotType_key in (1006)
	and v._Term_key = t._Term_key
	and v._Term_key = a._Object_key
	and a._MGIType_key = 13
	and a.private = 0
	and a.preferred = 1
	and t._Vocab_key = vv._Vocab_key
	'''
	]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_Marker_key', '_Term_key', '_AnnotType_key', '_Object_key', 'term', 'accID', 'name' ]

# prefix for the filename of the output file
filenamePrefix = 'hdp_annotation'

# global instance of a HDPAnnotationGatherer
gatherer = HDPAnnotationGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
	Gatherer.main (gatherer)
