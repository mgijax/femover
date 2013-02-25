#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *

# The list of queries to generate GXD Vocab query test data
# these will be pretty similar queries, so we use a template
GXD_VOCAB_GOID_TEMPLATE_SQL="""
    select distinct v._marker_key, m.symbol
    from mgd.acc_accession a , mgd.voc_marker_cache v, mgd.mrk_marker m, mgd.voc_term t, gxd_expression e
    where a.accid = '%s'
	and a._object_key = v._term_key
	and v._marker_key = m._marker_key
	and v._term_key = t._term_key
	and v.annotType = 'GO/Marker'
	and v._marker_key = e._marker_key
	and e._assaytype_key != 11
	and e.isforgxd = 1
    order by m.symbol
"""
Queries = [
{	ID:"microfibrilGXDGenes"
	DESCRIPTION:"All marker symbols associated with microfibril + GXD",
	SQLSTATEMENT:GXD_VOCAB_GOID_TEMPLATE_SQL%"GO:0001527"
},
{	ID:"perisheralNSAxonGXDGenes",
	DESCRIPTION:"All marker symbols associated with peripheral nervous system axon ensheathment + GXD",
	SQLSTATEMENT:GXD_VOCAB_GOID_TEMPLATE_SQL%"GO:0032292"
},
# copy above lines to make more tests
]

