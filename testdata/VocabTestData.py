#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *

# The list of queries to generate GXD Vocab query test data
# these will be pretty similar queries, so we use a template
GXD_VOCAB_GOID_TEMPLATE_SQL="""
    select distinct m.symbol
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
GXD_VOCAB_TERM_CHILDREN_TEMPLATE_SQL="""
    select distinct m.symbol
    from VOC_Term parent,
      MRK_Marker m,
      DAG_Closure dc,
      VOC_Annot va,
      VOC_Term qualifier,
      VOC_AnnotType t
    where parent.term = '%s'
      and va._Object_key = m._Marker_key
      and parent._Term_key = dc._AncestorObject_key
      and (dc._DescendentObject_key = va._Term_key
	or parent._Term_key = va._Term_key)
      and va._Qualifier_key = qualifier._Term_key
      and (qualifier.term is null or qualifier.term not like 'NOT%%')
      and va._AnnotType_key = t._AnnotType_key
      and t.name = 'GO/Marker'
      and exists (select 1 from GXD_Index ge
	where ge._Marker_key = va._Object_key)
    order by 1
"""
GXD_VOCAB_MPID_TEMPLATE_SQL="""
    select distinct v._marker_key, m.symbol
    from mgd.acc_accession a , mgd.voc_marker_cache v, mgd.mrk_marker m, mgd.voc_term t, gxd_expression e
    where a.accid = '%s'
	and a._object_key = v._term_key
	and v._marker_key = m._marker_key
	and v._term_key = t._term_key
	and v.annotType = 'Mammalian Phenotype/Genotype'
	and v._marker_key = e._marker_key
	and e._assaytype_key != 11
	and e.isforgxd = 1
    order by m.symbol
"""
Queries = [
{	ID:"microfibrilGXDGenes",
	DESCRIPTION:"All marker symbols associated with microfibril + GXD",
	SQLSTATEMENT:GXD_VOCAB_GOID_TEMPLATE_SQL%"GO:0001527"
},
{	ID:"perisheralNSAxonGXDGenes",
	DESCRIPTION:"All marker symbols associated with peripheral nervous system axon ensheathment + GXD",
	SQLSTATEMENT:GXD_VOCAB_TERM_CHILDREN_TEMPLATE_SQL%"peripheral nervous system axon ensheathment"
},
{	ID:"retroesophagealArteryGXDGenes",
	DESCRIPTION:"All marker symbols associated with 'retroesophageal right subclavian artery' + GXD",
	SQLSTATEMENT:GXD_VOCAB_MPID_TEMPLATE_SQL%"MP:0004160"
},
# copy above lines to make more tests
]

