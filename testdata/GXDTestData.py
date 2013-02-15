#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *

# The list of queries to generate GXD test data
Queries = [
###--- Assay type tests
{	ID:"RNaseProtectionResultCount",
	DESCRIPTION:"Assay Result count for RNaseProtection assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='RNase protection'
	"""
},
{	ID:"WesternBlotResultCount",
	DESCRIPTION:"Assay Result count for Western Blot assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Western blot'
	"""
},
{	ID:"NorthernBlotResultCount",
	DESCRIPTION:"Assay Result count for Northern Blot assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Northern blot'
	"""
},
{	ID:"NucleaseS1ResultCount",
	DESCRIPTION:"Assay Result count for Nuclease S1 assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Nuclease S1'
	"""
},
{	ID:"RT-PCRResultCount",
	DESCRIPTION:"Assay Result count for RT-PCR assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='RT-PCR'
	"""
},
{	ID:"InSituKnockInResultCount",
	DESCRIPTION:"Assay Result count for In situ knockin assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='In situ reporter (knock in)'
	"""
},
{	ID:"ImmunohistochemistryResultCount",
	DESCRIPTION:"Assay Result count for Immunohistochemistry assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='Immunohistochemistry'
	"""
},
{	ID:"RNAInSituResultCount",
	DESCRIPTION:"Assay Result count for RNA in situ Blot assay type",
	SQLSTATEMENT:"""
        select count(*) from gxd_expression ge, gxd_assaytype gt where ge._assaytype_key=gt._assaytype_key and ge.isforgxd=1 and gt.assaytype='RNA in situ'
	"""
},

###--- Assay type tests
]

