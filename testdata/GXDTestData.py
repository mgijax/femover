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

###--- Theiler Stage Tests
{	ID:"ts6ResultCount",
	DESCRIPTION:"Assay Result count for Theiler Stage 6",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1 and gs._stage_key=6
	"""
},
{	ID:"allResults",
	DESCRIPTION:"count of all gxd results",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1
	"""
},
{	ID:"ts6or7ResultCount",
	DESCRIPTION:"Assay Result count for TS 6 or 7",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1 and gs._stage_key in (6,7)
	"""
},
{	ID:"ts28ResultCount",
	DESCRIPTION:"Assay Result count for TS 28",
	SQLSTATEMENT:"""
	select count(*) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1 and gs._stage_key=28
	"""
},
{	ID:"allGenes",
	DESCRIPTION:"Count of all GXD markers",
	SQLSTATEMENT:"""
	select count(distinct _marker_key) from gxd_expression ge, gxd_Structure gs where ge._structure_key=gs._structure_key and ge.isforgxd=1
	"""
},

###--- Age Tests
{	ID:"age2-3ResultCount",
	DESCRIPTION:"Assay Result count for ages 2.0,2.5,3.0",
	SQLSTATEMENT:"""
	select count(*) from gxd_Expression where isforgxd=1 and (agemin <3.25 and agemax >=1.75)
	"""
},
{	ID:"age5and6and7ResultCount",
	DESCRIPTION:"Assay Result count for ages 5.0,6.0,7.0",
	SQLSTATEMENT:"""
	select count(*) from gxd_Expression where isforgxd=1 and ((agemin <5.25 and agemax >=4.75) or (agemin <6.25 and agemax >=5.75) or (agemin <7.25 and agemax >=6.75))
	"""
},
{	ID:"age1and4ResultCount",
	DESCRIPTION:"Result count for ages 1.0 and 4.0",
	SQLSTATEMENT:"""
	select count(*) from gxd_Expression where isforgxd=1 and ((agemin <1.25 and agemax >=0.75) or (agemin <4.25 and agemax >=3.75))
	"""
},
]

