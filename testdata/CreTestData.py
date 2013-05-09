#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *


TEMPLATE_CRE_STRUCTURE_SQL = """
WITH 
        struct AS (SELECT DISTINCT _Structure_key FROM gxd_structurename 
                WHERE structure @@ array_to_string(string_to_array('%s', ' '), ' & ')),
        syn AS (SELECT DISTINCT clo._Descendent_key FROM gxd_structureclosure clo 
                WHERE EXISTS (SELECT 1 FROM struct s WHERE clo._Structure_key = s._Structure_key) 
                AND NOT EXISTS (SELECT 1 FROM struct s WHERE clo._Descendent_key = s._Structure_key)),
        closure AS (SELECT * from struct UNION ALL SELECT * FROM syn) 
        SELECT count(distinct cre._allele_key) FROM gxd_expression e,gxd_allelegenotype gag,all_cre_cache cre, closure s WHERE e._Structure_key = s._Structure_key AND e.isrecombinase = 1 AND gag._genotype_key=e._genotype_key AND gag._allele_key=cre._allele_key;
"""
# The list of queries to generate GXD test data
Queries = [
{	ID:"creDorsalMesocardiumAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with dorsal mesocardium",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"dorsal mesocardium"
},
{	ID:"creHeartAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with heart",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"heart"
},
{	ID:"creEsophagusAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with esophagus",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"esophagus"
},
{	ID:"creBronchusEpitheliumAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with bronchus epithelium",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"bronchus epithelium"
},
{	ID:"creBronchialEpitheliumAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with bronchial epithelium",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"bronchial epithelium"
},
{	ID:"creUterusAlleleCount",
	DESCRIPTION:"Count of cre alleles associated with uterus",
	SQLSTATEMENT:TEMPLATE_CRE_STRUCTURE_SQL%"uterus"
},
# copy above lines to make more tests
]

