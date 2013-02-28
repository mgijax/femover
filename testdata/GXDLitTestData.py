#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *

# The list of queries to generate GXD Lit test data
Queries = [
###--- Miscellaneous GXD Lit tests
{	ID:"gxdlitPax1Count",
	DESCRIPTION:"Count of gxdlit records for Pax1",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index i, mrk_marker m where m._marker_key=i._marker_key and m.symbol='Pax1';
	"""
},
{	ID:"gxdlitTS9Count",
	DESCRIPTION:"Count of gxdlit records for TS9",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index_stages ins where ins._stageid_key in (74741,74742,74743);
	"""
},
{	ID:"gxdlitAge2Count",
	DESCRIPTION:"Count of gxdlit records for Age 2",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index_stages ins where ins._stageid_key =74732;
	"""
},
{	ID:"gxdlitImmunohistochemistryCount",
	DESCRIPTION:"Count of gxdlit records for Immunohistochemistry",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index_stages ins where ins._indexassay_key in (74717,74719);
	"""
},
{	ID:"gxdlitPostnatalCount",
	DESCRIPTION:"Count of gxdlit records for Postnatal",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index_stages ins 
		where ins._stageid_key =74770;
	"""
},
{	ID:"gxdlitEmbryonicCount",
	DESCRIPTION:"Count of gxdlit records for Embryonic",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index_stages ins where ins._stageid_key !=74770;
	"""
},
{	ID:"gxdlitRNAInSituCount",
	DESCRIPTION:"Count of gxdlit records for RNA in situ",
	SQLSTATEMENT:"""
	select count(distinct _index_key) from gxd_index_stages ins 
		where ins._indexassay_key in (74718,74720);
	"""
},
{	ID:"gxdlitCombinedCount",
	DESCRIPTION:"Count of gxdlit records for a combination of parameters (marker, assay type, stage)",
	SQLSTATEMENT:"""
	select count(distinct gi._index_key) from gxd_index gi, gxd_index_stages ins, mrk_marker m 
		where gi._index_key=ins._index_key and m._marker_key=gi._marker_key 
			and m.symbol='Pax2' 
			and ins._indexassay_key=74722 
			and ins._stageid_key=74770;
	"""
},
]

