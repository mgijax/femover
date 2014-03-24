#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *
import MarkerTestData

# The list of queries to generate GXD test data
Queries = [
{	ID:"allMarkers",
	DESCRIPTION:"Count of all mouse markers",
	SQLSTATEMENT:"""
	select count(*) from mrk_marker where _organism_key in (1) and _marker_status_key in (1,3);
	"""
},
# copy above lines to make more tests
]

