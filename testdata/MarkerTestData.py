#!/usr/local/bin/python

# this TestData import brings in the constants used below (E.g. ID, DESCRIPTION,...)
from TestData import *
import MarkerTestData

# The list of queries to generate GXD test data
Queries = [
{	ID:"allMarkers",
	DESCRIPTION:"Count of all mouse markers",
	SQLSTATEMENT:"""
	select count(*) from mrk_marker where _organism_key in (1) and _marker_status_key = 1 ;
	"""
},
]
# Feature Type (MCV) queries
Queries.extend([
{	ID:"markerCountMcv1",
	DESCRIPTION:"Count of polymorphic pseudogene markers",
	SQLSTATEMENT:"""
	select count(distinct m._marker_key) 
	from mrk_marker m join mrk_mcv_cache mcv on mcv._marker_key=m._marker_key 
	where m._marker_status_key = 1
		and mcv._mcvterm_key=7288449 
	"""
},
{	ID:"markerCountMcv2",
	DESCRIPTION:"Count of polymorphic pseudogene + other types markers",
	SQLSTATEMENT:"""
	select count(distinct m._marker_key) 
	from mrk_marker m join mrk_mcv_cache mcv on mcv._marker_key=m._marker_key 
	where m._marker_status_key = 1
		and mcv._mcvterm_key in (7288449,6238182,6238183) 
	"""
},
# copy above lines to make more tests
])

# Chromosome queries
Queries.extend([
{	ID:"markerCountChr19",
	DESCRIPTION:"Count of chr19 mouse markers",
	SQLSTATEMENT:"""
	select count(*) 
	from mrk_marker m 
	where m._marker_status_key = 1
		and _organism_key=1 
		and m.chromosome in ('19')
	"""
},
{	ID:"markerCountChrY",
	DESCRIPTION:"Count of chrY mouse markers",
	SQLSTATEMENT:"""
	select count(*) 
	from mrk_marker m 
	where m._marker_status_key = 1
		and _organism_key=1 
		and m.chromosome in ('Y')
	"""
},
{	ID:"markerCountChr19andY",
	DESCRIPTION:"Count of chr19 and chrY mouse markers",
	SQLSTATEMENT:"""
	select count(*) 
	from mrk_marker m 
	where m._marker_status_key = 1
		and _organism_key=1 
		and m.chromosome in ('19','Y')
	"""
},
# copy above lines to make more tests
])
