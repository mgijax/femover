#!/usr/local/bin/python

#
# lec   08/06/2013
#       - TR11423/human disease portal/scrum-dog
#               added location/coordinate display
#

import Table

###--- Globals ---###

# name of this database table
tableName = 'marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	marker_key    	int	NOT NULL,
	symbol        	text NULL,
	name          	text NULL,
	marker_type	text NULL,
	marker_subtype	text NULL,
	organism      	text NULL,
	primary_id     	text NULL,
	logical_db	text NULL,
	status       	text NULL,
	has_go_graph	int	NOT NULL,
	location_display 	text	NULL,
	coordinate_display 	text 	NULL,
	build_identifier	text    NULL,
	strain				text	NULL,
	strain_id			text	NULL,
	PRIMARY KEY(marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'symbol' : 'create index %s on %s (symbol)',
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the marker flower, containing basic information about each marker',
	Table.COLUMN : {
		'marker_key' : 'unique key identifying the marker, same as _Marker_key in mgd',
		'symbol' : 'marker symbol',
		'name' : 'descriptive name',
		'marker_type' : 'type of marker',
		'marker_subtype' : 'subtype of the marker',
		'organism' : 'type of organism containing the marker',
		'primary_id' : 'primary accession ID',
		'logical_db' : 'logical database assigning the ID',
		'status' : 'status for this marker',
		'has_go_graph' : '1 if this marker has a graph of its GO associations available, 0 if not',
		'location_display' : 'genetic location (cM, cytogentic offset) display',
		'coordinate_display' : 'genomic coordinate display',
		'build_identifier' : 'names the genome build to which the coordinates are relative',
		'strain' : 'name of mouse strain, if this marker has a coordiante location (will always be null of C57BL/6J)',
		'strain_id' : 'primary ID for the mouse strain (will always be null or ID of C57BL/6J)',
		},
	Table.INDEX : {
		'symbol' : 'look up markers by symbol',
		'primary_id' : 'look up markers by primary ID',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
