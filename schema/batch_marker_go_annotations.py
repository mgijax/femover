#!/usr/local/bin/python

import Table

# contains data definition information for the batch_marker_go_annotations
# table

###--- Globals ---###

# name of this database table
tableName = 'batch_marker_go_annotations'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	marker_key	int		not null,
	go_id		varchar(30)	null,
	go_term		varchar(255)	null,
	evidence_code	varchar(255)	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key') } 

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the marker flower, caching minimal data for GO annotations for markers (only the data needed to support batch query, to aid efficiency)',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this GO annotation for this marker',
		'marker_key' : 'identifies the marker',
		'go_id' : 'primary ID for the GO term (cached from annotation table)',
		'go_term' : 'text of the GO term (cached from the annotation table)',
		'evidence_code' : 'evidence code for the annotation (cached from the annotation table)',
		'sequence_num' : 'used for ordering GO annotations for a marker',
		},
	Table.INDEX : {
		'marker_key' : 'clusters data so GO annotations for a marker will be stored together on disk, for efficiency',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
