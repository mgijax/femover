#!/usr/local/bin/python

import Table

# contains data definition information for the marker_biotype_conflict table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_biotype_conflict'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	not null,
	marker_key	int	not null,
	acc_id		text	null,
	logical_db	text	null,
	biotype		text	null,
	sequence_num	int	not null,
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
	Table.TABLE : 'petal table in the marker flower, containing data about biotype conflicts for markers.  These are instances where the MGI marker type does not match the biotype (marker type) called for this marker by at least one other entity.',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record, no other purpose',
		'marker_key' : 'identifies the marker',
		'acc_id' : 'primary ID for this marker assigned by logical_db',
		'logical_db' : 'logical database with the different biotype',
		'biotype' : 'biotype (marker type) assigned by the logical_db for this marker',
		'sequence_num' : 'orders biotype conflict rows for each marker',
		},
	Table.INDEX : {
		'marker_key' : 'clusters data so all rows for a marker are stored together on disk, to aid quick retrieval',
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
