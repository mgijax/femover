#!/usr/local/bin/python

import Table

# contains data definition information for the marker_disease table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_disease'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		not null,
	marker_key	int		not null,
	disease		varchar(255)	not null,
	disease_id	varchar(30)	null,
	logical_db	varchar(80)	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains diseases associated with mouse and human markers',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies each record, no other significance',
		'marker_key' : 'identifies the marker with which the disease is associated',
		'disease' : 'name of the disease that is associated with the marker',
		'disease_id' : 'accession ID for the disease',
		'logical_db' : 'logical database (provider) for the ID',
		'sequence_num' : 'used to order the diseases for each marker',
		},
	Table.INDEX : {
		'marker_key' : 'used to cluster data so all diseases for a marker are stored together for quick access',
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
