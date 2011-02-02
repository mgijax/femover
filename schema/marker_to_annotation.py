#!/usr/local/bin/python

import Table

# contains data definition information for the marker_to_annotation table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	marker_key	int		NOT NULL,
	annotation_key	int 		NOT NULL,
	reference_key	int		NULL,
	qualifier	varchar(80)	NULL,
	annotation_type	varchar(255)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'marker_key' : 'create index %s on %s (marker_key, annotation_key)',
	'annotation_key' : 'create index %s on %s (annotation_key, marker_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
