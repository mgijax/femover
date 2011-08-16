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
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	}

keys = { 'marker_key' : ('marker', 'marker_key') } 

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
