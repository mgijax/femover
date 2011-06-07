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
	acc_id		varchar(30)	null,
	logical_db	varchar(80)	null,
	biotype		varchar(255)	null,
	sequence_num	int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'marker_key' : 'create index %s on %s (marker_key)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
