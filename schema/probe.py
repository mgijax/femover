#!/usr/local/bin/python

import Table

###--- Globals ---###

# name of this database table
tableName = 'probe'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	probe_key    	int	NOT NULL,
	name          	varchar(40) NULL,
	segment_type	varchar(255) NULL,
	primary_id     	varchar(30) NULL,
	logical_db	varchar(80) NULL,
	clone_id	varchar(30) NULL,
	PRIMARY KEY(probe_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'clone_id' : 'create index %s on %s (clone_id)',
	'id' : 'create index %s on %s (primary_id)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
