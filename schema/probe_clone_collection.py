#!/usr/local/bin/python

import Table

# contains data definition information for the probe_clone_collection table

###--- Globals ---###

# name of this database table
tableName = 'probe_clone_collection'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	probe_key	int		NOT NULL,
	collection	varchar(255)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'probe_key' : 'create index %s on %s (probe_key)',
	}

keys = { 'probe_key' : ('probe', 'probe_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
