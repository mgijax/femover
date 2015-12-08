#!/usr/local/bin/python

import Table

# contains data definition information for the driver table

###--- Globals ---###

# name of this database table
tableName = 'driver'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	driver			text	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {}

# index used to cluster data in the table
clusteredIndex = ('driver', 'create index %s on %s (driver)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'table containing recombinase drivers ', 
	Table.COLUMN : {
		'unique_key' : 'unique key for this record, no other purpose',
		'driver' : 'driver text'
		},
	Table.INDEX : {},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
