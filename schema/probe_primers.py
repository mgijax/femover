#!/usr/local/bin/python

import Table

# contains data definition information for the probe_primers table

###--- Globals ---###

# name of this database table
tableName = 'probe_primers'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	probe_key			int 	NOT NULL,
	primer_sequence_1	text	NULL,
	primer_sequence_2	text 	NULL,
	PRIMARY KEY(probe_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'probe_key' : ('probe', 'probe_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the probe flower, containing primer sequences for various probes',
	Table.COLUMN : {
		'probe_key' : 'identifies the probe',
		'primer_sequence_1' : 'sequence for first primer',
		'primer_sequence_2' : 'sequence for second primer',
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
