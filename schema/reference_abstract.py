#!/usr/local/bin/python

import Table

# contains data definition information for the reference_abstract table

###--- Globals ---###

# name of this database table
tableName = 'reference_abstract'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reference_key	int NOT NULL,
	abstract	text NULL,
	PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'reference_key' : ('reference', 'reference_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the reference flower; contains the abstract for each reference.  This is stored separately to allow fast access to other reference data, while only getting abstracts when they are needed.',
	Table.COLUMN : {
		'reference_key' : 'identifies which reference',
		'abstract' : 'a brief summary of the reference',
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
