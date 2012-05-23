#!/usr/local/bin/python

import Table

# contains data definition information for the anatomy_structures_synonyms
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'anatomy_structures_synonyms'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	structure		varchar(80)	null,
	synonym			varchar(80)	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'structure' : 'create index %s on %s (structure)',
	'synonym' : 'create index %s on %s (synonym)',
	}

keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'standalone table for populating a pick-list on the expression query form, mapping from an anatomical structure to its synonyms',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record',
		'structure' : 'an anatomical structure',
		'synonym' : 'one of the synonyms for that structure',
		},
	Table.INDEX : {
		'structure' : 'look up synonyms by structure',
		'synonym' : 'look up structures matching a synonym',
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
