#!/usr/local/bin/python

import Table

# contains data definition information for the strain table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	strain_key		int		not null,
	name			text	not null,
	primary_id		text	null,
	PRIMARY KEY(strain_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'primary_id' : 'create index %s on %s (primary_id)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for set of strains tables',
	Table.COLUMN : {
		'strain_key' : 'unique key for this mouse strain (matches _Strain_key in prod database)',
		'name' : 'strain nomenclature',
		'primary_id' : 'primary MGI ID of the mouse strain',
		},
	Table.INDEX : {
		'primary_id' : 'quick access by primary strain ID',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
