#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_provider_map table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'sequence_provider_map'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	logical_db		text	not null,
	provider_abbreviation	text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'logical_db' : 'create index %s on %s (logical_db)',
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'adjoins portions of the sequence flower, to supply a provider abbreviation for each logical_db assigning IDs to sequences',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record',
		'logical_db' : 'logical database that provides IDs for sequences',
		'provider_abbreviation' : 'abbreviation for the provider',
		},
	Table.INDEX : {
		'logical_db' : 'quick lookup of provider_abbreviation by logical_db',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
