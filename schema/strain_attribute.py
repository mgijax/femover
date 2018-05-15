#!/usr/local/bin/python

import Table

# contains data definition information for the strain_attribute table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_attribute'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	strain_key		int		not null,
	attribute		text	null,
	sequence_num	int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'strain_key' : ('strain', 'strain_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table of strain flower, containing strain types',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this table row, no other purpose',
		'strain_key' : 'identifies the strain with these attributes (foreign key)',
		'attribute' : 'text value specifying one attribute of the strain',
		'sequence_num' : 'pre-computed integer sort value for this attribute for this strain',
		},
	Table.INDEX : {
		'strain_key' : 'clusters data so all attributes of a strain are together on disk',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
