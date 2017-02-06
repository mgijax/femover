#!/usr/local/bin/python

import Table

# contains data definition information for the disease_group_row table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_group_row'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	disease_group_row_key	int		not null,
	disease_group_key	int		not null,
	disease_row_key		int		not null,
	disease_key		int		not null,
	PRIMARY KEY(disease_group_row_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'disease_group_key' : ('disease_group', 'disease_group_key'),
         'disease_row_key' : ('disease_row', 'disease_row_key'),
         'disease_key' : ('disease', 'disease_key') }

# index used to cluster data in the table
clusteredIndex = ('disease_group_key', 'create index %s on %s (disease_group_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'defines several groupings of rows for each disease (these are sections of related rows)',
	Table.COLUMN : {
		'disease_group_row_key' : 'uniquely identifies this disease/group/row trio',
		'disease_group_key' : 'uniquely identifies this disease/group pair',
		'disease_row_key' : 'uniquely identifies this disease row',
		'disease_key' : 'foreign key to disease table to identify the disease in question',
		},
	Table.INDEX : {
		'disease_group_key' : 'clustered index, keeps the groups for a given disease together on disk for fast access',
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
