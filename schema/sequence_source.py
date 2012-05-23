#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_source table

###--- Globals ---###

# name of this database table
tableName = 'sequence_source'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int		NOT NULL,
	sequence_key	int		NOT NULL,
	strain		varchar(256)	NULL,
	tissue		varchar(256)	NULL,
	age		varchar(256)	NULL,
	sex		varchar(256)	NULL,
	cell_line	varchar(256)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# index used to cluster data in the table
clusteredIndex = ('sequence_key', 'create index %s on %s (sequence_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the sequence flower, containing data about the biological source(s) of the sequence',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record',
		'sequence_key' : 'identifies the sequence',
		'strain' : 'strain or genetic background',
		'tissue' : 'anatomical tissue',
		'age' : 'age of the specimen',
		'sex' : 'gender of the specimen',
		'cell_line' : 'name of the cell line',
		},
	Table.INDEX : {
		'sequence_key' : 'clusters data by sequence, so that all sources for a sequence can be retrieved efficiently together',
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
