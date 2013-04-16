#!/usr/local/bin/python

import Table

# contains data definition information for the disease table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	disease_key	int		not null,
	disease		varchar(255)	not null,
	primary_id	varchar(40)	not null,
	logical_db	varchar(255)	not null,
	PRIMARY KEY(disease_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'disease' : 'create index %s on %s (disease)',
	'primaryID' : 'create index %s on %s (primary_id)',
	}

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains basic data for human diseases',
	Table.COLUMN : {
		'disease_key' : 'unique database key for the disease, same as the VOC_Term._Term_key used in mgd',
		'disease' : 'name of the disease, same as VOC_Term.term in mgd',
		'primary_id' : 'primary ID for the disease',
		'logical_db' : 'logical database assigning the ID',
		},
	Table.INDEX : {
		'disease_key' : 'primary key',
		'disease' : 'case-sensitive searching by disease term',
		'primaryID' : 'lookup by disease ID',
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
