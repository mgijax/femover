#!/usr/local/bin/python

import Table

# contains data definition information for the mapping_link table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mapping_link'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	experiment_key	int		not null,
	link_type		text	not null,
	url				text	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
	'experiment_key' : ('mapping_experiment', 'experiment_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('experiment_key', 'create index %s on %s (experiment_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'stores special links for certain mapping experiments',
	Table.COLUMN : {
		'unique_key' : 'generated primary key for the table; no other significance',
		'experiment_key' : 'foreign key to mapping_experiment table, identifying experiment with the link',
		'link_type' : 'identifies the type of link',
		'url' : 'URL to use for the link',
		},
	Table.INDEX : {
		'experiment_key' : 'clustered index for bringing all links for an experiment together for quick access',
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
