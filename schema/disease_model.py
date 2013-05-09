#!/usr/local/bin/python

import Table

# contains data definition information for the disease_model table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'disease_model'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	disease_model_key	int		not null,
	genotype_key		int		not null,
	disease			varchar(255)	not null,
	disease_id		varchar(40)	not null,
	is_not_model		int		not null,
	PRIMARY KEY(disease_model_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'disease_id' : 'create index %s on %s (disease_id)',
	'genotype_key' : 'create index %s on %s (genotype_key)',
	}

# column name -> (related table, column in related table)
keys = { 'genotype_key' : ('genotype', 'genotype_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'defines a mouse model for a human disease',
	Table.COLUMN : {
		'disease_model_key' : 'unique key that identifies a mouse model for a human disease',
		'genotype_key' : 'foreign key to ',
		'disease' : 'name of the disease, cached here for convenience',
		'disease_id' : 'primary ID for the disease, cached here for convenience',
		'is_not_model' : 'flag to indicate if this genotype was expected to be a model for the human disease but was not (1), or if it actually does model the disease (0)',
		},
	Table.INDEX : {
		'disease_id' : 'provides quick access to models for a disease by its ID, for convenience in SQL access',
		'genotype_key' : 'provides access to diseases for which a given genotype is a mouse model',
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
