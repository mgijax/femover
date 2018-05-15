#!/usr/local/bin/python

import Table

# contains data definition information for the strain_imsr_data table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_imsr_data'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	NOT NULL,
	strain_key		int	NOT NULL,
	imsr_id			text NULL,
	repository		text NULL,
	source_url		text NULL,
	match_type		text NOT NULL,
	imsr_strain		text NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'imsr_id' : 'create index %s on %s (imsr_id)',
	'repository' : 'create index %s on %s (repository)',
	}

# column name -> (related table, column in related table)
keys = {
	'strain_key' : ('strain', 'strain_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('strain_key', 'create index %s on %s (strain_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the strain flower, containing data related to the strain at IMSR',
	Table.COLUMN : {
		'unique_key' : 'unique key for this strain/ID pair',
		'strain_key' : 'identifies the strain',
		'imsr_id' : 'accession ID used by IMSR for the strain',
		'repository' : 'data provider that submitted the data to IMSR',
		'source_url' : 'URL for this strain at the given repository',
		'match_type' : 'how did the imsr_strain match MGI data?',
		'imsr_strain' : 'what does IMSR call this strain?',
		},
	Table.INDEX : {
		'strain_key' : 'clusters data so all IDs for a strain are stored near each other on disk, to aid quick retrieval',
		'imsr_id' : 'look up a strain by its IMSR ID',
		'repository' : 'look up the strains for a repository',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
