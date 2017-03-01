#!/usr/local/bin/python

import Table

# contains data definition information for the marker_polymorphism_allele table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_polymorphism_allele'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key			int		not null,
	polymorphism_key	int		not null,
	allele				text	null,	
	fragments			text	null,
	strains				text	null,
	sequence_num		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = { 'polymorphism_key' : ('marker_polymorphism', 'polymorphism_key'), }

# index used to cluster data in the table
clusteredIndex = ('polymorphism_key', 'create index %s on %s (polymorphism_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : '''Each row provides detailed data for one allele for a single polymorphism. There can be multiple rows in this table that correspond to a single polymorphism in marker_polymorphism.''',
	Table.COLUMN : {
		'unique_key' : 'generated primary key for this table; no other significance',
		'polymorphism_key' : 'foreign key to marker_polymorphism table; identifies the polymorphism for this allele',
		'allele' : 'arbitrary symbol assigned to the RFLV pair/fragment size; no correlation to phenotypic allele symbols',
		'fragments' : 'fragment length (comma-delimited)',
		'strains' : 'strains with this allele (comma-delimited)',
		'sequence_num' : 'pre-computed sequence number for ordering alleles of a polymorphism',
		},
	Table.INDEX : {
		'polymorphism_key' : 'clustered index to bring all alleles of a polymorphism together, providing quick retrieval',
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
