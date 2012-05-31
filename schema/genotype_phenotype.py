#!/usr/local/bin/python

import Table

# contains data definition information for the genotype_phenotype table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'genotype_phenotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	genotype_phenotype_key	int		not null,
	genotype_key		int		not null,
	is_header_term		int		not null,
	term			varchar(255)	null,
	term_id			varchar(40)	null,
	indentation_level	int		not null,
	reference_count		int		not null,
	note_count		int		not null,
	sequence_num		int		not null,
	PRIMARY KEY(genotype_phenotype_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'genotype_key' : ('genotype', 'genotype_key') }

# index used to cluster data in the table
clusteredIndex = ('genotype_key',
	'create index %s on %s (genotype_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table in the genotype flower, containing data about phenotype (MP) annotations for each genotype',
	Table.COLUMN : {
		'genotype_phenotype_key' : 'unique identifier for this record, no other significance',
		'genotype_key' : 'identifies the genotype',
		'is_header_term' : '1 if this term is an MP header term, 0 if it is a non-header term',
		'term' : 'term annotated to this genotype',
		'term_id' : 'preferred ID for the term',
		'indentation_level' : 'how many levels to indent this term for display',
		'reference_count' : 'number of references supporting this annotation',
		'note_count' : 'count of notes for this annotation',
		'sequence_num' : 'used to order annotations for each genotype',
		},
	Table.INDEX : {
		'genotype_key' : 'clusters data so all annotations for a genotype are stored together on disk, to aid efficient retrieval',
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
