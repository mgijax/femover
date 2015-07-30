#!/usr/local/bin/python

import Table

# contains data definition information for the allele_disease table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_disease'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	allele_disease_key	int		not null,
	allele_key		int		not null,
	is_heading		int		not null,
	is_not			int		not null,
	term			text	null,
	term_id			text	null,
	has_footnote		int		not null,
	sequence_num		int		not null,
	by_alpha		int		not null,
	PRIMARY KEY(allele_disease_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing data about diseases for which this allele is a mouse model',
	Table.COLUMN : {
		'allele_disease_key' : 'unique identifier for this row, no other significance',
		'allele_key' : 'identifies the allele',
		'is_heading' : '1 if this term is really a table header row, 0 if it is not (in which case it is a disease)',
		'is_not' : '1 if this is a NOT annotation (so the disease was expected, but not found), 0 if it is a positive annotation',
		'term' : 'name of the disease (or text of the header term if is_heading = 1)',
		'term_id' : 'accession ID for the disease term',
		'has_footnote' : '1 if this row has a footnote, 0 if not',
		'sequence_num' : 'used to order rows for each allele',
		'by_alpha' : 'used to order rows for each disease',
		},
	Table.INDEX : {
		'allele_key' : 'clusters data so all records for an allele are together on disk, to aid retrieval speed',
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
