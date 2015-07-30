#!/usr/local/bin/python

import Table

# contains MP term information for the pheno summary table 

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'phenotable_term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	phenotable_term_key		int		not null,
	phenotable_system_key		int		not null,
	term		text	null,
	term_id		text	null,
	indentation_depth			int	null,
	term_seq			int	null,
	term_key		int		not null,
	PRIMARY KEY(phenotable_term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'system_key' : 'create index %s on %s (phenotable_system_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render phenotype information on the allele and genotype detail pages ',
	Table.COLUMN : {
		'phenotable_term_key' : 'unique key identifying this row',
		'phenotable_system_key' : 'key for the corresponding system',
		'term' : 'an MP term',
		'term_id' : 'an MP term MGIID',
		'indentation_depth' : 'how far to indent this term',
		'term_seq' : 'term sort order (by DAG)',
		'term_key' : 'term key from VOC_Term',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
