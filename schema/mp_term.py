#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'mp_term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	mp_term_key		int		not null,
	mp_system_key		int		not null,
	term		varchar(255)	null,
	term_seq			int	null,
	indentation_depth			int	null,
	term_id			varchar(40)	null,
	PRIMARY KEY(mp_term_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'system_key' : 'create index %s on %s (mp_system_key)',
}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'represents the data needed to render phenotype information on the allele and genotype detail pages ',
	Table.COLUMN : {
		'mp_term_key' : 'unique key identifying this annotation (does not correspond to _Annot_key in mgd)',
		'mp_system_key' : 'key for the genotype',
		'term_seq' : 'sort order for this term relative to the system',
		'indentation_depth' : 'indentation depth in the tree view',
		'term' : 'text of the term itself',
		'term_id' : 'primary accession ID for the term',
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
