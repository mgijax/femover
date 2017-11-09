#!/usr/local/bin/python

import Table

# contains data definition information for the term_ancestor table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_ancestor'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		not null,
	term_key		int		not null,
	ancestor_term_key	int		not null,
	ancestor_term		text	null,
	ancestor_primary_id	text	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'ancestor_term_key' : 'create index %s on %s (ancestor_term_key)',
	'ancestor_primary_id' : 'create index %s on %s (ancestor_primary_id)',
	}

# index used to cluster data in the table
clusteredIndex = ('term_key', 'create index %s on %s (term_key)')

keys = {
	'term_key' : ('term', 'term_key'),
	'ancestor_term_key' : ('term', 'term_key'),
	}

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains distinct ancestor terms for each term in the term table, without regard to which path from the root down to the term',
	Table.COLUMN : {
		'unique_key' : 'unique key for this term/ancestor pair',
		'term_key' : 'foreign key to term table',
		'ancestor_term_key' : 'foreign key to term table, for the ancestor term',
		'ancestor_term' : 'name of the ancestor term, cached here for convenience',
		'ancestor_primary_id' : 'accession ID of the ancestor term, cached here for convenience',
		},
	Table.INDEX : {
		'term_key' : 'quick lookup by term key',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
