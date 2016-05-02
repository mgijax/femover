#!/usr/local/bin/python

import Table

# contains data definition information for the term_to_term table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_to_term'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	term_key_1		int	not null,
	term_key_2		int	not null,
	relationship_type	text	not null,
	evidence		text	null,
	cross_reference		text	null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key_1' : 'create index %s on %s (term_key_1)',
	'term_key_2' : 'create index %s on %s (term_key_2)',
	'relationship_type' : 'create index %s on %s (relationship_type)',
	}

# column name -> (related table, column in related table)
keys = {
	'term_key_1' : ('term', 'term_key'),
	'term_key_2' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'contains relationships between vocabulary terms',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies this record (no other significance)',
		'term_key_1' : 'the term at the left side of the association',
		'term_key_2' : 'the term at the right side of the association',
		'relationship_type' : 'describes the association, as read from left to right',
		'evidence' : 'abbreviation for the evidence term supporting this association between terms (optional)',
		'cross_reference' : 'ID from another resource that is a cross-reference supporting this association between terms (optional)',
		},
	Table.INDEX : {
		'term_key_1' : 'index on the left term',
		'term_key_2' : 'index on the right term',
		'relationship_type' : 'quick access to all associations of a certain type',
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
