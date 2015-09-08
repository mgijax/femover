#!/usr/local/bin/python

import Table

# contains data definition information for the term_to_header table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_to_header'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	not null,
	term_key	int	not null,
	header_term_key	int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	'header_term_key' : 'create index %s on %s (header_term_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'header_term_key' : ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'maps from a term key used in an annotation to the keys of the header terms which are ancestors of that term (can be more than one row per term, if multiple headers are reachable)',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies one row in this table',
		'term_key' : 'key of the annotated term',
		'header_term_key' : 'key of one of the slim terms (ancestor of the annotated term)',
		},
	Table.INDEX : {
		'term_key' : 'quick access to headers for a term',
		'header_term_key' : 'look up all terms under a given header term',
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
