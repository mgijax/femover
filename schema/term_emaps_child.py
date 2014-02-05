#!/usr/local/bin/python

import Table

# contains data definition information for the term_emaps_child table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_emaps_child'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	emapa_term_key		int	not null,
	emaps_child_term_key	int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'emapsTerm' : 'create index %s on %s (emaps_child_term_key)',
	}

# column name -> (related table, column in related table)
keys = {
	'emapa_term_key'	: ('term', 'term_key'),
	'emaps_child_term_key'	: ('term', 'term_key'),
	}

# index used to cluster data in the table
clusteredIndex = ('emapaTerm', 'create index %s on %s (emapa_term_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'maps from an EMAPA term to all EMAPS terms generated from it',
	Table.COLUMN : {
		'unique_key' : 'uniquely identifies each record (no larger purpose)',
		'emapa_term_key' : 'identifies the EMAPA term from which the EMAPS terms were generated',
		'emaps_child_term_key' : 'identifies the EMAPS terms generated from the EMAPA term',
		},
	Table.INDEX : {
		'emapsTerm' : 'quick access to look up the EMAPA term used to generate the EMAPS term',
		'emapaTerm' : 'quick access to find all EMAPS terms generated from a given EMAPA term, clustered for quick access',
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
