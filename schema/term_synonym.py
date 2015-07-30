#!/usr/local/bin/python

import Table

# contains data definition information for the term_synonym table

###--- Globals ---###

# name of this database table
tableName = 'term_synonym'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	term_key	int	NOT NULL,
	synonym		text	NULL,
	synonym_type	text	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'synonym' : 'create index %s on %s (synonym)',
	}

keys = { 'term_key' : ('term', 'term_key'), }

# index used to cluster data in the table
clusteredIndex = ('term_key', 'create index %s on %s (term_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'stores synonyms for vocabulary terms (in the term table)',
	Table.COLUMN : {
		'unique_key' : 'unique key for this record, needed for object identity in Hibernate',
		'term_key' : 'foreign key to term table, identifies the term for which we have a synonym',
		'synonym' : 'text of the synonym for the term',
		'synonym_type' : 'type of synonym',
		},
	Table.INDEX : {
		'term_key' : 'cluster by term key, so all synonyms of a term are stored together on disk',
		'synonym' : 'quick lookup by synonym, case sensitive',
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
