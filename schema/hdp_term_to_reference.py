#!/usr/local/bin/python

import Table

# contains data definition information for the annotation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_term_to_reference'

#
# statement to create this table
#
# hdp (human disease portal)
#
# A row in this table represents:
#	a term -> reference association between:
#		a) mouse marker/OMIM (1005)
#		b) mouse marker/alle/OMIM (1012)
#
# See gather for more information
#
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int	not null,
	term_key		int	not null,
	reference_key		int	not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term_key' : 'create index %s on %s (term_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        }

keys = {
        'term_key' : ('term', 'term_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the term/reference petal, containing one row for each term/reference',
	Table.COLUMN : {
		'unique_key' : 'unique key for this record',
		'term_key' : 'omim/disease term that is annotated to a mouse marker',
		'reference_key' : 'reference annotated to the term',
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
