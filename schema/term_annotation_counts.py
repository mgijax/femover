#!/usr/local/bin/python

import Table

# contains data definition information for the term_annotation_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'term_annotation_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key				int		not null,
	term_key				int		not null,
	annotated_object_type			varchar(80)	not null,
	object_count_to_term			int		not null,
	object_count_with_descendents		int		not null,
	annot_count_to_term			int		not null,
	annot_count_with_descendents		int		not null,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'term_key' : 'create index %s on %s (term_key)',
	}

keys = {
	'term_key' : ('term', 'term_key'),
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
