#!/usr/local/bin/python

import Table

# contains data definition information for the sequence_gene_trap table

###--- Globals ---###

# name of this database table
tableName = 'sequence_gene_trap'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	sequence_key		int		NOT NULL,
	tag_method		varchar(40)	NOT NULL,
	vector_end		varchar(40)	NOT NULL,
	reverse_complement	varchar(40)	NOT NULL,
	good_hit_count		int		NULL,
	point_coordinate	float		NULL,
	PRIMARY KEY(sequence_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'sequence_key' : ('sequence', 'sequence_key') }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
