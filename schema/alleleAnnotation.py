#!/usr/local/bin/python

import Table

# contains data definition information for the alleleAnnotation table

###--- Globals ---###

# name of this database table
tableName = 'allele_annotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key		int		NOT NULL,
	allele_key		int		NOT NULL,
	annotation_type		varchar(255)	NOT NULL,
	vocab_name		varchar(255)	NOT NULL,
	term			varchar(255)	NOT NULL,
	term_id			varchar(30)	NULL,
	annotation_key		int		NULL,
	qualifier		varchar(50)	NULL,
	evidence_term		varchar(255)	NULL,
	reference_key		int		NULL,
	jnum_id			varchar(30)	NULL,
	header_term		varchar(255)	NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'allele_key' : 'create index %s on %s (allele_key, annotation_type)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
