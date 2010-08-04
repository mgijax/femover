#!/usr/local/bin/python

import Table

# contains data definition information for the alleleAnnotation table

###--- Globals ---###

# name of this database table
tableName = 'alleleAnnotation'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	uniqueKey		int		NOT NULL,
	alleleKey		int		NOT NULL,
	annotationType		varchar(255)	NOT NULL,
	vocabName		varchar(255)	NOT NULL,
	term			varchar(255)	NOT NULL,
	termID			varchar(30)	NULL,
	annotationKey		int		NULL,
	qualifier		varchar(50)	NULL,
	evidenceTerm		varchar(255)	NULL,
	referenceKey		int		NULL,
	jnumID			varchar(30)	NULL,
	headerTerm		varchar(255)	NULL,
	PRIMARY KEY(uniqueKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'alleleKey' : 'create index %s on %s (alleleKey, annotationType)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
