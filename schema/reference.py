#!/usr/local/bin/python

import Table

# contains data definition information for the reference table

###--- Globals ---###

# name of this database table
tableName = 'reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	referenceKey   	int NOT NULL,
	referenceType  	varchar(4) NULL,
	primaryAuthor  	varchar(60) NULL,
	authors		varchar(510) NULL,
	title		varchar(510) NULL,
	journal      	varchar(100) NULL,
	vol       	varchar(20) NULL,
	issue       	varchar(25) NULL,
	pubDate       	varchar(30) NULL,
	year       	int NULL,
	pages       	varchar(30) NULL,
	jnumID       	varchar(30) NULL,
	jnumNumeric    	int NULL,
	pubmedID	varchar(30) NULL,
	citation    	varchar(255) NULL,
	shortCitation 	varchar(255) NULL,
	PRIMARY KEY(referenceKey))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'jnumID' : 'create index %s on %s (jnumID)',
	'refsKeySort' : 'create index %s on %s (referenceKey, jnumNumeric)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
