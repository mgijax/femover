#!/usr/local/bin/python

import Table

# contains data definition information for the reference table

###--- Globals ---###

# name of this database table
tableName = 'reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reference_key   int NOT NULL,
	reference_type  varchar(4) NULL,
	primary_author  varchar(60) NULL,
	authors		varchar(510) NULL,
	title		varchar(510) NULL,
	journal      	varchar(100) NULL,
	vol       	varchar(20) NULL,
	issue       	varchar(25) NULL,
	pub_date       	varchar(30) NULL,
	year       	int NULL,
	pages       	varchar(30) NULL,
	jnum_id       	varchar(30) NULL,
	jnum_numeric    int NULL,
	pubmed_id	varchar(30) NULL,
	mini_citation  	varchar(765) NULL,
	short_citation 	varchar(765) NULL,
	long_citation  	varchar(2040) NULL,
	PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'jnum_id' : 'create index %s on %s (jnum_id)',
	'journal' : 'create index %s on %s (journal)',
	'refs_key_sort' : 'create index %s on %s (reference_key, jnum_numeric)',
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
