#!/usr/local/bin/python

import Table

# contains data definition information for the reference_book table

###--- Globals ---###

# name of this database table
tableName = 'reference_book'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reference_key	int NOT NULL,
	editor    	varchar(160) NULL,
	book_title  	varchar(200) NULL,
	edition  	varchar(50) NULL,
	place	  	varchar(50) NULL,
	publisher  	varchar(50) NULL,
	PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
