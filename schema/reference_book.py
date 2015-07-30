#!/usr/local/bin/python

import Table

# contains data definition information for the reference_book table

###--- Globals ---###

# name of this database table
tableName = 'reference_book'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	reference_key	int NOT NULL,
	editor    	text NULL,
	book_title  	text NULL,
	edition  	text NULL,
	place	  	text NULL,
	publisher  	text NULL,
	PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'reference_key' : ('reference', 'reference_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the reference flower, containing data related specifically to books',
	Table.COLUMN : {
		'reference_key' : 'uniquely identifies the reference',
		'editor' : 'book editor',
		'book_title' : 'title of the book',
		'edition' : 'which edition',
		'place' : 'where published',
		'publisher' : 'name of the publisher',
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
