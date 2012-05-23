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
	indexed_for_gxd	int	not null,
	PRIMARY KEY(reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	'jnum_id' : 'create index %s on %s (jnum_id)',
	'journal' : 'create index %s on %s (journal)',
	'refs_key_sort' : 'create index %s on %s (reference_key, jnum_numeric)',
	'indexed_for_gxd' : 'create index %s on %s (indexed_for_gxd)',
	}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'central table for the reference flower, containing basic info for various periodicals, books, and other data sources',
	Table.COLUMN : {
		'reference_key' : 'uniquely identifies a reference, same as _Refs_key from mgd',
		'reference_type' : 'type of the reference',
		'primary_author' : 'primary author',
		'authors' : 'full list of authors',
		'title' : 'reference title',
		'journal' : 'name of the journal',
		'vol' : 'volume number',
		'issue' : 'issue number',
		'pub_date' : 'publication date',
		'year' : 'year of publication',
		'pages' : 'pages cited',
		'jnum_id' : 'J: number (accession ID)',
		'jnum_numeric' : 'numeric part of the J: number',
		'pubmed_id' : 'PubMed ID',
		'mini_citation' : 'smallest-format citation',
		'short_citation' : 'short-format citation',
		'long_citation' : 'long-format citation',
		'indexed_for_gxd' : '1 if reference is indexed for expression data, 0 if not',
		},
	Table.INDEX : {
		'jnum_id' : 'provides quick lookup by J: number',
		'journal' : 'lookup by journal name',
		'refs_key_sort' : 'allows quick ordering by J: number',
		'indexed_for_gxd' : 'allows quick lookup of all expression-related references',
		},
	}

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, {}, comments,
		clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
	Table.main(table)
