#!/usr/local/bin/python

import Table

# contains anchor markers and information for displaying a minimap on maker detail

###--- Globals ---###

# name of this database table
tableName = 'marker_minimap_marker'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
	unique_key	int	NOT NULL,
	marker_key	int 	NOT NULL,
	anchor_marker_key	int NOT NULL,
	anchor_symbol		text NOT NULL,
	cm_offset	float	NOT NULL,
	max_cm_offset        float        NOT NULL,
	PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
	}

keys = { 'marker_key' : ('marker', 'marker_key'),
         'anchor_marker_key': ('marker', 'marker_key') 
}

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
	Table.TABLE : 'petal table for the allele flower, containing accession IDs assigned to the various alleles',
	Table.COLUMN : {
		'unique_key' : 'unique identifier for this record, no other significance',
		'marker_key' : 'identifies the marker for this minimap',
		'anchor_marker_key' : 'identifies each anchor marker for the primary marker',
		'anchor_symbol' : 'symbol of anchor marker for display purposes',
		'cm_offset' : 'cm offset for calculating minimap display',
                'max_cm_offset' : 'max cm offset for the chromosome that all these anchor markers share'
		},
	Table.INDEX : {
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
