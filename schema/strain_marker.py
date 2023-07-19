#!./python

import Table

# contains data definition information for the strain_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_marker'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        strain_marker_key               int             not null,
        canonical_marker_key    int             null,
        canonical_marker_id             text    null,
        canonical_marker_symbol text    null,
        canonical_marker_name   text    null,
        strain_key                              int             not null,
        strain_name                             text    null,
        strain_id                               text    null,
        feature_type                    text    null,
        chromosome                              text    null,
        start_coordinate                float   null,
        end_coordinate                  float   null,
        strand                                  text    null,
        length                                  int             null,
        sequence_num                    int             not null,
        PRIMARY KEY(strain_marker_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'strain_key' : 'create index %s on %s (strain_key)',
        'canonical_id' : 'create index %s on %s (canonical_marker_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'canonical_marker_key' : ('marker', 'marker_key'),
        'strain_key' : ('strain', 'strain_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('canonical_marker_key', 'create index %s on %s (canonical_marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table for strain markers (strain-specific markers)',
        Table.COLUMN : {
                'strain_marker_key' : 'primary key for this table, identifies a strain marker object',
                'canonical_marker_key' : 'marker key for the canonical marker associated with this strain marker (FK to marker table)',
                'canonical_marker_id' : 'primary ID for the canonical marker associated with this strain marker (if any)',
                'canonical_marker_symbol' : 'official symbol for the canonical marker associated with this strain marker (if any)',
                'canonical_marker_name' : 'official name for the canonical marker associated with this strain marker (if any)',
                'strain_key' : 'strain key of the associated strain (FK to strain table)',
                'strain_name' : 'name of the strain, cached for convenience',
                'strain_id' : 'primary ID of the strain, cached for convenience',
                'feature_type' : 'feature type of this marker in this strain',
                'chromosome' : 'chromosome of the marker in this strain',
                'start_coordinate' : 'start coordinate for the marker in the assembly for this strain',
                'end_coordinate' : 'end coordinate for the marker in the assembly for this strain',
                'strand' : '+ or - strand for this marker in the assembly for this strain',
                'length' : 'length of the sequence, pre-computed for convenience',
                'sequence_num' : 'pre-computed integer for ordering the strain markers for a given canonical marker',
                },
        Table.INDEX : {
                'strain_key' : 'quick retrieval of strain markers for a given strain',
                'canonical_marker_key' : 'clustered index to ensure all strain markers are together on disk for a given canonical marker',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
