#!./python

import Table

# contains data definition information for the strain table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        strain_key              int             not null,
        name                    text    not null,
        primary_id              text    null,
        strain_type             text    null,
        species                 text    null,
        description             text    null,
        is_standard             int             not null,
        is_sequenced    int             not null,
        PRIMARY KEY(strain_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        'strain_type' : 'create index %s on %s (strain_type)',
        'species' : 'create index %s on %s (species)',
        }

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table for set of strains tables',
        Table.COLUMN : {
                'strain_key' : 'unique key for this mouse strain (matches _Strain_key in prod database)',
                'name' : 'strain nomenclature',
                'primary_id' : 'primary MGI ID of the mouse strain',
                'strain_type' : 'high-level type of strain (attributes are more specific)',
                'species' : 'type of mouse for this strain',
                'description' : 'textual strain description (from note type 1013)',
                'is_standard' : '0/1 flag for whether this is an official strain or not',
                'is_sequenced' : '0/1 flag for whether this is one of the 19 sequenced strains or not',
                },
        Table.INDEX : {
                'primary_id' : 'quick access by primary strain ID',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
