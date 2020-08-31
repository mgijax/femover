#!./python

import Table

# contains data definition information for the antigen table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'antigen'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        antigen_key             int             not null,
        primary_id              text            not null,
        name                    text            not null,
        species                 text            not null,
        strain                  text            not null,
        sex                     text            not null,
        age                     text            not null,
        tissue                  text            not null,
        cell_line               text            not null,
        tissue_description      text            null,
        region_covered          text            null,
        note                    text            null,
        PRIMARY KEY(antigen_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        }

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the antibody flower, storing attributes of an antigen which is associated with any number of antibodies',
        Table.COLUMN : {
                'antigen_key' : 'unique key for the antigen',
                'primary_id' : 'primary accession ID for the antigen',
                'name' : 'textual name for the antigen',
                'species' : 'species in which the antigen originated',
                'strain' : 'mouse strain for the antigen',
                'sex' : 'gender of the source',
                'age' : 'age of the source',
                'tissue' : 'tissue of the source',
                'cell_line' : 'cell line of the source',
                'tissue_description' : 'textual description of the tissue',
                'region_covered' : 'region covered by the antigen',
                'note' : 'additional notes',
                },
        Table.INDEX : {
                'primary_id' : 'enables quick lookups by antigen ID',
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
