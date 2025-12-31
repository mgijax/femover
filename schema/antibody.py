#!./python

import Table

# contains data definition information for the antibody table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'antibody'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        antibody_key            int     not null,
        name                    text    not null,
        primary_id              text    not null,
        host                    text    not null,
        antibody_type           text    not null,
        antibody_class          text    not null,
        expression_result_count int     not null,
        synonyms                text    null,
        note                    text    null,

        ag_region_covered       text    null,
        ag_note                 text    null,
        ag_species              text    not null,
        ag_strain               text    not null,
        ag_sex                  text    not null,
        ag_age                  text    not null,
        ag_tissue               text    not null,
        ag_cell_line            text    not null,
        ag_tissue_description   text    null,

        PRIMARY KEY(antibody_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        }

# column name -> (related table, column in related table)
keys = { }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table of the antibody flower',
        Table.COLUMN : {
                'antibody_key' : 'unique key for the antibody',
                'name' : 'name of the antibody',
                'primary_id' : 'primary accession ID of the antibody',
                'host' : 'species in which the antibody developed',
                'antibody_type' : 'type of antibody',
                'antibody_class' : 'class of antibody',
                'expression_result_count' : 'number of expression results citing this antibody',
                'synonyms' : 'comma-separated list of synonyms for this antibody',
                'note' : 'additional notes',
                'ag_region_covered' : 'region of antigen covered by antibody',
                'ag_note' : 'general note about the antigen',
                'ag_species' : 'species in which the antigen originated',
                'ag_strain' : 'mouse strain for the antigen',
                'ag_sex' : 'gender of the antigen source',
                'ag_age' : 'age of the antigen source',
                'ag_tissue' : 'tissue of the antigen source',
                'ag_cell_line' : 'cell line of the antigen source',
                'ag_tissue_description' : 'textual description of the antigen source tissue',
                },
        Table.INDEX : {
                'primary_id' : 'quick lookup by accession ID',
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
