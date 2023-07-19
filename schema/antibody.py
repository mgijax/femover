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
        antigen_key             int     not null,
        synonyms                text    null,
        note                    text    null,
        PRIMARY KEY(antibody_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'primary_id' : 'create index %s on %s (primary_id)',
        'antigen_key' : 'create index %s on %s (antigen_key)',
        }

# column name -> (related table, column in related table)
keys = { 'antigen_key' : ('antigen', 'antigen_key'),
        }

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
                'antigen_key' : 'database key of the antigen associated with this antibody',
                'synonyms' : 'comma-separated list of synonyms for this antibody',
                'note' : 'additional notes',
                },
        Table.INDEX : {
                'primary_id' : 'quick lookup by accession ID',
                'antigen_key' : 'quick lookup of all antibodies for an antigen',
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
