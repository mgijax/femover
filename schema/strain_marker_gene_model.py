#!./python

import Table

# contains data definition information for the strain_marker_gene_model table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'strain_marker_gene_model'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                      int             not null,
        strain_marker_key       int             not null,
        gene_model_id           text    null,
        logical_db                      text    null,
        sequence_num            int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'gene_model_id' : 'create index %s on %s (gene_model_id)',
        }

# column name -> (related table, column in related table)
keys = {
        'strain_marker_key' : ('strain_marker', 'strain_marker_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('strain_marker_key', 'create index %s on %s (strain_marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for strain marker flower, contains gene model IDs for each strain marker',
        Table.COLUMN : {
                'unique_key' : 'primary key for this table, no other purpose',
                'strain_marker_key' : 'identifies the strain marker for this ID (FK to strain_marker table)',
                'gene_model_id' : 'ID for a gene model associated with this strain marker',
                'logical_db' : 'name of the entity assigning the ID',
                'sequence_num' : 'pre-computed ordering of IDs for each strain marker',
                },
        Table.INDEX : {
                'gene_model_id' : 'quick lookup of a row by ID',
                'strain_marker_key' : 'clustered index to bring together on disk all IDs for a given strain marker',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
