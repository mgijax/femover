#!./python

import Table

# contains data definition information for the specimen_result_cell_type table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'specimen_result_cell_type'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                int        not null,
        specimen_result_key       int        not null,
        cell_type                 text       not null,
        cell_type_id              text       not null,
        sequence_num              int        not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'cell_type' : 'create index %s on %s (cell_type)',
        }

keys = {
        'specimen_result_key' : ('specimen_result', 'specimen_result_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('specimen_result_key', 'create index %s on %s (specimen_result_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains information about cell types for assay_specimen results (can be multiple cell types per result)',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies a single result/cell type pair',
                'specimen_result_key' : 'identifies the specimen_result',
                'cell_type' : 'one cell type for the given result',
                'cell_type_id' : 'Accession ID of cell type term',
                'sequence_num' : 'ascending integer for sorting cell types per result',
                },
        Table.INDEX : {
                'specimen_result_key' : 'clustered index for grouping all cell types of a given result together',
                'cell_type' : 'index for retrieval of all results with a certain cell type',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
