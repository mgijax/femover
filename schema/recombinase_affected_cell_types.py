#!./python

import Table

# contains data definition information for the
# recombinase_affected_cell_types table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_affected_cell_types'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        allele_key              int             not null,
        cell_type_header_key    int             not null,
        cell_type_header        text            not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'allele_key' : ('allele', 'allele_key'),
        'cell_type_header_key' : ('term', 'term_key')
        }

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'lists cell type header terms where recombinase activity is detected',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record',
                'allele_key' : 'identifies the allele',
                'cell_type_header_key' : 'key of the cell type header term',
                'cell_type_header' : 'name of the cell type header term',
                },
        Table.INDEX : {
                'allele_key' : 'clusters rows on disk so that all cell types for an allele can be retrieved efficiently',
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
