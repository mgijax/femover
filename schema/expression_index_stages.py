#!./python

import Table

# contains data definition information for the expression_index_stages table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index_stages'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        index_key               int             not null,
        assay_type              text    null,
        age                     float           not null,
        age_string              text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'index_key' : ('expression_index', 'index_key') }

# index used to cluster data in the table
clusteredIndex = ('index_key', 'create index %s on %s (index_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the expression literature flower, containing the age/assay_type pairs for each literature index record',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record',
                'index_key' : 'identifies the literature index record (the marker/reference pair)',
                'assay_type' : 'type of expression assay (Note that literature index assay types differ from full-coded assay types.  A mapping is in expressionindex_assay_type_map.)',
                'age' : 'float equivalent of age_string',
                'age_string' : 'age in DPC, with special values (A for adule, E for embryonic)',
                },
        Table.INDEX : {
                'index_key' : 'clusters rows such that all age/assay_type pairs for a given literature index record will be together on disk for quick access',
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
