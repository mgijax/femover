#!./python

import Table

# contains data definition information for the expression_index_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_index_counts'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        index_key                       int     not null,
        assay_age_count                 int     not null,
        fully_coded_assay_count         int     not null,
        fully_coded_result_count        int     not null,
        PRIMARY KEY(index_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'index_key' : ('expression_index', 'index_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for expression literature flower, containing pre-computed counts for each literature index record',
        Table.COLUMN : {
                'index_key' : 'identifies the literature index record',
                'assay_age_count' : 'count of assay/age pairs',
                'fully_coded_assay_count' : 'count of full-coded assays',
                'fully_coded_result_count' : 'count of full-coded assay results',
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
