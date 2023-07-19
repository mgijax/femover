#!./python

import Table

# contains data definition information for the universal_expression_result table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'universal_expression_result'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        uni_key         integer         not null,
        is_classical    integer         not null,
        result_key      integer         not null,
        PRIMARY KEY(uni_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'is_classical' : 'create index %s on %s (is_classical)'
        }

# column name -> (related table, column in related table)
keys = {}

# index used to cluster data in the table
clusteredIndex = ('result_key', 'create index %s on %s (result_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'universal_expression_result',
        Table.COLUMN : {
                'uni_key' : 'generated primary key for this table, ties together uni_* tables',
                'is_classical' : 'flag to indicate whether this result is for classical data (1) or not (0)',
                'result_key' : 'refers to either a result_key (for classical data) or an _RNASeqCombined_key (for RNA-Seq data)',
                },
        Table.INDEX : {
                'result_key' : 'efficient lookup by result key',
                'is_classical' : 'to help in looking for only classical or RNA-Seq data',
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
