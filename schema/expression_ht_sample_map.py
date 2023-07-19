#!./python

import Table

# contains data definition information for the expression_ht_sample_map table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_ht_sample_map'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key                              integer         not null,
        consolidated_sample_key integer         not null,
        sample_key                              integer         not null,
        sequence_num                    integer         not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'sample_key' : 'create index %s on %s (sample_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'consolidated_sample_key' : ('expression_ht_consolidated_sample', 'consolidated_sample_key'),
        'sample_key' : ('expression_ht_sample', 'sample_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('consolidated_sample_key', 'create index %s on %s (consolidated_sample_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : None,
        Table.COLUMN : {
                'unique_key' : 'primary key generated for this table',
                'consolidated_sample_key' : 'identifies the consolidated sample',
                'sample_key' : 'identifies the particular sample',
                'sequence_num' : 'pre-computed ordering of rows',
                },
        Table.INDEX : {
                'sample_key' : 'quick lookup by sample key',
                'consolidated_sample_key' : 'quick lookup by consolidated sample key',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
