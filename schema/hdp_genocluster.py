#!./python

import Table

# contains data definition information for the hdp_genocluster table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_genocluster'
#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents the unique "geno-cluster" key.
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int     not null,
        hdp_genocluster_key             int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'hdp_genocluster_key' : 'create unique index %s on %s (hdp_genocluster_key)',
        }

keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table for the genoclusetr petal, containing one row for each human disease portal genocluster',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record',
                'hdp_genocluster_key' : 'unique key identifying this human disease portal genotype cluster',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
