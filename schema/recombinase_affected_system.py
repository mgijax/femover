#!./python

import Table

# contains data definition information for the
# recombinase_affected_system table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'recombinase_affected_system'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        allele_key              int             not null,
        allele_system_key       int             not null,
        system                  text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {
        'allele_key' : ('allele', 'allele_key'),
        'allele_system_key' : ('recombinase_allele_system', 'allele_system_key')
        }

# index used to cluster data in the table
clusteredIndex = ('allele_key', 'create index %s on %s (allele_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the allele and recombinase flowers, allowing ease in finding affected systems for each allele',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record',
                'allele_key' : 'identifies the allele',
                'allele_system_key' : 'identifies the allele/system pair',
                'system' : 'name of the affected system, cached here for convenience',
                },
        Table.INDEX : {
                'allele_key' : 'clusters rows on disk so that all affected systems for an allele can be retrieved efficiently',
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
