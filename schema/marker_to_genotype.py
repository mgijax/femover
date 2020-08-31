#!./python

import Table

# contains data definition information for the marker_to_genotype table

###--- Globals ---###

# name of this database table
tableName = 'marker_to_genotype'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             NOT NULL,
        marker_key      int             NOT NULL,
        genotype_key    int             NOT NULL,
        reference_key   int             NULL,
        qualifier       text    NULL,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'marker_key' : 'create index %s on %s (marker_key)',
        }

keys = {
        'marker_key' : ('marker', 'marker_key'),
        'genotype_key' : ('genotype', 'genotype_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('genotype_key', 'create index %s on %s (genotype_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'join table between the marker and genotype flowers, showing which markers have been mutated in which genotypes',
        Table.COLUMN : {
                'unique_key' : 'unique key identifying this association, no other purpose',
                'marker_key' : 'identifies the marker',
                'genotype_key' : 'identifies a genotype containing alleles of the marker',
                'reference_key' : 'identifies the reference for this association (always null for now)',
                'qualifier' : 'qualifier describing this association',
                },
        Table.INDEX : {
                'marker_key' : 'search for all genotypes in which this marker has been mutated',
                'genotype_key' : 'clusters the data so all markers mutated in a given genotype can be retrieved quickly (the most common use of this table)',
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
