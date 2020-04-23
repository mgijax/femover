#!./python

import Table

# contains data definition information for the grid cluster notation table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
# human disease portal
tableName = 'hdp_gridcluster'

#
# statement to create this table
#
# hdp (human disease portal)
#
# This table represents the unique "grid cluster" key 
# of the homologene clusters for hdp-specific
# super-simple genotypes (or null genotypes).
#
# plus it creates a "grid cluster" key for mouse/human markers
# that do NOT exist in the homologene cluster.
#
# see the gatherer for more information
#
createStatement = '''CREATE TABLE %s  ( 
        hdp_gridcluster_key     int     not null,
        homologene_id           text    null,
        source                  text    null,
        PRIMARY KEY(hdp_gridcluster_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = {}

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'central table for the grid cluster petal, containing one row for each human disease portal grid cluster',
        Table.COLUMN : {
                'hdp_gridcluster_key' : 'unique key identifying this human disease portal grid cluster',
                'homologene_id' : 'homologene accession id',
                'source' : 'source of the homology cluster (HGNC, HomoloGene)',
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
