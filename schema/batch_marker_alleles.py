#!./python

import Table

# contains data definition information for the batch_marker_alleles table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'batch_marker_alleles'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        marker_key      int             not null,
        allele_symbol   text    null,
        allele_id       text    null,
        sequence_num    int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the marker flower, containing basic allele data for each marker (only has data needed to support batch query, aiding efficiency)',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this marker/allele pair',
                'marker_key' : 'identifies the marker',
                'allele_symbol' : 'symbol for the allele (cached here from the allele table)',
                'allele_id' : 'primary accession ID for the allele (cached here from the allele table)',
                'sequence_num' : 'used to order alleles for each marker by nomenclature',
                },
        Table.INDEX : {
                'marker_key' : 'clusters data so alleles for each marker are grouped together on disk',
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
