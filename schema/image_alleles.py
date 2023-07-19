#!./python

import Table

# contains data definition information for the image_alleles table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'image_alleles'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        image_key               int             not null,
        allele_key              int             not null,
        allele_symbol           text    null,
        allele_name             text    null,
        allele_id               text    null,
        sequence_num            int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'allele_key' : 'create index %s on %s (allele_key)',
        }

keys = {
        'image_key' : ('image', 'image_key'),
        'allele_key' : ('allele', 'allele_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('image_key', 'create index %s on %s (image_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the image flower, caching minimal data about alleles to display on an image summary/detail page',
        Table.COLUMN : {
                'unique_key' : 'unique key for this record',
                'image_key' : 'identifies the image',
                'allele_key' : 'key of an allele associated with this image',
                'allele_symbol' : 'symbol for the allele, cached here for convenience',
                'allele_name' : 'name of the allele, cached here for convenience',
                'allele_id' : 'primary ID for the allele, cached here for convenience',
                'sequence_num' : 'used to order alleles for an image',
                },
        Table.INDEX : {
                'image_key' : 'clusters data so that all alleles for an image will be nearby on disk, and thus quickly returned',
                'allele_key' : 'can be used to look up images associated with an allele, though this should really be done with the allele_to_image table',
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
