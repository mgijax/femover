#!./python

import Table

# contains data definition information for the allele_imsr_counts table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_imsr_counts'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        allele_key              int     not null,
        cell_line_count         int     not null,
        strain_count            int     not null,
        count_for_marker        int     not null,
        PRIMARY KEY(allele_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

keys = { 'allele_key' : ('allele', 'allele_key') }

# index used to cluster data in the table
clusteredIndex = None

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table for the allele flower, containing counts of records in IMSR for each allele',
        Table.COLUMN : {
                'allele_key' : 'identifies the allele',
                'cell_line_count' : 'count of cell lines in IMSR containing this allele',
                'strain_count' : 'count of mouse strains in IMSR containing this allele',
                'count_for_marker' : 'count of cell lines and mouse strains in IMSR which contain any allele of the same marker (gene) as this allele',
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
