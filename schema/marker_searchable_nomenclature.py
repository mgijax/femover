#!./python

import Table

# contains data definition information for the marker_searchable_nomenclature
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_searchable_nomenclature'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key              int             not null,
        marker_key              int             not null,
        term                    text    not null,
        term_type               text    not null,
        sequence_num            int             not null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'term' : 'create index %s on %s (term)',
        'lower_term' : 'create index %s on %s (lower(term))',
        }

# column name -> (related table, column in related table)
keys = {
        'marker_key' : ('marker', 'marker_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'petal table in the marker flower, containing all nomenclature strings which can be used to search for markers',
        Table.COLUMN : {
                'unique_key' : 'uniquely identifies this record, no other purpose',
                'marker_key' : 'identifies the marker',
                'term' : 'a term that can be used to search for the marker',
                'term_type' : 'type of term',
                'sequence_num' : 'used to order the terms by preference (highest priority have lowest sequence_num) for each marker',
                },
        Table.INDEX : {
                'marker_key' : 'clusters data so all searchable terms for a marker are together on disk, useful for extraction scripts that load Solr indexes',
                'term' : 'search by term, case sensitive',
                'lower_term' : 'search by term, case insensitive using lower() function',
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
