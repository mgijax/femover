#!./python

import Table

# contains data definition information for the marker_microarray table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_microarray'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        unique_key      int             not null,
        marker_key      int             not null,
        probeset_id     text    not null,
        platform        text    null,
        report_name     text    null,
        PRIMARY KEY(unique_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {} 

# column name -> (related table, column in related table)
keys = { 'marker_key' : ('marker', 'marker_key') }

# index used to cluster data in the table
clusteredIndex = ('marker_key', 'create index %s on %s (marker_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'stores microarray probeset data for markes',
        Table.COLUMN : {
                'unique_key' : 'unique key identifying this record (no other purpose)',
                'marker_key' : 'foreign key to marker table',
                'probeset_id' : 'accession ID for this probeset',
                'platform' : 'name of the microarray platform',
                'report_name' : 'filename for the corresponding report on MGI ftp site',
                },
        Table.INDEX : {
                'marker_key' : 'clustered index to ensure that records for a marker are stored near each other on disk',
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
