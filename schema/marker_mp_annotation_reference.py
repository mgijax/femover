#!./python

import Table

# contains data definition information for the marker_mp_annotation_reference
# table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_mp_annotation_reference'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        mp_reference_key        int             not null,
        mp_annotation_key       int             not null,
        reference_key           int             not null,
        jnum_id                 text            not null,
        sequence_num            int             not null,
        PRIMARY KEY(mp_reference_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'reference_key' : 'create index %s on %s (reference_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'mp_annotation_key' : ('marker_mp_annotation', 'mp_annotation_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('mp_annotation_key', 'create index %s on %s (mp_annotation_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'contains one or more references for each MP annotation in the marker_mp_annotation table',
        Table.COLUMN : {
                'mp_reference_key' : 'generated primary key for this table',
                'mp_annotation_key' : 'foreign key to the marker_mp_annotation table, identifying the annotation for which this is a reference',
                'reference_key' : 'foreign key to the reference table, identifyign the reference',
                'jnum_id' : 'J: number (accession ID) for the reference, cached here for convenience',
                'sequence_num' : 'sequence number for ordering the references of an annotation',
                },
        Table.INDEX : {
                'reference_key' : 'quick retrieval of all annotations made using a given reference',
                'mp_annotation_key' : 'clustered index to group the references of an annotation together for fast retrieval',
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
