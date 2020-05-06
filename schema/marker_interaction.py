#!./python

import Table

# contains data definition information for the marker_interaction table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'marker_interaction'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        mi_key                  int     not null,
        marker_key              int     not null,
        interacting_marker_key  int     not null,
        interacting_marker_symbol       text    not null,
        interacting_marker_id   text    not null,
        relationship_category   text    not null,
        relationship_term       text    not null,
        qualifier               text    not null,
        evidence_code           text    not null,
        reference_key           int     not null,
        jnum_id                 text    not null,
        sequence_num            int     not null,
        in_teaser               int     not null,
        is_reversed             int     not null,
        PRIMARY KEY(mi_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {
        'forTeaser' : 'create index %s on %s (marker_key, sequence_num, in_teaser)',
        'mi_key' : 'create index %s on %s (mi_key)',
        'marker_key' : 'create index %s on %s (marker_key)',
        'interacting_marker_key' : 'create index %s on %s (interacting_marker_key)',
        'is_reversed' : 'create index %s on %s (is_reversed)',
        }

# column name -> (related table, column in related table)
keys = {
        'marker_key' : ('marker', 'marker_key'),
        'interacting_marker_key' : ('marker', 'marker_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('markerSeqNum',
        'create index %s on %s (marker_key, sequence_num)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'A record in this table represents an interaction relationship between two markers, described by the relationship term and its qualifier',
        Table.COLUMN : {
                'mi_key' : 'primary key, uniquely identifying a record in this table',
                'marker_key' : 'foreign key to marker table, identifying the base marker for this relationship',
                'interacting_marker_key' : 'foreign key to marker table, identifying the interacting marker',
                'interacting_marker_symbol' : 'symbol of the interacting marker, cached for convenience',
                'interacting_marker_id' : 'primary accession ID of the interacting marker, cached for convenience',
                'relationship_category' : 'text name for the category of relationship',
                'relationship_term' : 'term describing the relationship (drawn from terms in the category)',
                'qualifier' : 'modifier on the relationship',
                'evidence_code' : 'type of evidence used to support the relationship',
                'reference_key' : 'foreign key to reference table, identifying the citation supporting this relationship',
                'jnum_id' : 'accession ID (J: number) for the reference, cached for convenience',
                'sequence_num' : 'integer, used to order interacting markers for a given base marker',
                'in_teaser' : 'integer, indicates whether this marker should be included as a teaser (1) or not (0) on the marker detail page for marker_key',
                },
        Table.INDEX : {
                'markerSeqNum' : 'clustered index ensures interacting markers for a given marker are stored in order together on disk',
                'forTeaser' : 'provides quick access to markers which should be included as teasers on the marker detail page for other markers',
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
