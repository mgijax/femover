#!./python

import Table

# contains data definition information for the allele_related_marker table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'allele_related_marker'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        arm_key                 int     not null,
        allele_key              int     not null,
        related_marker_key      int     not null,
        related_marker_symbol   text    not null,
        related_marker_id       text        null,
        relationship_category   text    not null,
        relationship_term       text    not null,
        qualifier               text    not null,
        evidence_code           text    not null,
        reference_key           int     not null,
        jnum_id                 text    not null,
        sequence_num            int     not null,
        in_teaser               int     not null,
        PRIMARY KEY(arm_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = {}

# column name -> (related table, column in related table)
keys = {
        'allele_key' : ('allele', 'allele_key'),
        'related_marker_key' : ('marker', 'marker_key'),
        'reference_key' : ('reference', 'reference_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('alleleSeqNum',
                'create index %s on %s (allele_key, sequence_num, in_teaser)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'A record in this table represents a relationship between an allele and a marker (separate from the traditional "This is an allele of this marker" relationship), described by the relationship term and its qualifier',
        Table.COLUMN : {
                'arm_key' : 'primary key, uniquely identifying a record in this table',
                'allele_key' : 'foreign key to allele table, identifying the base allele for this relationship',
                'related_marker_key' : 'foreign key to marker table, identifying the related marker',
                'related_marker_symbol' : 'symbol of the related marker, cached for convenience',
                'related_marker_id' : 'primary accession ID of the related marker, cached for convenience',
                'relationship_category' : 'text name for the category of relationship',
                'relationship_term' : 'term describing the relationship (drawn from terms in the category)',
                'qualifier' : 'modifier on the relationship',
                'evidence_code' : 'type of evidence used to support the relationship',
                'reference_key' : 'foreign key to reference table, identifying the citation supporting this relationship',
                'jnum_id' : 'accession ID (J: number) for the reference, cached for convenience',
                'sequence_num' : 'integer, used to order related markers for a given base allele',
                'in_teaser' : 'integer, 1 if the marker is to be shown as a teaser for this allele, 0 if not'
                },
        Table.INDEX : {
                'alleleSeqNum' : 'clustered index ensures related markers for a given allele are stored in order together on disk',
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
