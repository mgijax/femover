#!./python

import Table

# contains data definition information for the probe_polymorphism table

###--- Globals ---###

# name of this database table
tableName = 'probe_polymorphism'

# MySQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        probe_polymorphism_key  int             NOT NULL,
        probe_reference_key             int             NOT NULL,
        marker_key                              int             NOT NULL,
        marker_id                               text    NOT NULL,
        marker_symbol                   text    NULL,
        endonuclease                    text    NULL,
        sequence_num                    int             NOT NULL,
        PRIMARY KEY(probe_polymorphism_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.
indexes = { 'marker_key' : 'create index %s on %s (marker_key)', }

keys = {
        'probe_reference_key' : ('probe_to_reference', 'probe_reference_key'),
        'marker_key' : ('marker', 'marker_key'),
        }

# index used to cluster data in the table
clusteredIndex = ('probe_reference_key', 'create index %s on %s (probe_reference_key)')

# comments describing the table, columns, and indexes
comments = {
        Table.TABLE : 'defines basic polymorphism data for a given probe in a given reference',
        Table.COLUMN : {
                'probe_polymorphism_key' : 'identifies this polymorphism; same as prb_rflv._RFLV_key in prod',
                'probe_reference_key' : 'identifies this probe/reference pair; same as prb_reference._Reference_key in prod',
                'marker_key' : 'identifies the marker',
                'marker_id' : 'primary ID of the marker, cached here for convenience',
                'marker_symbol' : 'symbol of the marker, cached here for convenience',
                'endonuclease' : 'endonuclease (An enzyme that cleaves its nucleic acid substrate at internal sites in the nucleotide sequence)',
                'sequence_num' : 'pre-computed sequence number for ordering polymorphisms of a probe/reference pair',
                },
        Table.INDEX : {
                'probe_reference_key' : 'clusters data so that all polymorphisms for a probe/reference pair will be near each other on disk, aiding quick access',
                'marker_key' : 'look up all polymorphisms involving a given marker',
                },
        }

# global instance of this Table object
table = Table.Table (tableName, createStatement, indexes, keys, comments, clusteredIndex)

###--- Main program ---###

# if executed as a script, pass the global Table object into the general
# main program for Table subclasses
if __name__ == '__main__':
        Table.main(table)
