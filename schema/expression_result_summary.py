#!./python

import Table

# contains data definition information for the expression_result_summary table

# Note: All table and field names should be all-lowercase with underscores
# used to separate words.

###--- Globals ---###

# name of this database table
tableName = 'expression_result_summary'

# SQL statement to create this table
createStatement = '''CREATE TABLE %s  ( 
        result_key              int             not null,
        assay_key               int             not null,
        assay_type              text            not null,
        assay_id                text            null,
        marker_key              int             null,
        marker_symbol           text            null,
        theiler_stage           text            null,
        age                     text            null,
        age_abbreviation        text            null,
        age_min                 float           null,
        age_max                 float           null,
        structure               text            null,
        structure_printname     text            null,
        structure_key           int             null,
        detection_level         text            null,
        is_expressed            text            null,
        reference_key           int             null,
        jnum_id                 text            null,
        has_image               int             not null,
        genotype_key            int             null,
        is_wild_type            int             not null,
        pattern                 text            null,
        specimen_key            int             null,
        PRIMARY KEY(result_key))''' % tableName

# Maps from index suffix to create statement for that index.  In each
# statement, the first %s is for the index name, and the second is for the
# table name.

clusteredIndex = ('clusteredIndex', 'create index %s on %s (marker_key, assay_key)')

indexes = {
        'assay_key' : 'create index %s on %s (assay_key)',
        'assay_type' : 'create index %s on %s (assay_type)',
        'marker_key' : 'create index %s on %s (marker_key)',
        'structure_key' : 'create index %s on %s (structure_key)',
        'reference_key' : 'create index %s on %s (reference_key)',
        'genotype_key' : 'create index %s on %s (genotype_key)',
        'specimen_key' : 'create index %s on %s (specimen_key)',
        }

# column name -> (related table, column in related table)
keys = {
        'assay_key' : ('expression_assay', 'assay_key'),
        'marker_key' : ('marker', 'marker_key'),
        'structure_key' : ('term', 'term_key'),
        'reference_key' : ('reference', 'reference_key'),
        'genotype_key' : ('genotype', 'genotype_key'),
        'specimen_key' : ('assay_specimen', 'specimen_key'),
        }

comments = {
        Table.TABLE : 'central table of expression result flower; stores individual expression results and associated data',
        Table.COLUMN : {
            'result_key' : 'unique key identifying an expression result; note that this does not correspond to GXD_Expression._Result_key in the mgd database',
            'assay_key' : 'specifies the assay with which this result is associated',
            'assay_type' : 'specifies the type of the expression assay',
            'assay_id' : 'accession ID for the assay',
            'marker_key' : 'foreign key to the marker table, specifying which marker was assayed',
            'marker_symbol' : 'caches the symbol of the assayed marker',
            'theiler_stage' : 'developmental stage for the structure',
            'age' : 'textual age, as recorded by MGI curators',
            'age_abbreviation' : 'abbreviated age for web display',
            'age_min' : 'computed minimum age (in DPC), based on textual age',
            'age_max' : 'computed maximum age (in DPC), based on textual age',
            'structure' : 'name of the anatomical structure in which expression was studied',
            'structure_printname' : 'includes names of any parent nodes for structure, to establish context to uniquely identify the structure from its name',
            'structure_key' : 'unique key for the given structure',
            'detection_level' : 'strength of expression as reported in the literature',
            'is_expressed' : 'summarization of detection_level -- was expression detected?',
            'reference_key' : 'foreign key to reference table; identifies the source of the expression data',
            'jnum_id' : 'accession ID for the reference',
            'has_image' : 'does this expression result have an image available for display?',
            'genotype_key' : 'foreign key to genotype table',
            'is_wild_type' : 'is this a wild-type expression result?',
            'pattern' : 'describes the pattern of expression observed',
            'specimen_key' : 'link to assay_specimen table (also same as _specimen_key in MGD)',
                },
        Table.INDEX : {
            'clusteredIndex' : 'used to group data for a marker and its assays together, for speed of access since this is a common use-case',
            'assay_key' : 'allows quick access to all results for a particular assay',
            'marker_key' : 'allows quick access to all results for a marker',
            'structure_key' : 'allows quick access to all results for a given anatomical structure',
            'reference_key' : 'allows quick access to all results for a given reference',
            'genotype_key' : 'allows quick access for all results for a given genotype',
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
