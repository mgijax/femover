#!./python
# 
# gathers data for the 'term_to_term' table in the front-end database.
# This table holds relationships from one vocabulary term to another.

import Gatherer
import logger
import gc

###--- Globals ---###

###--- Functions ---###

###--- Classes ---###

i = 0           # global sequence num

class TermToTermGatherer (Gatherer.CachingMultiFileGatherer):
        # Is: a data gatherer for the term_to_term table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for relationships
        #       among terms, collates results, writes tab-delimited text file
        # Note: This doesn't have to be a multi-file gatherer, as it only
        #       produces a single file, but it's convenient for the automatic
        #       management of how many output rows to keep in memory.

        def collateResults (self):
                # slice and dice the query results to produce our set of
                # final results

                cols, rows = self.results[0]

                term1 = Gatherer.columnNumber(cols, 'termKey1')
                term2 = Gatherer.columnNumber(cols, 'termKey2')
                relType = Gatherer.columnNumber(cols, 'relationship_type')
                evidence = Gatherer.columnNumber(cols, 'evidence')
                xref = Gatherer.columnNumber(cols, 'cross_reference')

                for row in rows:
                        self.addRow('term_to_term',
                                [ row[term1], row[term2], row[relType],
                                        row[evidence], row[xref] ])

                logger.debug ('Collected %d term relationships' % len(rows))
                return

###--- globals ---###

cmds = [
        # 0. DO terms to their HPO terms (top of union), plus
        #       MP headers to HPO terms (bottom of union).  Note that the
        #       cross-references are left null, as they are to come later.
        '''select a._Object_key as termKey1, a._Term_key as termKey2,
                'DO to HPO' as relationship_type,
                e.abbreviation as evidence, null as cross_reference
        from voc_annot a, voc_evidence ve, voc_term e
        where a._AnnotType_key = 1024
                and a._Annot_key = ve._Annot_key
                and ve._EvidenceTerm_key = e._Term_key
        union
        select r._Object_key_1, r._Object_key_2, 
                'MP header to HPO high-level', e.abbreviation, null
        from mgi_relationship r, voc_term e
        where r._Category_key = 1005
        and r._Evidence_key = e._Term_key
        union
        select r._Object_key_1, r._Object_key_2,
                'MP to EMAPA', e.abbreviation, null
        from mgi_relationship r, voc_term e
        where r._Category_key = 1007
        and r._Evidence_key = e._Term_key
        ''',
        ]

# order of fields (from the query results) to be written to the
# output file
files = [ ('term_to_term',
                [ 'termKey1', 'termKey2', 'relationship_type', 'evidence',
                        'cross_reference' ],
                [ Gatherer.AUTO, 'termKey1', 'termKey2', 'relationship_type',
                        'evidence', 'cross_reference' ]),
        ]

# global instance of a markerIDGatherer
gatherer = TermToTermGatherer (files, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
