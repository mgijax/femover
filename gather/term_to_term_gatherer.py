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

                # [0] First Sql in cmds
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
                logger.debug ('Collected %d term relationships: 0' % len(rows))

                # [1] Second Sql in cmds
                cols, rows = self.results[1]
                term1 = Gatherer.columnNumber(cols, 'termKey1')
                term2 = Gatherer.columnNumber(cols, 'termKey2')
                relType = Gatherer.columnNumber(cols, 'relationship_type')
                evidence = Gatherer.columnNumber(cols, 'evidence')
                xref = Gatherer.columnNumber(cols, 'cross_reference')
                for row in rows:
                        self.addRow('term_to_term',
                                [ row[term1], row[term2], row[relType],
                                        row[evidence], row[xref] ])
                logger.debug ('Collected %d term relationships: 1' % len(rows))

                # [2] Third Sql in cmds
                cols, rows = self.results[2]
                term1 = Gatherer.columnNumber(cols, 'termKey1')
                term2 = Gatherer.columnNumber(cols, 'termKey2')
                relType = Gatherer.columnNumber(cols, 'relationship_type')
                evidence = Gatherer.columnNumber(cols, 'evidence')
                xref = Gatherer.columnNumber(cols, 'cross_reference')
                for row in rows:
                        self.addRow('term_to_term',
                                [ row[term1], row[term2], row[relType],
                                        row[evidence], row[xref] ])
                        self.addRow('term_to_term',
                                [ row[term2], row[term1], row[relType],
                                        row[evidence], row[xref] ])
                logger.debug ('Collected %d term relationships: 2' % len(rows))

                # [3] Fourth Sql in cmds (MP to HP Synonym)
                cols, rows = self.results[3]
                term1 = Gatherer.columnNumber(cols, 'termKey1')
                term2 = Gatherer.columnNumber(cols, 'termKey2')
                relType = Gatherer.columnNumber(cols, 'relationship_type')
                evidence = Gatherer.columnNumber(cols, 'evidence')
                xref = Gatherer.columnNumber(cols, 'cross_reference')
                for row in rows:
                        self.addRow('term_to_term',
                                [ row[term1], row[term2], row[relType],
                                        row[evidence], row[xref] ])
                        self.addRow('term_to_term',
                                [ row[term2], row[term1], row[relType],
                                        row[evidence], row[xref] ])
                logger.debug ('Collected %d term relationships: 3' % len(rows))

                # [4] Fifth Sql in cmds (HP to MP Synonym)
                cols, rows = self.results[4]
                term1 = Gatherer.columnNumber(cols, 'termKey2') # 2 & 1 are flipped
                term2 = Gatherer.columnNumber(cols, 'termKey1') #
                relType = Gatherer.columnNumber(cols, 'relationship_type')
                evidence = Gatherer.columnNumber(cols, 'evidence')
                xref = Gatherer.columnNumber(cols, 'cross_reference')
                for row in rows:
                        self.addRow('term_to_term',
                                [ row[term1], row[term2], row[relType],
                                        row[evidence], row[xref] ])
                        self.addRow('term_to_term',
                                [ row[term2], row[term1], row[relType],
                                        row[evidence], row[xref] ])
                logger.debug ('Collected %d term relationships: 4' % len(rows))
                return

###--- globals ---###

cmds = [
        # 0. DO terms to their HPO terms (top of union), plus
        #       MP headers to HPO terms (bottom of union).  Note that some
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
        '''
        select _object_key_1 as termKey1, 
          _object_key_2 as termKey2, 
          'MP HP Popup'as relationship_type,
          mrp1.value as evidence,
          mrp2.value as cross_reference
        from  mgi_relationship mr, 
          mgi_relationship_property mrp1,
          mgi_relationship_property mrp2
        where mr._category_key = 1011
          and mr._relationship_key = mrp1._relationship_key
          and mr._relationship_key = mrp2._relationship_key
          and mrp1._propertyname_key = 109733906
          and mrp2._propertyname_key = 109733907
        union
        select _object_key_2, _object_key_1, 'MP HP Popup', mrp1.value, mrp2.value
        from  mgi_relationship mr, 
          mgi_relationship_property mrp1,
          mgi_relationship_property mrp2
        where mr._category_key = 1011
          and mr._relationship_key = mrp1._relationship_key
          and mr._relationship_key = mrp2._relationship_key
          and mrp1._propertyname_key = 109733906
          and mrp2._propertyname_key = 109733907
        ''',
        '''
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiTermLexicalMatching' as evidence,
         'exactMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and lower(vt1.term) = lower(vt2.term)
        ''',
        '''
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'exactMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt2._term_key = ms._object_key
          and lower(vt1.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1017
        union
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'relatedMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt2._term_key = ms._object_key
          and lower(vt1.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1018        
        union
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'narrowMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt2._term_key = ms._object_key
          and lower(vt1.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1019        
        union
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'broadMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt2._term_key = ms._object_key
          and lower(vt1.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1020        
        ''',
        '''
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'exactMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt1._term_key = ms._object_key
          and lower(vt2.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1017
        union
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'relatedMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt1._term_key = ms._object_key
          and lower(vt2.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1018
        union
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'broadMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt1._term_key = ms._object_key
          and lower(vt2.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1019
        union
        select
          vt1._term_key as termKey1,
          vt2._term_key as termKey2,
         'MP HP Popup' as relationship_type,
         'mgiSynonymLexicalMatching' as evidence,
         'narrowMatch' as cross_reference
        from 
          voc_term vt1, 
          voc_term vt2,
          mgi_synonym ms
        where vt1._vocab_key = 5
          and vt1.isobsolete = 0
          and vt2._vocab_key = 106
          and vt2.isobsolete = 0
          and vt1._term_key = ms._object_key
          and lower(vt2.term) = lower(ms.synonym)
          and ms._mgitype_key = 13
          and ms._synonymtype_key = 1020
        '''

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
