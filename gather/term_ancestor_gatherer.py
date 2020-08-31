#!./python
# 
# gathers data for the 'term_ancestor' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class TermAncestorGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the term_ancestor table
        # Has: queries to execute against the source database
        # Does: queries the source database for ancestors of vocabulary terms,
        #       collates results, writes tab-delimited text file

        def collateResults (self):

                # first, handle the traditional DAG-organized vocabularies
                # from query 0.

                cols, rows = self.results[0]

                keyCol = Gatherer.columnNumber (cols, 'termKey')
                ancestorKeyCol = Gatherer.columnNumber (cols, 'ancestorKey')
                termCol = Gatherer.columnNumber (cols, 'ancestorTerm')
                idCol = Gatherer.columnNumber (cols, 'accID')

                ancestors = {}          # term key -> { ancestor ID : 1 }
                self.finalResults = []
                self.finalColumns = ['termKey', 'ancestorKey', 'ancestorTerm', 'ancestorID']

                logger.debug ('Got %d ancestor rows' % len(rows))

                for row in rows:
                        termKey = row[keyCol]
                        ancestorKey = row[ancestorKeyCol]

                        if termKey in ancestors:

                                # if we've already seen this ancestor for this term, then skip it

                                if ancestorKey in ancestors[termKey]:
                                        continue

                                # otherwise, add this ancestor to the set of those seen

                                ancestors[termKey][ancestorKey] = 1
                        else:
                                # this is the first ancestor for this term
                                ancestors[termKey] = { ancestorKey : 1 }

                        self.finalResults.append ( [ termKey, ancestorKey, row[termCol], row[idCol] ] )

                # clear this to allow memory to be reclaimed
                ancestors = {}  
                return

###--- globals ---###

cmds = [
        # 0. ancestors for vocabulary terms;
        #       could to a 'distinct' here, but we'll do it in code to save
        #       load on the database, and hopefully get a better response time
        '''select dc._DescendentObject_key as termKey,
                t._Term_key as ancestorKey,
                t.term as ancestorTerm,
                a.accID
        from DAG_DAG dd
        inner join DAG_Closure dc on (dd._DAG_key = dc._DAG_key)
        inner join VOC_Term t on (dc._AncestorObject_key = t._Term_key)
        inner join VOC_Vocab v on (t._Vocab_key = v._Vocab_key)
        left outer join ACC_Accession a on (dc._AncestorObject_key = a._Object_key
                and v._LogicalDB_key = a._LogicalDB_key
                and a._MGIType_key = 13
                and a.private = 0
                and a.preferred = 1)
        where dd._MGIType_key = 13''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'termKey', 'ancestorKey', 'ancestorTerm', 'ancestorID', ]

# prefix for the filename of the output file
filenamePrefix = 'term_ancestor'

# global instance of a TermAncestorGatherer
gatherer = TermAncestorGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
