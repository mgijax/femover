#!./python
# 
# gathers data for the 'term_sibling' table in the front-end database

import Gatherer
import logger

###--- Classes ---###

class TermSiblingGatherer (Gatherer.ChunkGatherer):
        # Is: a data gatherer for the term_sibling table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for siblings of terms,
        #       collates results, writes tab-delimited text file

        def getMinKeyQuery(self):
                return 'select min(_Vocab_key) from voc_vocab'
        
        def getMaxKeyQuery(self):
                return 'select max(_Vocab_key) from voc_vocab'
        
        def cacheTerms(self):
            # cache data for the vocabulary terms by processing queries 2 and 3
            # returns { term key : (acc ID, term, sequence num, is-leaf flag) }
            
            # first get a set of those term keys for leaf nodes
            
            isLeaf = set()  # contains term keys where the term is a leaf node (has no children)
            
            cols, rows = self.results[3]
            termKeyCol = Gatherer.columnNumber(cols, '_Term_key')

            for row in rows:
                isLeaf.add(row[termKeyCol])
            logger.debug('Collected %d leaf nodes' % len(isLeaf))

            # then collect the term data in general

            terms = {}      # maps from term key to:  ( acc ID, term, sequence num, is-leaf flag )
            
            cols, rows = self.results[4]
            termKeyCol = Gatherer.columnNumber(cols, '_Term_key')
            accIDCol = Gatherer.columnNumber(cols, 'accID')
            seqNumCol = Gatherer.columnNumber(cols, 'sequenceNum')
            termCol = Gatherer.columnNumber(cols, 'term')

            for row in rows:
                leaf = 0
                if row[termKeyCol] in isLeaf:
                    leaf = 1
                terms[row[termKeyCol]] = (row[accIDCol], row[termCol], row[seqNumCol], leaf)
                
            logger.debug('Collected data for %d terms' % len(terms))
            
            return terms
            
        def cacheEdges(self):
            # cache data for the DAG edges from query 4
            # returns: (parent map, children map)
            #    parent map is { term key : [ (edge label, parent key), ... ] }
            #    children map is { term key : [ (edge label, child key), ... ] }
            
            parents = {}
            children = {}
            
            cols, rows = self.results[5]
            parentCol = Gatherer.columnNumber(cols, 'parentKey')
            childCol = Gatherer.columnNumber(cols, 'childKey')
            labelCol = Gatherer.columnNumber(cols, 'label')
            
            for row in rows:
                parentKey = row[parentCol]
                childKey = row[childCol]
                label = row[labelCol]

                parentTuple = (label, parentKey)
                childTuple = (label, childKey)

                if parentKey not in children:
                    children[parentKey] = []
                children[parentKey].append(childTuple)
                
                if childKey not in parents:
                    parents[childKey] = []
                parents[childKey].append(parentTuple)

            logger.debug('Cached %s edges' % len(children))
            return parents, children

        def collateResults (self):
            # need to slice and dice query results to produce:
            self.finalColumns = [ 'childKey', 'siblingKey', 'term', 'accID', 'sequenceNum', 'is_leaf', 'label', 'parentKey' ]
            self.finalResults = []
            
            logger.debug('In collateResults()')
            
            terms = self.cacheTerms()
            parents, children = self.cacheEdges()
            
            # start at child nodes, look up to parents and then down to find siblings
            for childKey in parents.keys():
                for (parentLabel, parentKey) in parents[childKey]:
                    if parentKey in children:
                        for (siblingLabel, siblingKey) in children[parentKey]:
                            # skip edges back to self
                            if (siblingKey != childKey):
                                (accID, term, seqNum, leaf) = terms[siblingKey]
                                self.finalResults.append( [ childKey, siblingKey, term, accID, seqNum, leaf, siblingLabel, parentKey ] )
            
            logger.debug('Compiled %d in finalResults' % len(self.finalResults))
            return

        def postprocessResults(self):
                # for any vocabularies without sequence numbers, compute and add them
                # also, we need to add edge numbers

                logger.debug('In postprocessResults()')
                snCol = Gatherer.columnNumber(self.finalColumns, 'sequenceNum')
                termCol = Gatherer.columnNumber(self.finalColumns, 'term')
                
                positions = {}          # lowercase term : [ positions in self.finalResults of that term ]
                pos = 0
                maxSeqNum = 0
                
                for row in self.finalResults:
                        if row[snCol] == None:
                                lowerTerm = row[termCol].lower()
                                if lowerTerm in positions:
                                        positions[lowerTerm].append(pos)
                                else:
                                        positions[lowerTerm] = [ pos ]
                        else:
                                maxSeqNum = max(maxSeqNum, row[snCol])

                        pos = pos + 1
                
                if len(positions) > 0:
                        logger.debug('Found %d terms to sort' % len(positions))

                        toSort = list(positions.keys())
                        toSort.sort()
                        logger.debug('Sorted terms')

                        seqNum = maxSeqNum + 1
                        for term in toSort:
                                for pos in positions[term]:
                                        self.finalResults[pos][snCol] = seqNum
                                        seqNum = seqNum + 1

                        logger.debug("Assigned sequenceNum starting at %d" % (maxSeqNum + 1))
                return

###--- globals ---###

cmds = [
        # 0 : cleanup from previous chunks
        '''drop table if exists selected_vocabs''',
        
        # 1 : identify the vocabularies processed in this chunk (only non-simple ones)
        '''create temp table selected_vocabs as
                select _Vocab_key, isSimple
                from voc_vocab
                where _Vocab_key >= %d and _Vocab_key < %d
                    and isSimple = 0''',

        # 2 : identify terms that are leaves (having no descendants) in this chunk
        '''create index idx1 on selected_vocabs (_Vocab_key)''',
        
        # 3 : identify terms that are leaves (having no descendants) in this chunk
        '''select t._Term_key, 1 as is_leaf
                from voc_term t, selected_vocabs v
                where t._Vocab_key = v._Vocab_key
                        and not exists (select 1
                                from dag_node dn, dag_edge de
                                where t._Term_Key = dn._Object_key
                                        and dn._Node_key = de._Parent_key)''',

        # 4 : return other term data for this chunk
        '''select t._Term_key, a.accID, t.term, t.sequenceNum
                from voc_term t
                inner join selected_vocabs v on (t._Vocab_key = v._Vocab_key)
                left outer join acc_accession a on (
                    t._Term_key = a._Object_key
                    and a._MGIType_key = 13
                    and a.preferred = 1
                    and a.private = 0
                    and a._LogicalDB_key = (select min(_LogicalDB_key)
                        from acc_accession b
                        where b._MGIType_key = 13
                            and b.preferred = 1
                            and b.private = 0
                            and a._Object_key = b._Object_key) )''',
                                        
        # 5. get the edges among those terms (for the vocabs in this chunk)
        '''select p._Object_key as parentKey, c._Object_key as childKey, l.label
            from dag_node p, dag_edge e, dag_label l, dag_node c, voc_term t, selected_vocabs v
            where v._Vocab_key = t._Vocab_key
                and t._Term_key = p._Object_key
                and p._Node_key = e._Parent_key
                and c._Node_key = e._Child_key
                and e._Label_key = l._Label_key''',
        ]

# order of fields (from the query results) to be written to the output file
# (use the parentKey to identify the path number)
fieldOrder = [ Gatherer.AUTO, 'childKey', 'siblingKey', 'term', 'accID', 'sequenceNum',
        'is_leaf', 'label', 'parentKey' ]

# prefix for the filename of the output file
filenamePrefix = 'term_sibling'

# global instance of a TermSiblingGatherer
gatherer = TermSiblingGatherer (filenamePrefix, fieldOrder, cmds)
gatherer.setChunkSize(1)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
