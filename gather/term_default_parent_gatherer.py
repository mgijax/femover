#!./python
# 
# gathers data for the 'term_default_parent' table in the front-end database
#
# This code could eventually be merged in as part of the gatherer for the 'term' table, but we're
# keeping it separate for now while the rules are in flux and until the vocabulary_gatherer is
# refactored (TR12542).
#
# Current vocabularies and their default-parent rules:
#       1. Adult Mouse Anatomy
#               a. prefer part-of relationships over is-a
#               b. use smart alpha to choose first parent
#       2. Mammalian Phenotype Ontology, GO, HPO
#               a. use same rules from #1 until further notice

import Gatherer
import logger
import symbolsort

###--- Globals ---###

parentKeyCol = None
parentTermCol = None
edgeLabelCol = None
childKeyCol = None

IS_A = 'is-a'
PART_OF = 'part-of'

###--- Function ---###

def amaSortKey(a):
        # produce a sort key to compare based on AMA rules, which have three levels:
        #       1. sort by child term key
        #       2. then by edge type (part-of relationships first)
        #       3. then smart-alpha by parent term

        edgeValue = 1           # not part-of
        if a[edgeLabelCol] == PART_OF:
                edgeValue = 0   # is part of
        return (a[childKeyCol], edgeValue, symbolsort.splitter(a[parentTermCol]))

###--- Classes ---###

class DefaultParentGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the term_default_parent table
        # Has: queries to execute against the source database
        # Does: queries the source database for data needed to identify the default parent term (for display) of
        #       terms from certain vocabularies, collates results, writes tab-delimited text file

        def collateResults(self):
                global parentKeyCol, parentTermCol, edgeLabelCol, childKeyCol

                cols, rows = self.results[0]
                logger.debug('Retrieved %d data rows' % len(rows))
                
                parentKeyCol = Gatherer.columnNumber(cols, 'parent_key')
                parentTermCol = Gatherer.columnNumber(cols, 'parent_term')
                edgeLabelCol = Gatherer.columnNumber(cols, 'label')
                childKeyCol = Gatherer.columnNumber(cols, 'child_key')
                
                rows.sort(key=amaSortKey)
                logger.debug('Sorted %d data rows' % len(rows))
                
                defaultParents = {}             # maps from child key to (default parent key, edge label)
                for row in rows:
                        childKey = row[childKeyCol]
                        if not childKey in defaultParents:
                                defaultParents[childKey] = (row[parentKeyCol], row[edgeLabelCol])

                logger.debug('Assigned default parents for %d children' % len(defaultParents))

                self.finalColumns = [ 'child_key', 'parent_key', 'label' ]
                self.finalResults = []
                
                children = list(defaultParents.keys())
                children.sort()
                
                for childKey in children:
                        self.finalResults.append( (childKey, defaultParents[childKey][0], defaultParents[childKey][1]) )

                logger.debug('Collated result set')
                return
        
###--- globals ---###

cmds = [
        # 0. parent data for each child term key, with a preliminary ordering.
        # includes all parents of the children terms, so we can finalize the ordering in memory and then
        # pick the first parent as the default one.
        '''select child._Object_key as child_key, l.label,
                        parent._Object_key as parent_key, p.term as parent_term
                from voc_vocab v, voc_term t, voc_vocabdag d, dag_node child, dag_edge e,
                        dag_node parent, voc_term p, dag_label l
                where v._Vocab_key = t._Vocab_key
                        and v.name in ('Adult Mouse Anatomy', 'Mammalian Phenotype', 'GO', 'Human Phenotype Ontology', 'Disease Ontology', 'Cell Ontology')
                        and v._Vocab_key = d._Vocab_key
                        and d._DAG_key = child._DAG_key
                        and t._Term_key = child._Object_key
                        and child._Node_key = e._Child_key
                        and e._Parent_key = parent._Node_key
                        and parent._Object_key = p._Term_key
                        and e._Label_key = l._Label_key
                order by 1, 2''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, 'child_key', 'parent_key', 'label', ]

# prefix for the filename of the output file
filenamePrefix = 'term_default_parent'

# global instance of a DefaultParentGatherer
gatherer = DefaultParentGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
