#!./python
# 
# gathers data for the 'antibody_sequence_num' table in the front-end database

import Gatherer
import logger
import symbolsort
import gc

NS = 'Not Specified'

###--- Functions ---###

def byNameKey(a):
        # assumes a is [key, ID, name, gene, #refs, ...]
        # sort by: name, ID

        return (symbolsort.splitter(a[2]), symbolsort.splitter(a[1]))

def byGeneKey(a):
        # assumes a is [key, ID, name, gene, #refs, ...]
        # sort by: gene, name, ID
        
        return (symbolsort.splitter(a[3]), symbolsort.splitter(a[2]), symbolsort.splitter(a[1]))

def byRefCountKey(a):
        # assumes a is [key, ID, name, gene, #refs, ...]
        # sort by: #refs (desc), name, ID
        
        return (-a[4], symbolsort.splitter(a[2]), symbolsort.splitter(a[1]))
        
###--- Classes ---###

class AntibodySequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the antibody_sequence_num table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for antibodies,
        #       collates results, sorts them, writes tab-delimited text file

        def collateResults(self):
                columns, rows = self.results[0]
        
                toSort = []             # list of [name, type, ID, key] elements

                keyCol = Gatherer.columnNumber(columns, '_Antibody_key')
                idCol = Gatherer.columnNumber(columns, 'accID')
                nameCol = Gatherer.columnNumber(columns, 'name')
                geneCol = Gatherer.columnNumber(columns, 'gene')
                numRefsCol = Gatherer.columnNumber(columns, 'numRefs')

                for row in rows:
                        toSort.append ( [ row[keyCol], row[idCol], row[nameCol], row[geneCol], row[numRefsCol] ] )
                        
                logger.debug('Collected %d rows to sort' % len(toSort))

                rows = []
                self.results = []
                gc.collect()
                
                logger.debug('Ran garbage collection')
                
                toSort.sort(key=byNameKey)
                i = 0
                for row in toSort:
                        i = i + 1
                        row.append(i)
                        
                logger.debug('Sorted %d antibodies by name' % len(toSort))
                
                toSort.sort(key=byGeneKey)
                i = 0
                for row in toSort:
                        i = i + 1
                        row.append(i)
                
                logger.debug('Sorted %d antibodies by gene' % len(toSort))
                
                toSort.sort(key=byRefCountKey)
                i = 0
                for row in toSort:
                        i = i + 1
                        row.append(i)
                
                logger.debug('Sorted %d antibodies by refCount' % len(toSort))
                
                self.finalColumns = [ '_Antibody_key', 'by_name', 'by_gene', 'by_ref_count' ]
                self.finalResults = []

                for row in toSort:
                        self.finalResults.append( (row[0], row[5], row[6], row[7]) )
                        
                toSort = []
                gc.collect()
                
                logger.debug('Collated results; ran garbage collection')
                return
        
###--- globals ---###

cmds = [
        # 0. data needed to sort the antibodies
        '''with
        ref_counts as (
            select r._object_key as _antibody_key, count(distinct r._refs_key) as numRefs
            from mgi_reference_assoc r, mgi_refassoctype t
            where r._MGIType_key = 6
            and r._RefAssocType_key = t._RefAssocType_key
            group by r._object_key
        ), 
        genes1 as (
          select am._antibody_key, mm.symbol
          from gxd_antibodymarker am, mrk_marker mm
          where am._marker_key = mm._marker_key
          order by am._antibody_key, mm.symbol
        ),
        genes2 as (
          select _antibody_key, STRING_AGG(symbol, ' ') as gene
          from genes1
          group by _antibody_key
        )
        select ab._Antibody_key, t.term as type, a.accID, ab.antibodyname as name, rc.numRefs, g2.gene
                from gxd_antibody ab, voc_term t, acc_accession a, ref_counts rc, genes2 g2
                where a._LogicalDB_key = 1
                        and ab._Antibody_key = a._Object_key
                        and a._MGIType_key = 6
                        and a.preferred = 1
                        and ab._antibodytype_key = t._Term_key
                        and ab._antibody_key = rc._antibody_key
                        and ab._antibody_key = g2._antibody_key ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Antibody_key', 'by_name', 'by_gene', 'by_ref_count' ]

# prefix for the filename of the output file
filenamePrefix = 'antibody_sequence_num'

# global instance of a AntibodySequenceNumGatherer
gatherer = AntibodySequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
