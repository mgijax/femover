#!./python
# 
# gathers data for the 'marker_polymorphism_allele' table in the front-end database

import Gatherer
import logger
import symbolsort

###--- Function ---###

###--- Classes ---###

def tripleCompare(a):
        # return a sort key for sorting items like 'a', each having: (rflv key, allele, fragments)
        # sort by:  rflv key (ascending), allele (smart-alpha), fragments (ascending)
        
        return (a[0], symbolsort.splitter(a[1]), a[2])

class MPAGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the marker_polymorphism_allele table
        # Has: queries to execute against the source database
        # Does: queries the source database for alleles for polymorphisms (which are themselves associated
        #       with markers, collates results, writes tab-delimited text file
        
        def cacheStrains(self):
                cols0, rows0 = self.results[0]
                
                strains = {}            # allele key : [ list of strains ]
                strainsStr = {}         # allele key : string with comma-delimited strains
                
                keyCol = Gatherer.columnNumber(cols0, '_Allele_key')
                strainCol = Gatherer.columnNumber(cols0, 'strain')
                
                for row in rows0:
                        alleleKey = row[keyCol]
                        if alleleKey not in strains:
                                strains[alleleKey] = []
                        strains[alleleKey].append(row[strainCol])
                        
                logger.debug('Cached strains for %d alleles' % len(strains))
                
                strainKeys = list(strains.keys())
                strainKeys.sort()
                for key in strainKeys:
                        strains[key].sort(key=symbolsort.splitter)
                        strainsStr[key] = ', '.join(strains[key])

                logger.debug('Sorted strains for %d alleles' % len(strains))
                return strainsStr
        
        def collateResults(self):
                strains = self.cacheStrains() 
                
                self.finalColumns = [ '_RFLV_key', 'allele', 'fragments', 'strains', ]
                self.finalResults = []
                
                cols1, rows1 = self.results[1]
                
                rflvKeyCol = Gatherer.columnNumber(cols1, '_RFLV_key')
                alleleKeyCol = Gatherer.columnNumber(cols1, '_Allele_key')
                alleleCol = Gatherer.columnNumber(cols1, 'allele')
                fragmentsCol = Gatherer.columnNumber(cols1, 'fragments')
                
                for row in rows1:
                        alleleKey = row[alleleKeyCol]
                        fragments = row[fragmentsCol].replace(',', ', ')        # add space after commas
                        strainString = None

                        if alleleKey in strains:
                                strainString = strains[alleleKey]

                        self.finalResults.append ( [ row[rflvKeyCol], row[alleleCol], fragments, strainString ])
                return
        
        def postprocessResults(self):
                seqNum = {}                             # (rflv key, allele name, fragments) : integer sequence num
                
                self.finalColumns.append('sequence_num')
                
                fragmentsCol = Gatherer.columnNumber(self.finalColumns, 'fragments')
                rflvCol = Gatherer.columnNumber(self.finalColumns, '_RFLV_key')
                alleleCol = Gatherer.columnNumber(self.finalColumns, 'allele')
                
                for row in self.finalResults:
                        seqNum[(row[rflvCol], row[alleleCol], row[fragmentsCol])] = 0
                logger.debug('Collected data for %d rows' % len(seqNum))
                
                triples = list(seqNum.keys())
                triples.sort(key=tripleCompare)
                logger.debug('Sorted %d rows' % len(seqNum))
                
                i = 0
                for triple in triples:
                        i = i + 1
                        seqNum[triple] = i
                logger.debug('Computed sequence numbers')
                        
                for row in self.finalResults:
                        row.append(seqNum[(row[rflvCol], row[alleleCol], row[fragmentsCol])])
                logger.debug('Appended sequence numbers')
                return

###--- globals ---###

cmds = [
        # 0. gather strains for each allele, to be collated and ordered in code
        '''select distinct a._Allele_key, s.strain
                from prb_allele_strain pas, prb_strain s, prb_allele a
                where pas._Allele_key = a._Allele_key
                and pas._Strain_key = s._Strain_key''',
                
        # 1. gather fragments and alleles for each polymorphism
        '''select v._RFLV_key, a._Allele_key, a.fragments, a.allele
                from prb_allele a, prb_rflv v
                where a._RFLV_key = v._RFLV_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ Gatherer.AUTO, '_RFLV_key', 'allele', 'fragments', 'strains', 'sequence_num', ]

# prefix for the filename of the output file
filenamePrefix = 'marker_polymorphism_allele'

# global instance of a MPAGatherer
gatherer = MPAGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
