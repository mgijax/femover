#!./python
# 
# gathers data for the 'expression_ht_sample' table in the front-end database

import Gatherer
import logger
import VocabSorter
import AgeUtils
import symbolsort
from expression_ht import samples

VocabSorter.setVocabs(90)               # EMAPA

###--- Globals ---###

expKeyCol = None                # column indexes populated by cacheColumns()
relevanceCol = None
ageMinCol = None
ageMaxCol = None
structureCol = None
tsCol = None
organismCol = None
nameCol = None

###--- Functions ---###

def cleanOrganism(organism):
        # switches ordering on organism, if a comma separates words
        # (eg- "mouse, laboratory" becomes "laboratory mouse")
        
        if not organism:
                return organism
        
        pieces = [x.strip() for x in organism.split(',')]
        if len(pieces) == 2:
                clean_organism = f"{pieces[1]} {pieces[0]}"
        else:
                clean_organism = organism

        return clean_organism
        
def cacheColumns(cols):
        # populate the global variables for which columns are where in the final results
        global expKeyCol, relevanceCol, ageMaxCol, ageMinCol, structureCol, tsCol, organismCol, nameCol
        
        expKeyCol = Gatherer.columnNumber(cols, '_Experiment_key')
        relevanceCol = Gatherer.columnNumber(cols, 'relevancy')
        ageMaxCol = Gatherer.columnNumber(cols, 'ageMax')
        ageMinCol = Gatherer.columnNumber(cols, 'ageMin')
        structureCol = Gatherer.columnNumber(cols, '_EMAPA_key')
        tsCol = Gatherer.columnNumber(cols, 'theiler_stage')
        organismCol = Gatherer.columnNumber(cols, 'organism')
        nameCol = Gatherer.columnNumber(cols, 'name')

        logger.debug('Cached 8 column indices')
        return

preferredOrganisms = {
        'mouse, laboratory' : '   mouse',       # pad with spaces to sort to top
        'laboratory mouse' : '   mouse',
        }

last = 999999999         # faux value for sorting, to ensure these come last
lastString = 'zzzzzzz'   # faux value for sorting, to ensure these come last

def sampleSortKey(a):
        # generate a key for sorting sample a by:
        #       0. experiment key, 1. relevance (yes first), 2. ageMin, 3. ageMax, 4. structure (topologically),
        #       5. Theiler stage, 6. organism (mouse then others alphabetically), 7. sample name
        # assumes: cacheColumns() was called previously

        elements = [ a[expKeyCol] ]             # 0. consider experiment key first

        # We'll only consider age, Theiler stage, and structure columns for relevant data.
        considerExtras = False

        if a[relevanceCol].lower() == 'yes':    # 1. yes first, no later
                elements.append(0)
                considerExtras = True
        else:
                elements.append(1)

        if considerExtras:
                if a[ageMinCol] == None:        # 2. age min column (None values last)
                        elements.append(last)
                else:
                        elements.append(a[ageMinCol])

                if a[ageMaxCol] == None:        # 3. age max column (None values last)
                        elements.append(last)
                else:
                        elements.append(a[ageMaxCol])

                if a[tsCol] == None:            # 4. Theiler stage column (None values last)
                        elements.append(last)
                else:
                        elements.append(int(a[tsCol]))

                if a[structureCol] == None:     # 5. structure column (None values last)
                        elements.append(last)
                else:
                        elements.append(VocabSorter.getSequenceNum(a[structureCol]))
        else:
                # Even for not relevant data, we'll need to include the right number of columns for
                # those data we're skipping.

                elements = elements + [ last, last, last, last ]        # columns 2, 3, 4, 5

        if a[organismCol] != None:              # 6. organism column
                organism = a[organismCol].lower()
                if organism in preferredOrganisms:
                        elements.append(preferredOrganisms[organism])
                else:
                        elements.append(organism)
        else:
                elements.append(lastString)

        if a[nameCol] == None:                  # 7. sample name column
                elements.append(symbolsort.splitter(lastString))
        else:
                elements.append(symbolsort.splitter(a[nameCol]))

        return tuple(elements)

###--- Classes ---###

class HTSampleGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the expression_ht_sample table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for high-throughput expression
        #       samples, collates results, writes tab-delimited text file

        def cleanOrganisms(self):
                # go through the organism column and swap the pieces of any organisms containing a comma
                # (eg- "mouse, laboratory" becomes "laboratory mouse")

                organismCol = Gatherer.columnNumber(self.finalColumns, 'organism')
                for row in self.finalResults:
                        row[organismCol] = cleanOrganism(row[organismCol])
                return
                
        def abbreviateAges(self):
                # update the age field to be its abbreviation for each sample in 'rows'
                
                ageCol = Gatherer.columnNumber(self.finalColumns, 'age')
                
                for row in self.finalResults:
                        row[ageCol] = AgeUtils.getAbbreviation(row[ageCol])

                logger.debug('Abbreviated ages for %d samples' % len(self.finalResults))
                return
        
        def applySequenceNumbers(self):
                # re-order the given sample rows and append a sequence number for each

                cacheColumns(self.finalColumns)
                self.finalResults.sort(key=sampleSortKey)
                logger.debug('Sorted %d samples' % len(self.finalResults))

                i = 0
                self.finalColumns.append('sequence_num')
                for sample in self.finalResults:
                        i = i + 1
                        sample.append(i)
                logger.debug('Applied sequence numbers')

                return samples

        def collateResults(self):
                cols, rows = self.results[0]
                
                self.finalColumns = cols
                self.finalResults = rows
                
                self.convertFinalResultsToList()
                self.cleanOrganisms()
                self.abbreviateAges()
                self.applySequenceNumbers()
                return
                
###--- globals ---###

cmds = [
        '''select t._Experiment_key, r.term as relevancy, rt.term as rnaseqType, t._Sample_key, s.name, s._Genotype_key,
                        o.commonName as organism, x.term as sex, s.age, s._Emapa_key, g.stage as theiler_stage,
                        s.ageMin, s.ageMax, s._celltype_term_key
                from %s t
                inner join gxd_htsample s on (t._Sample_key = s._Sample_key)
                inner join voc_term r on (s._Relevance_key = r._Term_key)
                inner join mgi_organism o on (s._Organism_key = o._Organism_key)
                inner join voc_term rt on (s._rnaseqType_key = rt._term_key)
                inner join voc_term x on (s._Sex_key = x._Term_key)
                left outer join gxd_theilerstage g on (s._Stage_key = g._Stage_key)
                order by t._Experiment_key, s.name''' % samples.getSampleTempTable()
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Sample_key', '_Experiment_key', 'name', 'rnaseqType', '_Genotype_key', 'organism', 'sex', 'age',
        'ageMin', 'ageMax', '_EMAPA_key', 'theiler_stage', '_celltype_term_key', 'relevancy', 'sequence_num',
        ]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_sample'

# global instance of a HTSampleGatherer
gatherer = HTSampleGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
