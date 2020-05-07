#!./python
# 
# gathers data for the 'referenceSequenceNum' table in the front-end database

import Gatherer
import logger
import utils

###--- Classes ---###

class ReferenceSequenceNumGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the referenceSequenceNum table
        # Has: queries to execute against the source database
        # Does: queries source db for sorting data for references,
        #       collates results, writes tab-delimited text file

        def collateResults (self):
                data = {}
                cols, rows = self.results[0]
                
                refsKeyCol = Gatherer.columnNumber (cols, '_Refs_key')
                authorsCol = Gatherer.columnNumber (cols, 'authors')
                titleCol = Gatherer.columnNumber (cols, 'title')
                yearCol = Gatherer.columnNumber (cols, 'year')
                numericPartCol = Gatherer.columnNumber (cols, 'numericPart')

                byDate = []             # lists of (sortable value, reference key) for sorting by various fields
                byAuthor = []
                byTitle = []
                for row in rows:
                        referenceKey = row[refsKeyCol]
                        data[referenceKey] = { '_Refs_key' : referenceKey }

                        author = None
                        if row[authorsCol]:
                                author = row[authorsCol].lower()

                        title = None
                        if row[titleCol]:
                                title = row[titleCol].lower()

                        numericPart = 0
                        if row[numericPartCol] != None:
                                numericPart = int(row[numericPartCol])
                        
                        byDate.append ( (int(row[yearCol]), numericPart, referenceKey) )
                        byAuthor.append ( (author, referenceKey) )
                        byTitle.append ( (title, referenceKey) )
                        data[referenceKey] = {
                                '_Refs_key' : referenceKey,
                                'byPrimaryID' : numericPart
                                }

                logger.debug ('Pulled out data to sort')

                byDate.sort()
                byAuthor.sort(key=lambda x: utils.stringSortKey(x[0], True))
                byTitle.sort(key=lambda x: utils.stringSortKey(x[0], True))

                logger.debug ('Sorted data')

                for (lst, field) in [ (byDate, 'byDate'), (byAuthor, 'byAuthor'), (byTitle, 'byTitle') ]:
                        i = 1
                        for t in lst:
                                referenceKey = t[-1]
                                data[referenceKey][field] = i
                                i = i + 1

                self.finalColumns = [ '_Refs_key', 'byDate', 'byAuthor', 'byPrimaryID', 'byTitle' ]
                self.finalResults = []
                
                refKeys = list(data.keys())
                refKeys.sort()
                
                for key in refKeys:
                        refDict = data[key]
                        row = [ refDict['_Refs_key'],
                                refDict['byDate'],
                                refDict['byAuthor'],
                                refDict['byPrimaryID'],
                                refDict['byTitle'],
                                ]
                        self.finalResults.append (row)
                return

###--- globals ---###

cmds = [
        '''select m._Refs_key, m.authors, m.year, c.numericPart, c.jnumID, m.title
        from bib_refs m, bib_citation_cache c
        where m._Refs_key = c._Refs_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Refs_key', 'byDate', 'byAuthor', 'byPrimaryID', 'byTitle'
        ]

# prefix for the filename of the output file
filenamePrefix = 'reference_sequence_num'

# global instance of a ReferenceSequenceNumGatherer
gatherer = ReferenceSequenceNumGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
