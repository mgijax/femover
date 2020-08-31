#!./python
# 
# gathers data for the 'expression_ht_experiment' table in the front-end db

import Gatherer
import logger
from VocabUtils import getTerm
from expression_ht import experiments
from expression_ht import samples

###--- Classes ---###

class HTExperimentGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the expression_ht_experiment table
        # Has: queries to execute against the source database
        # Does: queries the source database for data for high-throughput
        #       expression experiments, collates results, writes tab-delimited
        #       text file
        
        def collateResults(self):
                cols, rows = experiments.getExperimentIDs(True)
                experimentKeyCol = Gatherer.columnNumber(cols, '_Experiment_key')
                idCol = Gatherer.columnNumber(cols, 'accID')

                primaryIDs = {}
                for row in rows:
                        primaryIDs[row[experimentKeyCol]] = row[idCol]

                sampleCounts = samples.getSampleCountsByExperiment()

                cols, rows = self.results[0]
                
                experimentKeyCol = Gatherer.columnNumber(cols, '_Experiment_key')
                sourceKeyCol = Gatherer.columnNumber(cols, '_Source_key')
                nameCol = Gatherer.columnNumber(cols, 'name')
                descriptionCol = Gatherer.columnNumber(cols, 'description')
                studyTypeKeyCol = Gatherer.columnNumber(cols, '_StudyType_key')
                methodKeyCol = Gatherer.columnNumber(cols, '_ExperimentType_key')
                isInAtlasCol = Gatherer.columnNumber(cols, 'is_in_atlas')

                self.finalColumns = fieldOrder
                self.finalResults = []

                for row in rows:
                        experimentKey = row[experimentKeyCol]

                        primaryID = None
                        if experimentKey in primaryIDs:
                                primaryID = primaryIDs[experimentKey]
                                
                        studyType = getTerm(row[studyTypeKeyCol])
                        method = getTerm(row[methodKeyCol])

                        self.finalResults.append ([ experimentKey, primaryID, getTerm(row[sourceKeyCol]),
                                row[nameCol], row[descriptionCol], studyType, method, sampleCounts[experimentKey], row[isInAtlasCol] ])

                logger.debug ('Compiled %d data rows' % len(self.finalResults))
                return
                        
###--- globals ---###

cmds = [
        # 0. basic experiment data
        '''select e._Experiment_key, e._Source_key, e.name, e.description, e._StudyType_key, e._ExperimentType_key, t.is_in_atlas
                from %s t, gxd_htexperiment e
                where t._Experiment_key = e._Experiment_key''' % experiments.getExperimentTempTable()
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Experiment_key', 'accID', 'source', 'name', 'description', 'study_type', 'method', 'sample_count', 'is_in_atlas'
        ]

# prefix for the filename of the output file
filenamePrefix = 'expression_ht_experiment'

# global instance of a HTExperimentGatherer
gatherer = HTExperimentGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
