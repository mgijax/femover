#!./python
# 
# gathers data for the 'antigen' table in the front-end database

import Gatherer
import utils

###--- Classes ---###

class AntigenGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the antigen table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for antigens,
        #       collates results, writes tab-delimited text file

        def postprocessResults (self):
                # does customization of organism names

                self.convertFinalResultsToList()
                orgCol = Gatherer.columnNumber(self.finalColumns, 'species')

                for row in self.finalResults:
                        row[orgCol] = utils.cleanupOrganism(row[orgCol])
                return

###--- globals ---###

cmds = [
        '''select av._Antigen_key,
                av.antigenName,
                av.regionCovered,
                av.antigenNote, 
                av.modification_date,
                aa.accID as mgiID, 
                s.age, 
                s.name as library, 
                s.description, 
                vt1.term as sex, 
                vt2.term as cellLine, 
                ps.strain, 
                pt.tissue, 
                mgio.commonName as species
        from gxd_antigen av,
                acc_accession aa,
                prb_source s,
                voc_term vt1,
                voc_term vt2,
                prb_strain ps, 
                prb_tissue pt, 
                mgi_organism mgio 
        where av._Antigen_key = aa._Object_key
                and aa._MGIType_key = 7
                and aa._LogicalDB_key = 1
                and aa.prefixPart = 'MGI:'
                and aa.preferred = 1
                and av._Source_key = s._Source_key
                and s._Gender_key = vt1._Term_key
                and s._CellLine_key = vt2._Term_key
                and s._Strain_key = ps._Strain_key
                and s._Tissue_key = pt._Tissue_key
                and s._Organism_key = mgio._Organism_key''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Antigen_key', 'mgiID', 'antigenName', 'species', 'strain', 'sex',
        'age', 'tissue', 'cellLine', 'description', 'regionCovered',
        'antigenNote'
        ]

# prefix for the filename of the output file
filenamePrefix = 'antigen'

# global instance of a AntigenGatherer
gatherer = AntigenGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
