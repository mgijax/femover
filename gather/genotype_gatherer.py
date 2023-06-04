#!./python
# 
# gathers data for the 'genotype' table in the front-end database
#
# 10/24/2013    lec
#       - TR11423/added 'exists_as' term to 'genotype' table
#

import Gatherer
import logger
import string
import TagConverter
import GenotypeClassifier
import StrainUtils

###--- Functions ---###

def genCellLineDict (rows):
        
        cellLineDict = {}
        results = []

        return TYPES[displayType]


###--- Classes ---###

class GenotypeGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the genotype table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for genotypes,
        #       collates results, writes tab-delimited text file

        def collateResults (self):

                #
                # final query has the basic genotype data; we will start there
                # and augment it
                #
                self.finalColumns = self.results[-1][0]
                self.finalResults = self.results[-1][1]
                self.convertFinalResultsToList()

                #
                # store which genotypes have primary images
                #
                cols, rows = self.results[3]
                genotypeCol = Gatherer.columnNumber (cols, '_Object_key')
                hasImage = {}

                for row in rows:
                        hasImage[row[genotypeCol]] = 1

                logger.debug ('Found %d genotypes with images' % \
                        len(hasImage)) 

                #
                # The two earlier queries get the allele composition strings
                # that need to be added for each genotype
                #
                combos = [ (0, 'combo1'), (1, 'combo2'), (2, 'combo3') ]

                for (index, id) in combos:
                        dict = {}               # genotype key -> note
                        (cols, rows) = self.results[index]
                        keyCol = Gatherer.columnNumber (cols, '_Genotype_key')
                        noteCol = Gatherer.columnNumber (cols, 'note')

                        for row in rows:
                                key = row[keyCol]
                                if key in dict:
                                        dict[key] = dict[key] + row[noteCol]
                                else:
                                        dict[key] = row[noteCol]

                        logger.debug ('Found %d %s notes' % (len(dict), id))

                        keyCol = Gatherer.columnNumber (self.finalColumns,
                                        '_Genotype_key')
                        for row in self.finalResults:
                                key = row[keyCol]
                                if key in dict:
                                        combo = TagConverter.convert (
                                                dict[key],superscript=False)
                                else:
                                        combo = None

                                self.addColumn (id, combo, row,
                                        self.finalColumns)

                        logger.debug ('Mapped %d %s notes' % (len(dict), id))

                #
                # store which genotypes have cell line data
                #
                genoUniqueCellLines = {}
                genoCellLineStrings = {}
                cols, rows = self.results[4]
                keyCol = Gatherer.columnNumber (cols, '_genotype_key')
                cellLineCol = Gatherer.columnNumber (cols, 'cellline')
                for row in rows:
                        key = row[keyCol]
                        if key in genoUniqueCellLines:
                                cellLine = row[cellLineCol]
                                uniqueCellLines = genoUniqueCellLines[key]
                                if cellLine not in uniqueCellLines:
                                        uniqueCellLines.append(cellLine)
                                genoUniqueCellLines[key] = uniqueCellLines
                        else:
                                uniqueCellLines = []
                                uniqueCellLines.append(row[cellLineCol])
                                genoUniqueCellLines[key] = uniqueCellLines


                cols, rows = self.results[5]
                keyCol = Gatherer.columnNumber (cols, '_genotype_key')
                cellLineCol = Gatherer.columnNumber (cols, 'cellline')
                for row in rows:
                        key = row[keyCol]
                        if key in genoUniqueCellLines:
                                cellLine = row[cellLineCol]
                                uniqueCellLines = genoUniqueCellLines[key]
                                if cellLine not in uniqueCellLines:
                                        uniqueCellLines.append(cellLine)
                                genoUniqueCellLines[key] = uniqueCellLines
                        else:
                                uniqueCellLines = []
                                uniqueCellLines.append(row[cellLineCol])
                                genoUniqueCellLines[key] = uniqueCellLines

                for key in genoUniqueCellLines:
                        genoCellLineStrings[key] = ", ".join(genoUniqueCellLines[key]) 


                logger.debug ('Mapping %d genotypes to celllines' % len(genoCellLineStrings))

                #
                # add final flags and data columns to final result set
                #
                keyCol = Gatherer.columnNumber (self.finalColumns,
                        '_Genotype_key')
                for row in self.finalResults:
                        key = row[keyCol]

                        if key in hasImage:
                                image = 1
                        else:
                                image = 0

                        disease = GenotypeClassifier.isDiseaseModel(key)
                        pheno = GenotypeClassifier.hasPhenoData(key)
                        
                        cellLine = ''
                        if key in genoCellLineStrings:
                          cellLine = genoCellLineStrings[key]
                        
                        self.addColumn ('hasImage', image, row,
                                self.finalColumns)
                        self.addColumn ('hasPhenoData', pheno, row,
                                self.finalColumns)
                        self.addColumn ('hasDiseaseModel', disease, row,
                                self.finalColumns)
                        self.addColumn ('classification',
                                GenotypeClassifier.getClass (key),
                                row, self.finalColumns) 
                        self.addColumn ('cell_lines',
                                cellLine, row, self.finalColumns) 
                return

###--- globals ---###

noteCmd = '''select mn._Object_key as _Genotype_key,
                        mn.note
                from mgi_note mn,
                        mgi_notetype mnt
                where mn._MGIType_key = 12
                        and mn._NoteType_key = mnt._NoteType_key
                        and mnt.noteType = '%s'
                order by mn._Object_key'''
cmds = [

        # 0. "Combination Type 1" notes for use in sorting by allele composition
        noteCmd % 'Combination Type 1',

        # 1. "Combination Type 2" notes for use on allele detail page
        noteCmd % 'Combination Type 2',

        # 2. "Combination Type 3" notes for use on allele summary page
        noteCmd % 'Combination Type 3',

        # 3. get which genotypes have primary images associated
        '''select distinct _Object_key
                from img_imagepane_assoc
                where _MGIType_key = 12
                and isPrimary = 1''',

        # 4. cell_line_1 for genotype
        '''select gap._genotype_key, acl.cellline 
                from gxd_allelepair gap, all_cellline acl
                where _mutantcellline_key_1 IS NOT NULL
                and gap._mutantcellline_key_1 = acl._cellline_key''',

        # 5. cell_line_1 for genotype
        '''select gap._genotype_key, acl.cellline 
                from gxd_allelepair gap, all_cellline acl
                where _mutantcellline_key_2 IS NOT NULL
                and gap._mutantcellline_key_2 = acl._cellline_key''',

        # 6. assumes that a genotype has only one ID, that it is from MGI, and
        # that it is both preferred and non-private
        '''select distinct g._Genotype_key, s.strain, a.accID, g.note,
                        g.isConditional, t.term, sid.strain_id
                from gxd_genotype g
                inner join acc_accession a on (g._Genotype_key = a._Object_key
                        and a._MGIType_key = 12
                        and a._LogicalDB_key = 1
                        and a.preferred = 1)
                inner join prb_strain s on (g._Strain_key = s._Strain_key)
                inner join voc_term t on (g._ExistsAs_key = t._term_key)
                left outer join %s sid on (g._Strain_key = sid._Strain_key)''' % StrainUtils.getStrainIDTempTable(),
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Genotype_key', 'strain', 'strain_id', 'accID', 'isConditional', 'note',
        'combo1', 'combo2', 'combo3', 'hasImage', 'hasPhenoData', 'hasDiseaseModel',
        'classification', 'cell_lines', 'term' ]

# prefix for the filename of the output file
filenamePrefix = 'genotype'

# global instance of a GenotypeGatherer
gatherer = GenotypeGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
