#!./python
# 
# gathers data for the 'antibody' table in the front-end database

import Gatherer
import utils

###--- Classes ---###

class AntibodyGatherer (Gatherer.Gatherer):
        # Is: a data gatherer for the antibody table
        # Has: queries to execute against the source database
        # Does: queries the source database for primary data for antibodies,
        #       collates results, writes tab-delimited text file

        def collateResults(self):
                # Purpose: map our query results into the final result set,
                # which is to be written out to the output file

                self.finalColumns = self.results[-1][0]
                self.finalResults = self.results[-1][1]
                return

        def postprocessResults (self):
                # does customization of organism names

                self.convertFinalResultsToList()
                orgCol = Gatherer.columnNumber(self.finalColumns,
                        'antibodySpecies')

                for row in self.finalResults:
                        row[orgCol] = utils.cleanupOrganism(row[orgCol])
                return


###--- globals ---###

cmds = [
        # 0. collect the counts of expression results
        '''select gp._Antibody_key, count(distinct _Expression_key) as gxdCount
        into temporary table gxdResults
        from gxd_antibodyprep gp, gxd_assay ga, gxd_expression ge
        where gp._AntibodyPrep_key = ga._AntibodyPrep_key 
                and ga._Assay_Key = ge._Assay_key 
                and ge.isForGXD = 1 
        group by 1
        ''',

        # 1. index the table of expression result counts
        'create index gxdResults2 on gxdResults(_Antibody_key)',

        # 2. gather the aliases for each antibody as a comma-delimited string
        '''select _Antibody_key, array_to_string(array_agg(alias), ', ') as aliases
        into temporary table antibody_aliases
        from gxd_antibodyalias
        group by _Antibody_key
        order by _Antibody_key
        ''',

        # 3. index the table of aliases
        'create index antibody_aliases2 on antibody_aliases(_Antibody_key)',

        # 4. retrieve the collected set of antibody data; expression count
        # should default to 0 if outer join has no match
        '''select ab._Antibody_key,
                ab.antibodyName,
                coalesce(f.gxdCount, 0) as gxdCount,
                aa.accID as mgiID,
                ae.commonName as antibodySpecies,
                ac.term as class,
                ap.antibodyType,
                ab.antibodyNote,
                ab._Antigen_key,
                b.aliases
        from gxd_antibody ab
        inner join acc_accession aa on (
                ab._Antibody_key = aa._Object_key
                and aa._MGIType_key = 6
                and aa._LogicalDB_key = 1
                and aa.prefixPart = 'MGI:'
                and aa.preferred = 1)
        inner join mgi_organism ae on (ab._Organism_key = ae._Organism_key)
        inner join voc_term ac on (
                ab._AntibodyClass_key = ac._Term_key)
        inner join gxd_antibodytype ap on (
                ab._AntibodyType_key = ap._AntibodyType_key)
        left outer join gxdResults f on (f._Antibody_key = ab._Antibody_key)
        left outer join antibody_aliases b on (
                ab._Antibody_key = b._Antibody_key)
        ''',
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [
        '_Antibody_key', 'antibodyName', 'mgiID', 'antibodySpecies',
        'antibodyType', 'class', 'gxdCount', '_Antigen_key', 'aliases',
        'antibodyNote'
        ]

# prefix for the filename of the output file
filenamePrefix = 'antibody'

# global instance of a AntibodyGatherer
gatherer = AntibodyGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
