#!./python
# 
# gathers data for the 'strain_snp_cell' and 'strain_snp_row' tables in the front-end database

import config
import Gatherer
import logger
import dbAgnostic
import StrainUtils
import symbolsort
import KeyGenerator
import gc
import Checksum

###--- Globals ---###

SS_ROW = 'strain_snp_row'
SS_CELL = 'strain_snp_cell'

STRAIN_SEQ_NUM = {}                     # maps from strain key to sequence num
CHROMOSOME_SEQ_NUM = {}         # maps from chromosome to sequence num
STRAIN_NAME = {}                        # maps from strain key to strain name
STRAIN_ID = {}                          # maps from strain key to strain's primary ID

ROW_KEY_GENERATOR = KeyGenerator.KeyGenerator() 
CELL_KEY_GENERATOR = KeyGenerator.KeyGenerator()        

strainTempTable = StrainUtils.getStrainTempTable()

###--- Functions ---###

def dataRowKey(a):
        # get key to sort a data row, where each row is:
        #       [ ref strain key, cmp strain key, chromosome, all count, same count, diff count ]
        # assumes a and b match in column 0, so we sort on columns 1 and 2
        return (STRAIN_SEQ_NUM[a[1]], CHROMOSOME_SEQ_NUM[a[2]])

def strainRowKey(a):
        # get key for sorting strain row alphanumerically on field 0
        return symbolsort.splitter(a[0])
    
###--- Classes ---###

class StrainSnpGatherer (Gatherer.FileCacheGatherer):
        # Is: a data gatherer for the strain_snp_row and strain_snp_cell tables
        # Has: queries to execute against the source database
        # Does: queries the source database for SNP counts for strains,
        #       collates results, writes tab-delimited text file

        def collateChromosomes(self):
                # parse query 0 and populate global CHROMOSOME_SEQ_NUM
                
                global CHROMOSOME_SEQ_NUM

                cols, rows = self.results[0]
                chromCol = Gatherer.columnNumber(cols, 'chromosome')
                for row in rows:
                        CHROMOSOME_SEQ_NUM[row[chromCol]] = len(CHROMOSOME_SEQ_NUM)

                logger.debug('Collected %d chromosomes' % len(CHROMOSOME_SEQ_NUM))
                return

        def collateStrains(self):
                # parse query 1 and populate globals listed below
                
                global STRAIN_SEQ_NUM, STRAIN_NAME, STRAIN_ID
                
                toSort = []
                cols, rows = self.results[3]
                keyCol = Gatherer.columnNumber(cols, '_Strain_key')
                nameCol = Gatherer.columnNumber(cols, 'strain')
                idCol = Gatherer.columnNumber(cols, 'accID')

                for row in rows:
                        toSort.append( (row[nameCol], row[keyCol]) )
                        STRAIN_NAME[row[keyCol]] = row[nameCol]
                        STRAIN_ID[row[keyCol]] = row[idCol]

                logger.debug('Collected %d strains' % len(toSort))
                
                toSort.sort(key=strainRowKey)
                for (name, key) in toSort:
                        STRAIN_SEQ_NUM[key] = len(STRAIN_SEQ_NUM)

                logger.debug('Sorted %d strains' % len(toSort))
                return
                
        def produceOutputRows(self, refRows):
                # take the list of rows for the current reference strain and produce output rows for the
                # data files being written
                
                refRows.sort(key=dataRowKey)
                logger.debug(' - sorted %d rows' % len(refRows))

                rowCt = 0       # counter of rows added
                cellCt = 0      # counter of cells added
                
                lastCmpStrainKey = None
                for [ refStrainKey, cmpStrainKey, chromosome, allCount, sameCount, diffCount ] in refRows:
                        # When we have a new comparison strain, we need to make a new row.
                        if (cmpStrainKey != lastCmpStrainKey):
                                lastCmpStrainKey = cmpStrainKey
                                rowKey = ROW_KEY_GENERATOR.getKey( (refStrainKey, cmpStrainKey) )
                                row = [ rowKey, refStrainKey, cmpStrainKey, STRAIN_NAME[cmpStrainKey],
                                        STRAIN_ID[cmpStrainKey], STRAIN_SEQ_NUM[cmpStrainKey]
                                        ]
                                self.addRow(SS_ROW, row)
                                rowCt = rowCt + 1
                                
                        # Now add a new cell to the row
                        cellKey = CELL_KEY_GENERATOR.getKey( (refStrainKey, cmpStrainKey, chromosome) )
                        cell = [ cellKey, rowKey, chromosome, allCount, sameCount, diffCount, CHROMOSOME_SEQ_NUM[chromosome] ]
                        self.addRow(SS_CELL, cell)
                        cellCt = cellCt + 1
                        
                logger.debug(' - wrote %d cells in %d rows' % (cellCt, rowCt))
                return

        def getData(self, fromKey, toKey):
                # get a batch of data from the database and collect two sets of data from it (chromosomes and snps)

                # The strain on the database server from doing joins of strain alleles to strain alleles was
                # significantly impacting performance, as this gather ran in about 52 minutes solo, but 4.25
                # hours when run in parallel with other gatherers.  So, we're doing computations in python
                # and trying to minimize the load on the db server.
                
                cmd = '''select ref._mgdStrain_key as ref_strain_key,
                                ref._ConsensusSNP_key, cc.chromosome, ref.allele
                        from snp_consensussnp_strainallele ref, snp_coord_cache cc
                        where ref._ConsensusSNP_key = cc._ConsensusSNP_key
                                and cc._VarClass_key = 1878510
                                and ref.allele in ('A', 'C', 'G', 'T', '?')
                                and cc.isMultiCoord = 0
                                and exists (select 1 from %s t where ref._mgdStrain_key = t._Strain_key) 
                                and ref._ConsensusSNP_key >= %d
                                and ref._ConsensusSNP_key < %d''' % (strainTempTable, fromKey, toKey)
                                
                cols, rows = dbAgnostic.execute(cmd)    
                logger.debug(' - got data (%d rows)' % len(rows))
                
                strainCol = Gatherer.columnNumber(cols, 'ref_strain_key')
                snpCol = Gatherer.columnNumber(cols, '_ConsensusSnp_key')
                chromCol = Gatherer.columnNumber(cols, 'chromosome')
                alleleCol = Gatherer.columnNumber(cols, 'allele')
                
                chromosomes = {}                # consensus SNP key : chromosome
                snps = {}                               # consensus SNP key : { strain key : allele } }
                
                for row in rows:
                        snpKey = row[snpCol]
                        chromosomes[snpKey] = row[chromCol]
                        
                        if snpKey not in snps:
                                snps[snpKey] = {}
                        snps[snpKey][row[strainCol]] = row[alleleCol]
                        
                del rows
                del cols
                gc.collect()

                logger.debug(' - collated into %d SNPs' % len(snps))
                return chromosomes, snps

        def addToCounts(self, chromosomes, snps):
                # walk through the SNPs and their chromosomes, adding to the heat map counts as needed
                
                for snpKey in list(snps.keys()):
                        alleles = snps[snpKey]
                        strains = list(alleles.keys())
                        chromosome = chromosomes[snpKey]
                        
                        for refStrain in strains:
                                refAllele = alleles[refStrain]
                                for cmpStrain in strains:
                                        if refStrain != cmpStrain:
                                                # using a variable to try to save two hash lookups and speed up slightly
                                                if (refAllele != '?'):
                                                        ch = self.all[refStrain][cmpStrain]
                                                        ch[chromosome] = ch[chromosome] + 1

                                                cmpAllele = alleles[cmpStrain]
                                                
                                                if (refAllele != '?'):
                                                        if (refAllele == cmpAllele):
                                                                # using a variable to try to save two hash lookups and speed up slightly
                                                                ch = self.same[refStrain][cmpStrain]
                                                                ch[chromosome] = ch[chromosome] + 1
                                                        elif (cmpAllele != '?'):
                                                                # using a variable to try to save two hash lookups and speed up slightly
                                                                ch = self.diff[refStrain][cmpStrain]
                                                                ch[chromosome] = ch[chromosome] + 1
                        
                logger.debug(' - added to counts')
                return
        
        def produceRows(self, refStrain):
                # take the collections of snp counts and produce output rows for them
                
                chromosomes = list(CHROMOSOME_SEQ_NUM.keys())
                out = []

                for cmpStrain in list(self.all[refStrain].keys()):
                        for chromosome in chromosomes:
                                out.append([
                                        refStrain, cmpStrain, chromosome,
                                        self.all[refStrain][cmpStrain][chromosome],
                                        self.same[refStrain][cmpStrain][chromosome],
                                        self.diff[refStrain][cmpStrain][chromosome]
                                ])

                logger.debug(' - produced %d rows for %d' % (len(out), refStrain))
                return out                      

        def processBatch(self, fromKey, toKey):
                # process a batch of consensus SNPs from 'fromKey' (inclusive) up to 'toKey' (exclusive)
                
                logger.debug('Working on %d-%d' % (fromKey, toKey))

                chromosomes, snps = self.getData(fromKey, toKey)
                self.addToCounts(chromosomes, snps)
                
                del chromosomes
                del snps
                gc.collect()
                return
                                
        def initialize(self):
                # initialize the snp count collections self.all, self.same, and self.diff
                
                self.all = {}           # ref strain : { cmp strain : { chromosome : count of all snps } }
                self.same = {}          # ref strain : { cmp strain : { chromosome : count of snps with same allele calls } }
                self.diff = {}          # ref strain : { cmp strain : { chromosome : count of snps with different allele calls } }

                for refStrain in STRAIN_SEQ_NUM:
                        self.all[refStrain] = {}
                        self.same[refStrain] = {}
                        self.diff[refStrain] = {}

                        for cmpStrain in STRAIN_SEQ_NUM:
                                if refStrain != cmpStrain:
                                        self.all[refStrain][cmpStrain] = {}
                                        self.same[refStrain][cmpStrain] = {}
                                        self.diff[refStrain][cmpStrain] = {}

                                        for chromosome in CHROMOSOME_SEQ_NUM:
                                                self.all[refStrain][cmpStrain][chromosome] = 0
                                                self.same[refStrain][cmpStrain][chromosome] = 0
                                                self.diff[refStrain][cmpStrain][chromosome] = 0

                logger.debug('Initialized collections')
                return
        
        def collateResults(self):
                self.collateChromosomes()
                self.collateStrains()

                maxSnpKey = int(self.results[-1][1][0][0])
                logger.debug('Found max consensus SNP key = %d' % maxSnpKey)

                self.initialize()

                chunkSize = 175000
                startKey = 1
                while startKey <= maxSnpKey:
                        endKey = startKey + chunkSize
                        self.processBatch(startKey, endKey)
                        startKey = endKey
                        
                logger.debug('Producing output')

                strainKeys = list(self.all.keys())
                strainKeys.sort()
                
                for refStrain in strainKeys:
                        self.produceOutputRows(self.produceRows(refStrain))
                        
                logger.debug('Finished processing strains')
                return

###--- globals ---###

cmds = [
        # 0. get unique set of chromosomes having SNPs
        '''select c.chromosome, c.sequenceNum
                from mrk_chromosome c
                where c._Organism_key = 1
                        and exists (select 1 from snp_coord_cache cc where c.chromosome = cc.chromosome)
                order by c.sequenceNum''',
        
        # 1. strains with SNPs
        '''select distinct _mgdStrain_key into temporary table uniqueSnpStrains from snp_consensussnp_strainallele''',
        
        # 2. index it
        '''create unique index mt1 on uniqueSnpStrains (_mgdStrain_key)''' ,

        # 3. get unique set of strains with SNPs
        '''select p._Strain_key, p.strain, a.accID
                from prb_strain p, %s t, acc_accession a
                where p._Strain_key = t._Strain_key
                and p._Strain_key = a._Object_key
                and a._MGIType_key = 10
                and a._LogicalDB_key = 1
                and a.preferred = 1
                and exists (select 1 from uniqueSnpStrains s
                        where t._Strain_key = s._mgdStrain_key)
                order by p.strain''' % strainTempTable,
                
        # 4. get the max consensus SNP key
        'select max(_ConsensusSnp_key) as maxKey from snp_consensussnp',
        ]

# definitions for output files to be produced
files = [
        (SS_ROW,
                [ 'row_key', 'strain_key', 'comparison_strain_key', 'comparison_strain_name', 'comparison_strain_id', 'sequence_num' ],
                [ 'row_key', 'strain_key', 'comparison_strain_key', 'comparison_strain_name', 'comparison_strain_id', 'sequence_num' ]
                ),
        (SS_CELL,
                [ 'cell_key', 'row_key', 'chromosome', 'all_count', 'same_count', 'different_count', 'sequence_num' ],
                [ 'cell_key', 'row_key', 'chromosome', 'all_count', 'same_count', 'different_count', 'sequence_num' ]
                ),
        ]

# checksums for this gatherer
checksums = [
        Checksum.Checksum('strain_snp.accession', config.CACHE_DIR,
                Checksum.singleCount('select count(1) from snp_accession') ),
        Checksum.Checksum('strain_snp.consensus_snp', config.CACHE_DIR,
                Checksum.singleCount('select count(1) from snp_consensussnp') ),
        Checksum.Checksum('strain_snp.coord_cache', config.CACHE_DIR,
                Checksum.singleCount('select count(1) from snp_coord_cache') ),
        ]

# global instance of a StrainSnpGatherer
gatherer = StrainSnpGatherer (files, cmds)
gatherer.addChecksums(checksums)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
