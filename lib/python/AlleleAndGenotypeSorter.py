#!/usr/local/bin/python
# 
# Name: AlleleAndGenotypeSorter.py
# Purpose: provides an easy mechanism for determining ordering of genotypes
#       and alleles

import dbAgnostic
import symbolsort
import logger

###--- Globals ---###

ALLELE_SEQ_NUM = None           # dictionary: allele key -> sequence num
GENOTYPE_SEQ_NUM = None         # dictionary: genotype key -> sequence num

###--- Private Functions ---###

def __initialize():
        # initialize this module by loading data from the database, sorting
        # it, and caching sequence numbers for alleles and genotypes

        global GENOTYPE_SEQ_NUM, ALLELE_SEQ_NUM

        if (GENOTYPE_SEQ_NUM != None) or (ALLELE_SEQ_NUM != None):
                return

        __sortAlleles()
        __sortGenotypes()
        return

def __alleleSortKey(a):
        # return a sort key for (allele symbol, allele key) tuple
        return symbolsort.splitter(a[0])

def __sortAlleles():
        # load allele nomenclature from the database, sort the alleles, and
        # cache the sequence number for each

        global ALLELE_SEQ_NUM

        # get symbols for all alleles,  so we can sort them
        cmd = 'select a._Allele_key, a.symbol from all_allele a'

        (cols, rows) = dbAgnostic.execute(cmd)

        keyCol = dbAgnostic.columnNumber(cols, '_Allele_key')
        symbolCol = dbAgnostic.columnNumber(cols, 'symbol')

        toSort = []

        for row in rows:
                toSort.append ( (row[symbolCol].lower(), row[keyCol]) )

        toSort.sort (key=__alleleSortKey)

        ALLELE_SEQ_NUM = {}
        i = 0

        for (symbol, key) in toSort:
                i = i + 1
                ALLELE_SEQ_NUM[key] = i

        ALLELE_SEQ_NUM[None] = 0                # null alleles come first

        logger.debug ('AlleleAndGenotypeSorter: Sorted %d alleles' % i)
        return

def __genotypeSortKey(a):
        # return a sort key for tuple (genotype key, [ allele sequence numbers ] )
        return a[1]

def __sortGenotypes():
        # load genotypes and their alleles from the database, sort the
        # genotypes based on their alleles' nomenclature, and cache the
        # sequence number for each

        # Assumes:  _sortAlleles() has been previously called to sort alleles

        global GENOTYPE_SEQ_NUM

        # We first need to sort genotypes which have allele pairs

        # get all allele pairs for all genotypes
        cmd = '''select _Genotype_key,
                        _Allele_key_1, 
                        _Allele_key_2, 
                        sequenceNum
                from gxd_allelepair
                order by _Genotype_key, sequenceNum'''

        (cols, rows) = dbAgnostic.execute(cmd)

        genotypeCol = dbAgnostic.columnNumber(cols, '_Genotype_key')
        allele1Col = dbAgnostic.columnNumber(cols, '_Allele_key_1')
        allele2Col = dbAgnostic.columnNumber(cols, '_Allele_key_2')
        seqNumCol = dbAgnostic.columnNumber(cols, 'sequenceNum')

        # genotype key -> [ allele key 1, ... , allele key n ]
        genotypes = {}

        for row in rows:
                genotypeKey = row[genotypeCol]
                allele1 = row[allele1Col]
                allele2 = row[allele2Col]

                if genotypeKey not in genotypes:
                        genotypes[genotypeKey] = []

                genotypes[genotypeKey].append(getAlleleSequenceNum(allele1))
                genotypes[genotypeKey].append(getAlleleSequenceNum(allele2))

        genotypeList = list(genotypes.items())
        genotypeList.sort(key=__genotypeSortKey)

        GENOTYPE_SEQ_NUM = {}
        i = 0
        for (key, alleleList) in genotypeList:
                i = i + 1
                GENOTYPE_SEQ_NUM[key] = i

        logger.debug ('Sorted %d genotypes with alleles' % \
                len(genotypeList))

        # get all genotypes which do not have allele pairs, so we can order
        # them, too
        cmd = '''select g._Genotype_key
                from gxd_genotype g
                where not exists (select 1 from gxd_allelepair p
                        where g._Genotype_key = p._Genotype_key)
                order by 1'''

        (cols, rows) = dbAgnostic.execute(cmd)

        for row in rows:
                i = i + 1
                GENOTYPE_SEQ_NUM[row[0]] = i

        logger.debug ('Added %d genotypes without alleles' % len(rows))
        return

###--- Public Functions ---###

def getAlleleSequenceNum (alleleKey):
        __initialize()

        if alleleKey in ALLELE_SEQ_NUM:
                return ALLELE_SEQ_NUM[alleleKey]
        return len(ALLELE_SEQ_NUM) + 1

def getGenotypeSequenceNum (genotypeKey):
        __initialize()
        
        if genotypeKey in GENOTYPE_SEQ_NUM:
                return GENOTYPE_SEQ_NUM[genotypeKey]
        return len(GENOTYPE_SEQ_NUM) + 1

