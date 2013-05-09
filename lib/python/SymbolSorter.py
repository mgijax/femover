# Module: GenotypeClassifier.py
# Purpose: to provide an easy means to determine the classification (or type)
#	for a genotype (eg- conditional, hemizygous, etc.)

import dbAgnostic
import symbolsort
import logger

###--- Globals ---###

GENE_SYMBOL_SEQ_MAP=None
def _initGeneSymbols():
	global GENE_SYMBOL_SEQ_MAP
	logger.debug("initializing gene symbol sequence map")
	gsQuery = """
		select symbol from mrk_marker where _organism_key=1
	"""
	cols,rows = dbAgnostic.execute(gsQuery)
	symbols = [x[0] for x in rows]
	symbols.sort(lambda a,b : symbolsort.nomenCompare(a, b))

	GENE_SYMBOL_SEQ_MAP={}
	count = 0
	for symbol in symbols:
		count += 1
		GENE_SYMBOL_SEQ_MAP[symbol] = count
	logger.debug("done initializing gene symbol sequence map")

###--- Access Functions ---###
def getGeneSymbolSeq(symbol):
	global GENE_SYMBOL_SEQ_MAP
	if not GENE_SYMBOL_SEQ_MAP:
		_initGeneSymbols()
	return symbol in GENE_SYMBOL_SEQ_MAP and GENE_SYMBOL_SEQ_MAP[symbol] or len(GENE_SYMBOL_SEQ_MAP)+1

if __name__=="__main__":
	import unittest
	class SymbolSortTestCase(unittest.TestCase):
		def test_gene_symbols(self):
			symbolsToTest = [
			("Zfp11","Zfp100"),
			("Bcan","Btg2"),
			("Cbx1","Cdk11b"),
			]
			for s1,s2 in symbolsToTest:
				s1_seq = getGeneSymbolSeq(s1)
				s2_seq = getGeneSymbolSeq(s2)
				assert s1_seq < s2_seq, "%s seq(%s) not < %s seq(%s)"%(s1,s1_seq,s2,s2_seq)
	unittest.main()
