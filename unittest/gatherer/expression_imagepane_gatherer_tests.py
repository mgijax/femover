"""
Run expression_imagepane_gatherer test suites
"""
import sys,os.path
# adjust the path for running the tests locally, so that it can find gatherer (i.e. 2 dirs up)
sys.path.append(os.path.join(os.path.dirname(__file__), '../../gather'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../../lib/python'))

import unittest

import GXDUtils
import SymbolSorter
import expression_imagepane_gatherer
gatherer = expression_imagepane_gatherer.gatherer

### Constants ###

# assaytype keys
IMMUNO_TYPE = 6
RNA_TYPE = 1
KNOCKIN_TYPE = 9
NORTHERN_TYPE = 2
WESTERN_TYPE = 8
RTPCR_TYPE = 5
RNASE_TYPE = 4
NUCLEASE_TYPE = 3

### helper functions ###
def sortAssays(assays):
        """
        register assays and have gatherer generate sequence maps
                by _imagepane_key
        """
        for assay in assays:
                gatherer.registerImagePaneSortFields(
                        paneKey=assay[0],
                        assayTypeKey=assay[1],
                        markerSymbol=assay[2],
                        citation=assay[3],
                        hybridization=assay[4]
                )
        gatherer.sortImagePaneFields()
        
def getPaneKeysByAssayType():
    """
    return list of _imagepane_keys
            from gatherer.byAssayTypeSeqMap
    """
    return _getPaneKeysBySeqMap(gatherer.byAssayTypeSeqMap)

def getPaneKeysByMarker():
    """
    return list of _imagepane_keys
            from gatherer.byMarkerSeqMap
    """
    return _getPaneKeysBySeqMap(gatherer.byMarkerSeqMap)

def getPaneKeysByHybridizationAsc():
    """
    return list of _imagepane_keys
            from gatherer.byHybridizationAscSeqMap
    """
    return _getPaneKeysBySeqMap(gatherer.byHybridizationAscSeqMap)
    
def getPaneKeysByHybridizationDesc():
    """
    return list of _imagepane_keys
            from gatherer.byHybridizationDescSeqMap
    """
    return _getPaneKeysBySeqMap(gatherer.byHybridizationDescSeqMap)
    
def _getPaneKeysBySeqMap(seqMap):
    """
    return sorted list of keys from seqMap
            where seqMap defines the order
    """
    l = list(seqMap.keys())
    l.sort(key=lambda k: seqMap[k])
    
    return l
    

def initMockAssayTypeSorts():
    """
    Inject assay type sorts that we expect from the database
    """
    GXDUtils.ASSAY_TYPE_SEQMAP = {
        IMMUNO_TYPE: 1,
        RNA_TYPE: 2, 
        KNOCKIN_TYPE: 3,
        NORTHERN_TYPE: 4, 
        WESTERN_TYPE: 5, 
        RTPCR_TYPE: 6, 
        RNASE_TYPE: 7, 
        NUCLEASE_TYPE: 8 
    }
    
    
### Test cases ###


class SortByAssayTypeTestCase(unittest.TestCase):
    """
    Test the sorting of image panes by assay type for the GXD image pane summary
    has 3 levels, assay type, gene, then citation
    """
    
    def setUp(self):
        gatherer.initSeqMaps()
        
        initMockAssayTypeSorts()
        
        # Inject test markers into SymbolSorter
        SymbolSorter.GENE_SYMBOL_SEQ_MAP = {
                'pax1': 1,
                'pax2': 2,
                'pax10': 10
        }
        
    
    ### test sort by assay type ###
    def test_sortPaneByAssayType(self):
        """
        Sort by assay type
        goes
                immuno, rna in situ, in situ knockin,
                norther blot, western blot, rt-pcr,
                rnase protection, nuclease s1
                        
        """
        # register assay types in reverse order
        assays = [
                [1, NUCLEASE_TYPE, 'pax10','A citation','whole mount'],
                [2, RNASE_TYPE, 'pax10','A citation','whole mount'],
                [3, RTPCR_TYPE, 'pax10','A citation','whole mount'],
                [4, WESTERN_TYPE, 'pax10','A citation','whole mount'],
                [5, NORTHERN_TYPE, 'pax10','A citation','whole mount'],
                [6, KNOCKIN_TYPE, 'pax10','A citation','whole mount'],
                [7, RNA_TYPE, 'pax10','A citation','whole mount'],
                [8, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
        ]
        
        sortAssays(assays)
        
        sortedKeys = getPaneKeysByAssayType()
        self.assertEqual([8,7,6,5,4,3,2,1], sortedKeys)
        
        
    def test_sortWithinPaneByAssayType(self):
        """
        Sort by assay type within same pane
        Should pick the best assay type for each pane
                        
        """
        # nuclease is last, immuno and rna both sort above
        assays = [
                [1, NUCLEASE_TYPE, 'pax10','A citation','whole mount'],
                [1, RNA_TYPE, 'pax10','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [2, NUCLEASE_TYPE, 'pax10','A citation','whole mount'],
        ]
        
        sortAssays(assays)
        
        sortedKeys = getPaneKeysByAssayType()
        self.assertEqual([2,1], sortedKeys)
            
    def test_sortPaneByBestMarker(self):
        """
        If assay type is the same, use best marker for each pane
                            
        """
        # pax1 and pax2 should both beat pax10 within each pane
        assays = [
                [1, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [1, IMMUNO_TYPE, 'pax2','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
        ]
            
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByAssayType()
        self.assertEqual([2,1], sortedKeys)
        
        
    def test_sortPaneByCitation(self):
        """
        If assay type is the same and marker is same
                sort by citation for each image pane
                            
        """
        # pax1 and pax2 should both beat pax10 within each pane
        assays = [
                [1, IMMUNO_TYPE, 'pax1','B citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
            
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByAssayType()
        self.assertEqual([2,1], sortedKeys)
        
        
        
class SortByMarkerTestCase(unittest.TestCase):
    """
    Test the sorting of image panes by gene symbol for the GXD image pane summary
    has 3 levels, gene, assay type, then citation
    """
    
    def setUp(self):
        gatherer.initSeqMaps()
        
        initMockAssayTypeSorts()
            
        # Inject test markers into SymbolSorter
        SymbolSorter.GENE_SYMBOL_SEQ_MAP = {
                'pax1': 1,
                'pax2': 2,
                'pax10': 10
        }
            
    
    ### test sort by gene ###
    def test_sortPaneByGene(self):
        """
        Sort by gene symbol
        """
        # register genes in reverse order
        assays = [
                [1, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax2','A citation','whole mount'],
                [3, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByMarker()
        self.assertEqual([3,2,1], sortedKeys)
        
        
    def test_sortWithinPaneByGene(self):
        """
        Sort by gene symbol for multiple genes in the same pane
        Should select best marker for each pane
        """
        # pax1 and pax2 sort above pax10
        assays = [
                [1, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [1, IMMUNO_TYPE, 'pax2','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByMarker()
        self.assertEqual([2,1], sortedKeys)
        
        
    def test_sortPaneByAssayType(self):
        """
        Secondary sort for same marker is to sort by best
                assay type within each pane
        """
        # rna insitu and immuno sort above nuclease
        assays = [
                [1, NUCLEASE_TYPE, 'pax1','A citation','whole mount'],
                [1, RNA_TYPE, 'pax1','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
                [2, NUCLEASE_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByMarker()
        self.assertEqual([2,1], sortedKeys)
        
    def test_sortPaneByCitation(self):
        """
        Use citation if marker and assay type are the same
        """
        # rna insitu and immuno sort above nuclease
        assays = [
                [1, IMMUNO_TYPE, 'pax1','B citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByMarker()
        self.assertEqual([2,1], sortedKeys)


class SortByHybridizationTestCase(unittest.TestCase):
    """
    Test the sorting of image panes by hybridization for the GXD image pane summary
    has 3 levels, hybridization, assay type, gene symbol
    """
    
    def setUp(self):
        gatherer.initSeqMaps()
            
        initMockAssayTypeSorts()
        
        # Inject test markers into SymbolSorter
        SymbolSorter.GENE_SYMBOL_SEQ_MAP = {
                'pax1': 1,
                'pax2': 2,
                'pax10': 10
        }
            
    
    ### test sort by hybridization ###
    def test_sortPaneByHybridization(self):
        """
        Sort by hybridization
        """
        # register hybridization strings in reverse order
        # blots are represented by empty value
        assays = [
                [1, IMMUNO_TYPE, 'pax1','A citation',''],
                [2, IMMUNO_TYPE, 'pax1','A citation','not applicable'],
                [3, IMMUNO_TYPE, 'pax1','A citation','not specified'],
                [4, IMMUNO_TYPE, 'pax1','A citation','optical section'],
                [5, IMMUNO_TYPE, 'pax1','A citation','section from whole mount'],
                [6, IMMUNO_TYPE, 'pax1','A citation','section'],
                [7, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByHybridizationAsc()
        self.assertEqual([7,6,5,4,3,2,1], sortedKeys)
        
        
    def test_sortWithinPaneByHybridization(self):
        """
        Sort by best hybridization within each pane
        """
        # whole mount and section are above not specified
        assays = [
                [1, IMMUNO_TYPE, 'pax1','A citation','not specified'],
                [1, IMMUNO_TYPE, 'pax1','A citation','section'],
                [2, IMMUNO_TYPE, 'pax1','A citation','not specified'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByHybridizationAsc()
        self.assertEqual([2,1], sortedKeys)
        
        
    def test_sortPaneByAssayType(self):
        """
        If hybridization is the same
        Sort by best assay type within each pane
        """
        # IMMUNO and RNA in situ are above NUCLEASE
        assays = [
                [1, NUCLEASE_TYPE, 'pax1','A citation','whole mount'],
                [1, RNA_TYPE, 'pax1','A citation','whole mount'],
                [2, NUCLEASE_TYPE, 'pax1','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByHybridizationAsc()
        self.assertEqual([2,1], sortedKeys)
        
        
    def test_sortPaneByMarker(self):
        """
        If hybridization and assya type are the same
        Sort by best gene symbol within each pane
        """
        # pax1 and pax2 sort above pax 10
        assays = [
                [1, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [1, IMMUNO_TYPE, 'pax2','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax10','A citation','whole mount'],
                [2, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
        ]
        
        sortAssays(assays)
            
        sortedKeys = getPaneKeysByHybridizationAsc()
        self.assertEqual([2,1], sortedKeys)


    def test_sortNotSpecifiedAndBlots(self):
            """
            Not Specifieds are second to last, followed by blot assays
                    which have no hybridization value
            """
            
            # a blot assay has no hybridization value
            #   it should sort last
            assays = [
                [1, IMMUNO_TYPE, 'pax1','A citation',''],
                [2, IMMUNO_TYPE, 'pax1','A citation','Not Specified'],
            ]
            
            sortAssays(assays)
            
            sortedKeys = getPaneKeysByHybridizationAsc()
            self.assertEqual([2,1], sortedKeys)
            
            
    def test_reverseSortOrder(self):
            """
            When sorting in reverse/desc mode
                    all hybridization types sort reverse, except
                    Not Specified is still second to last
                    and blots, or empty values,  are still last
            """
            
            # a blot assay has no hybridization value
            #   it should sort last
            assays = [
                [1, IMMUNO_TYPE, 'pax1','A citation',''],
                [2, IMMUNO_TYPE, 'pax1','A citation','not applicable'],
                [3, IMMUNO_TYPE, 'pax1','A citation','not specified'],
                [4, IMMUNO_TYPE, 'pax1','A citation','optical section'],
                [5, IMMUNO_TYPE, 'pax1','A citation','section from whole mount'],
                [6, IMMUNO_TYPE, 'pax1','A citation','section'],
                [7, IMMUNO_TYPE, 'pax1','A citation','whole mount'],
            ]
            
            sortAssays(assays)
            
            sortedKeys = getPaneKeysByHybridizationDesc()
            # 3, 2 and 1 (not specified, not applicable, blots) still sort to bottom
            #   which is same as in asc sort
            self.assertEqual([4,5,6,7,3,2,1], sortedKeys)
            

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(SortByAssayTypeTestCase))
    suite.addTest(unittest.makeSuite(SortByMarkerTestCase))
    suite.addTest(unittest.makeSuite(SortByHybridizationTestCase))
    return suite

if __name__ == '__main__':
    unittest.main()
