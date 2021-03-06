"""
Run annotation_gatherer test suites
"""
import sys,os.path
# adjust the path for running the tests locally, so that it can find gatherer (i.e. 2 dirs up)
sys.path.append(os.path.join(os.path.dirname(__file__), '../../gather'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../../lib/python'))

import unittest

from annotation import transform
import annotation_gatherer
gatherer = annotation_gatherer.gatherer

class TransformationsTestCase(unittest.TestCase):
    """
    Test the transformations we perform on the original annotation data
    """
    
    def test_transformAnnotTypeMPDerived(self):
        cols = ['annottype']
        rows = [['Mammalian Phenotype/Marker (Derived)']]

        transform.transformAnnotationType(cols, rows)

        expected = 'Mammalian Phenotype/Marker'
        self.assertEqual(expected, rows[0][0])

    def test_transformAnnotTypeDODerived(self):
        cols = ['annottype']
        rows = [['DO/Marker (Derived)']]

        transform.transformAnnotationType(cols, rows)

        expected = 'DO/Marker'
        self.assertEqual(expected, rows[0][0])

    def test_transformAnnotTypeNotDerived(self):
        cols = ['annottype']
        rows = [['DO/Marker']]

        transform.transformAnnotationType(cols, rows)

        expected = 'DO/Marker'
        self.assertEqual(expected, rows[0][0])


class GroupAnnotationsTestCase(unittest.TestCase):
    """
    Test the group (rollup) rules for annotations
        I.e. multiple evidence records become one row
            if they share certain criteria
    """
    
    def setUp(self):
        self.cols = ['_annotevidence_key', 
                     'annottype', 
                     '_term_key', 
                     '_object_key',
                     '_qualifier_key',
                     '_evidenceterm_key',
                     'inferredfrom' 
        ]
    
    def makeRow(self, 
                evidenceKey=1,
                annotType='test',
                termKey=1,
                objectKey=1,
                qualifierKey=1,
                evidenceCode=1,
                inferredfrom=''):
        """
        return test row for the given input,
            matching the order of self.cols
        """
        row = [
               evidenceKey,
               annotType,
               termKey,
               objectKey,
               qualifierKey,
               evidenceCode,
               inferredfrom
        ]
        return row
        
    
    def test_simple_one_group(self):
        rows = [self.makeRow(evidenceKey=1,
                             annotType='DO/Genotype')]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(1, len(groupMap))
        values = list(groupMap.values())[0]
        self.assertEqual(1, len(values))
        self.assertEqual('DO/Genotype', values[0][1])
        
    
    
    def test_GO_multiple_annotations_do_not_rollup(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom=''),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=2,
                             objectKey=3,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom='')]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(2, len(groupMap))
        
        
    def test_GO_multiple_annotations_rollup(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom=''),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom='')]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(1, len(groupMap))
        
        
    def test_GO_different_qualifier(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1614151,
                             evidenceCode=1,
                             inferredfrom=''),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1614156,
                             evidenceCode=1,
                             inferredfrom='')]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(2, len(groupMap))
        
        
    def test_GO_annotation_extensions_simple(self):
        """
        One annotation extension
        """
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom='')]
        
        propertyMap = {
            1 : [{'property': 'testing', 'value': 'test'}]
        }
    
        groupMap = transform.groupAnnotations(self.cols, rows, propertyMap)
    
        self.assertEqual(1, len(groupMap))
        
        
    def test_GO_annotation_extensions_multiple_annots(self):
        """
        One annotation extension causes split
        """
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom=''),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom='')
                ]
        
        propertyMap = {
            1 : [{'property': 'testing', 'value': 'test'}]
        }
    
        groupMap = transform.groupAnnotations(self.cols, rows, propertyMap)
    
        self.assertEqual(2, len(groupMap))
        
        
    def test_GO_annotation_extensions_duplicate_properties(self):
        """
        Duplicate annotation extension causes merge, and only one property should propogate
        """
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom=''),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom='')
                ]
        
        propertyMap = {
            1 : [{'property': 'testing', 'value': 'test'}],
            2 : [{'property': 'testing', 'value': 'test'}]
        }
    
        groupMap = transform.groupAnnotations(self.cols, rows, propertyMap)
    
        self.assertEqual(1, len(groupMap))
        
        
    def test_inferredfrom(self):
        """
        inferredfrom causes split
        """
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom=''),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1,
                             evidenceCode=1,
                             inferredfrom='MGI:12345')
                ]
        
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(2, len(groupMap))
        
        
    def test_MP_multiple_annotations_do_not_rollup(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1),
                self.makeRow(evidenceKey=2,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=2,
                             objectKey=3)]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(2, len(groupMap))
        
        
    def test_MP_multiple_annotations_rollup(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1),
                self.makeRow(evidenceKey=2,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1)]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(1, len(groupMap))
        
        
    def test_MP_qualifier_different(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1614151),
                self.makeRow(evidenceKey=2,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1614156)]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(2, len(groupMap))
        
        
    def test_MP_qualifier_same(self):
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=2),
                self.makeRow(evidenceKey=2,
                             annotType='Mammalian Phenotype/Genotype',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=2)]
    
        groupMap = transform.groupAnnotations(self.cols, rows)
    
        self.assertEqual(1, len(groupMap))
        
        
class AggregatePropertiesTestCase(unittest.TestCase):
    """
    Test the grouping of properties for multiple annotation rows
        that have been rolled up
        Make sure that duplicates are eliminated
    """
    
    def setUp(self):
        self.cols = ['_annotevidence_key', 
                     'annottype', 
                     '_term_key', 
                     '_object_key',
                     '_qualifier_key',
                     '_evidenceterm_key',
                     'inferredfrom' 
        ]
        
    def makeRow(self, 
                evidenceKey=1,
                annotType='test',
                termKey=1,
                objectKey=1,
                qualifierKey=1,
                evidenceCode=1,
                inferredfrom=''):
        """
        return test row for the given input,
            matching the order of self.cols
        """
        row = [
               evidenceKey,
               annotType,
               termKey,
               objectKey,
               qualifierKey,
               evidenceCode,
               inferredfrom
        ]
        return row
        
    def test_no_properties(self):
        
        propertiesMap = {}
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1)]
        
        aggregatedProps = transform.getAggregatedProperties(self.cols, rows, propertiesMap)
        self.assertEqual(len(aggregatedProps), 0)
        
    def test_multiple_different_properties(self):
        
        propertiesMap = { 1: [ {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test'}], 
                         2: [{'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test2'}] }
        
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1)]
        
        aggregatedProps = transform.getAggregatedProperties(self.cols, rows, propertiesMap)
        self.assertEqual(len(aggregatedProps), 2)
        

    def test_duplicate_properties(self):
        
        propertiesMap = { 1: [ {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test'}], 
                           2: [  {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test'}] }
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1)]
        
        aggregatedProps = transform.getAggregatedProperties(self.cols, rows, propertiesMap)
        self.assertEqual(len(aggregatedProps), 1)
        
        
    def test_duplicate_properties_many_stanzas(self):
        """
        If the same multi-stanza properties list exists for difference evidence records,
            we consider it a duplicate.
        """
        
        propertiesMap = { 1: [ {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test_stanza1'},
                              {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 2,
                                'sequencenum': 1,
                                'value': 'test_stanza2'}], 
                           2: [  {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test_stanza1'},
                              {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 2,
                                'sequencenum': 1,
                                'value': 'test_stanza2'}] }
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1),
                self.makeRow(evidenceKey=2,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1)]
        
        aggregatedProps = transform.getAggregatedProperties(self.cols, rows, propertiesMap)
        self.assertEqual(len(aggregatedProps), 2)
        
        
    def test_duplicate_stanza_for_same_evidence(self):
        """
        If the same stanza is duplicated for the same evidence record,
            we consider it a duplicate to remove
        """
        
        propertiesMap = { 1: [ {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 1,
                                'sequencenum': 1,
                                'value': 'test'},
                              {'type':'Annotation Extension', 
                                'property': 'has pattern', 
                                'stanza': 2,
                                'sequencenum': 1,
                                'value': 'test'}], }
        rows = [self.makeRow(evidenceKey=1,
                             annotType='GO/Marker',
                             termKey=1,
                             objectKey=1,
                             qualifierKey=1),]
        
        aggregatedProps = transform.getAggregatedProperties(self.cols, rows, propertiesMap)
        self.assertEqual(len(aggregatedProps), 1)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TransformationsTestCase))
    suite.addTest(unittest.makeSuite(GroupAnnotationsTestCase))
    suite.addTest(unittest.makeSuite(AggregatePropertiesTestCase))
    return suite

if __name__ == '__main__':
    unittest.main()
