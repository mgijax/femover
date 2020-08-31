#!./python
"""
Run all individual gatherer test suites
"""
import sys,os.path
# adjust the path for running the tests locally, so that it can find gatherer (i.e. 1 dirs up)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import unittest

# import all sub test suites
from gatherer import annotation_gatherer_tests
from gatherer import expression_imagepane_gatherer_tests

# add the test suites
def master_suite():
        suites = []
        suites.append(annotation_gatherer_tests.suite())
        suites.append(expression_imagepane_gatherer_tests.suite())
        
        master_suite = unittest.TestSuite(suites)
        return master_suite

if __name__ == '__main__':
        test_suite = master_suite()
        runner = unittest.TextTestRunner()
        
        ret = not runner.run(test_suite).wasSuccessful()
        sys.exit(ret)

