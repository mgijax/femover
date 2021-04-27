# Module: GOGraphs.py
# Purpose: to encapsulate knowledge of how to determine which markers have
#       GO graphs available and which do not
# Assumes: config.GO_GRAPH_PATH is defined

import os
import re
import glob
import logger
import config

###--- Globals ---###

# marker MGI ID -> 1
# (for markers which have a GO graph)
MARKERS_WITH_GO_GRAPHS = {}

# mouse marker symbol -> 1
# (for homology classes which have a GO orthology graph)
HOMOLOGY_CLASSES_WITH_GRAPHS = {}

# have we already initialized this module?
INITIALIZED = False

###--- Private Functions ---###

def _initialize():
        global MARKERS_WITH_GO_GRAPHS, HOMOLOGY_CLASSES_WITH_GRAPHS
        global INITIALIZED

        MARKERS_WITH_GO_GRAPHS = {}
        HOMOLOGY_CLASSES_WITH_GRAPHS = {}

        if os.path.isdir(config.GO_GRAPH_PATH):
                # pull apart the path, with filename like:
                #       MGI_12345.html
                # This is for marker GO graphs.
                filenameRE = re.compile ('([A-Z]+)_([0-9]+).html')

                # or filenames for homology classes, named by mouse marker symbol, like:
                #       Pax6.html
                # This is for orthology GO graphs, naming based on the
                # mouse marker in the cluster.
                symbolRE = re.compile ('(.+).html')

                items = [ ('marker', MARKERS_WITH_GO_GRAPHS),
                        ('orthology', HOMOLOGY_CLASSES_WITH_GRAPHS) ]

                for (subdir, cache) in items:
                        files = glob.glob (os.path.join (config.GO_GRAPH_PATH,
                                subdir, '*.html'))

                        graphPath = os.path.join (config.GO_GRAPH_PATH, subdir) + '/'
                        
                        for path in files:
                                match = filenameRE.search(path)
                                if match:
                                        # marker GO graphs
                                        mgiID = '%s:%s' % (match.group(1),
                                                match.group(2))
                                        cache[mgiID] = 1

                                else:
                                        # homology GO graphs
                                        match = symbolRE.search(path)
                                        if match:
                                                symbol = match.group(1).replace(graphPath, '')
                                                cache[symbol] = 1

                        logger.debug ('Found %d %s GO graphs' % (
                                len(cache), subdir) )
        else:
                # don't have a lack of GO graphs be a fatal error; just log it
                logger.debug ('Bad path for GO_GRAPH_PATH: %s' % \
                        config.GO_GRAPH_PATH)

        INITIALIZED = True
        return

def _hasGraph (accID, cache):
        # determine if there's an entry for 'accID' in the given 'cache'.
        # initialize the cache if not already done.

        if not INITIALIZED:
                _initialize()

        if accID in cache:
                return 1
        return 0


###--- Functions ---###

def hasGOGraph (markerID):
        # determine whether the marker with the given 'markerID' (its
        # preferred MGI ID) has a GO graph (1) or not (0)

        return _hasGraph (markerID, MARKERS_WITH_GO_GRAPHS)

def hasComparativeGOGraph (mouseSymbol):
        # determine whether the orthology class containing the given mouse symbol
        # has a comparative GO graph (1) or not (0)

        return _hasGraph (mouseSymbol, HOMOLOGY_CLASSES_WITH_GRAPHS)
