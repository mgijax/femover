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

# HomoloGene class ID -> 1
# (for HomoloGene classes which have a GO orthology graph)
HOMOLOGENE_CLASSES_WITH_GRAPHS = {}

# have we already initialized this module?
INITIALIZED = False

###--- Private Functions ---###

def _initialize():
        global MARKERS_WITH_GO_GRAPHS, HOMOLOGENE_CLASSES_WITH_GRAPHS
        global INITIALIZED

        MARKERS_WITH_GO_GRAPHS = {}
        HOMOLOGENE_CLASSES_WITH_GRAPHS = {}

        if os.path.isdir(config.GO_GRAPH_PATH):
                # pull apart the path, with filename like:
                #       MGI_12345.html
                filenameRE = re.compile ('([A-Z]+)_([0-9]+).html')

                # or filenames for HomoloGene IDs like:
                #       36030.html
                hgFilenameRE = re.compile ('([0-9]+).html')

                items = [ ('marker', MARKERS_WITH_GO_GRAPHS),
                        ('orthology', HOMOLOGENE_CLASSES_WITH_GRAPHS) ]

                for (subdir, cache) in items:
                        files = glob.glob (os.path.join (config.GO_GRAPH_PATH,
                                subdir, '*.html'))

                        for path in files:
                                match = filenameRE.search(path)
                                if match:
                                        mgiID = '%s:%s' % (match.group(1),
                                                match.group(2))
                                        cache[mgiID] = 1

                                else:
                                        match = hgFilenameRE.search(path)
                                        if match:
                                                hgID = match.group(1)
                                                cache[hgID] = 1

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

def hasComparativeGOGraph (homoloGeneID):
        # determine whether the HomoloGene class with the given 'homoloGeneID'
        # has a comparative GO graph (1) or not (0)

        return _hasGraph (homoloGeneID, HOMOLOGENE_CLASSES_WITH_GRAPHS)
