#!./python
# 
# gathers data for the 'probe_primers' table in the front-end database

import Gatherer

###--- Classes ---###

ProbeIDGatherer = Gatherer.Gatherer
        # Is: a data gatherer for the probe_primers table
        # Has: queries to execute against the source database
        # Does: queries the source database for primer sequences for probes,
        #       collates results, writes tab-delimited text file

###--- globals ---###

cmds = [
        # 0. primers for probes.  Assumes we cannot have a null primer 1 and a non-null primer 2.
        '''select _Probe_key, primer1sequence, primer2sequence
        from prb_probe
        where primer1sequence is not null'''
        ]

# order of fields (from the query results) to be written to the
# output file
fieldOrder = [ '_Probe_key', 'primer1sequence', 'primer2sequence', ]

# prefix for the filename of the output file
filenamePrefix = 'probe_primers'

# global instance of a ProbeIDGatherer
gatherer = ProbeIDGatherer (filenamePrefix, fieldOrder, cmds)

###--- main program ---###

# if invoked as a script, use the standard main() program for gatherers and
# pass in our particular gatherer
if __name__ == '__main__':
        Gatherer.main (gatherer)
