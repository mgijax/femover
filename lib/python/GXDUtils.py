# Module: GXDUtils.py
# Purpose: to provide handy utility functions for dealing with GXD data

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
TIMES = [ (' day ',''), ('week ','w '), ('month ','m '), ('year ','y ') ]

# list of (old, new) pairs for use in seek-and-replace loops in abbreviate()
QUALIFIERS = [ ('embryonic', 'E'), ('postnatal', 'P') ]

def abbreviateAge (
        s               # string; specimen's age from gxd_expression.age
        ):
        # Purpose: convert 's' to a more condensed format for display on a
        #       query results page
        # Returns: string; with substitutions made as given in 'TIMES' and
        #       'QUALIFIERS' above.
        # Assumes: 's' contains at most one value from 'TIMES' and one value
        #       from 'QUALIFIERS'.  This is for efficiency, so we don't have
        #       to check every one for every invocation.
        # Effects: nothing
        # Throws: nothing

        # we have two different lists of (old, new) strings to check...
        for items in [ TIMES, QUALIFIERS ]:
                for (old, new) in items:

                        # if we do not find 'old' in 's', then we just go back
                        # up to continue the inner loop.

                        pos = s.find(old)
                        if pos == -1:
                                continue

                        # otherwise, we replace 'old' with 'new' and break out
                        # of the inner loop to go back to the outer one.

                        s = s.replace(old, new)
                        break
        return s

