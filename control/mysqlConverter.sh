#!/bin/sh

# Purpose: currently a no-op; in the future, if processing of text files is
#	needed before loading them into MySQL, this is where it would go
#
# this used to do some sybase-to-postgres conversions, but now it does nothing

# just report back the filename that should be loaded
echo $1
