#!/usr/bin/sh

# Purpose: to make any bcp file tweaks, in preparation for bulk load into
#	Postgres:
#		1. trim milliseconds from timestamps
#		2. escape any instances of the characters we will use for tabs
#			and newlines for Postgres
#		3. convert tabs and newline characters to the one-character
#			ones we will use for Postgres
# Assumes: bcp file format is from FreeTDS (hh:mm:ss:uuu)

# 1. trim milliseconds
# 2. escape embedded tabs and newlines, remove bad characters, convert FreeTDS
#	record separators
# 3. convert FreeTDS tabs to traditional tab characters

#sed 's/\([0-9][0-9]:[0-9][0-9]:[0-9][0-9]\).[0-9][0-9][0-9]/\1/g' $1 | \
#postgresTextCleaner.py | \
#sed 's/	/\\	/g' | \
#sed 's/$/\\/g' | \
#sed 's/#=#\\$//g' | \
#sed 's/&=&/	/g' > $1.new

cat $1 | postgresTextCleaner.py > $1.new

# replace the original file
rm $1
mv $1.new $1

# report back the file to be loaded
echo $1
