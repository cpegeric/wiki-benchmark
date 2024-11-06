#!/bin/bash

PWD=`pwd`
DBNAME=$1
DATADIR=$2
NTHREAD=$3

DBDIR=$DATADIR/$DBNAME

# make download directory
#mkdir zh en

# download the files
#(cd zh ; bash ../zhwiki.sh)
#(cd en ; bash ../enwiki.sh)

# create file list

realpath $DBNAME/*.xml-*.bz2 | sort > $DBNAME.list
realpath $DBNAME/*.txt-*.bz2 | sort > $DBNAME.index
sed -e "s/\(.*\)-index\(.*\).txt-\(.*\)/\1\2.xml-\3/g" $DBNAME.index > $DBNAME.list.tmp
if ! diff -q $DBNAME.list $DBNAME.list.tmp ; then
	echo "failed $DBNAME"
	exit 1
fi

sed -e "s/\(.*\)-index\(.*\).txt-\(.*\)/& \1\2.xml-\3/g" $DBNAME.index > $DBNAME.cmd

#if [ -d $DBDIR ]; then
#	echo "data directory $DBDIR already exist"
#	exit 2
#fi
#mkdir -p $DBDIR

rm -f run_$DBNAME.sh
cat $DBNAME.cmd | while read line ; do
	echo "if ! python3 $PWD/../python/wikidump.py $DBNAME $DBDIR $line; then; echo 'wikidump.py failed'; exit 1; fi" >> run_$DBNAME.sh
done

NFRAG=`expr $NTHREAD - 1`
for FRAGID in $(seq 0 $NFRAG)
do
	echo "python3 $PWD/../python/embed.py zh $FRAGID $NTHREAD &" >> run_$DBNAME.sh
done