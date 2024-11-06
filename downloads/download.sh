#!/bin/bash

PWD=`pwd`

# make download directory
mkdir zh en

# download the files
(cd zh ; bash ../zhwiki.sh)
(cd en ; bash ../enwiki.sh)

