#!/bin/sh

if [ -d zh ] ; then
	echo "directory zh exist"
	exit 1
fi

if [ -d en ] ; then
	echo "directory en exist"
	exit 2
fi

mkdir -p zh en

if ! wget https://dumps.wikimedia.org/enwiki/latest/ ; then
	echo "wget enwiki index.html failed"
	exit 1
fi

#xml
egrep -e "wiki.*.stream[0-9].*.xml.*.bz2\"" index.html | sed -e "s/<a href=\"\(.*\)\".*/\1/g" |  \
	awk '{print "wget https://dumps.wikimedia.org/enwiki/latest/" $0}' > download_en.sh

#index
egrep -e "wiki.*.stream-index[0-9].*.txt.*.bz2\"" index.html | sed -e "s/<a href=\"\(.*\)\".*/\1/g" | \
       	awk '{print "wget https://dumps.wikimedia.org/enwiki/latest/" $0}' >> download_en.sh

rm index.html


if ! wget https://dumps.wikimedia.org/zhwiki/latest/ ; then
        echo "wget zhwiki index.html failed"
        exit 1
fi

#xml
egrep -e "wiki.*.stream[0-9].*.xml.*.bz2\"" index.html | sed -e "s/<a href=\"\(.*\)\".*/\1/g" |  \
        awk '{print "wget https://dumps.wikimedia.org/zhwiki/latest/" $0}' > download_zh.sh

#index
egrep -e "wiki.*.stream-index[0-9].*.txt.*.bz2\"" index.html | sed -e "s/<a href=\"\(.*\)\".*/\1/g" | \
        awk '{print "wget https://dumps.wikimedia.org/zhwiki/latest/" $0}' >> download_zh.sh

rm index.html


(cd zh ; bash ../download_zh.sh)
(cd en ; bash ../download_en.sh)
