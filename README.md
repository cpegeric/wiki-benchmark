# Setup Guide

Before run these scripts, please install matrixone database, ollama with llama3.2 model.

1. setup environment

```
pip install wikitextparser
pip install python-docx
source env.sh
```

2. download the wikidump

download.sh better run in background.

```
> cd downloads 
> bash download.sh &
```

directory zh and en will be created and files are in zh and en directory.

3. create database zh and en

4. create tables for zh and en database

```
> cd python
> python3 create.py create zh
> python3 create.py create en
```

5. compile the script

```
> cd downloads
> bash compile.sh zh /datadir nthread
e.g.

> cd downloads
> bash compile.sh zh /tmp/wikidump 10
> bash compile.sh en /tmp/wikidump 10
```
zh_01.sh, zh_02.sh, en_01.sh and en_02.sh script will be created.

6. run the script in sequence

```
> bash zh_01.sh &> log
> bash zh_02.sh
```

01.sh will parse the wikidump and generate docx files in data directory.
wiki_page table will be updated with all docx files generated.

02.sh will generate the embedding from wiki_age table to wiki_chunk table.

Note: commands in zh_02.sh run in background mode. It will exit right away

7. Create fulltext with wiki_page table

8. Create vector index with wiki_chunk table

9. run ask.py to ask question from ollama
