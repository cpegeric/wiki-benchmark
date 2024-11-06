1. download the wikidump

```
> (cd downloads ; bash download.sh)
```

directory zh and en will be created and files are in zh and en directory.

2. create database zh and en

3. create tables for zh and en database

```
> (cd python ; python3 create.py zh ; python3 create.py en)
```

4. compile the script

```
> cd downloads
> bash compile.sh zh /datadir nthread
e.g.

> cd downloads
> bash compile.sh zh /tmp/wikidump 10
> bash compile.sh en /tmp/wikidump 10
```
zh_01.sh, zh_02.sh, en_01.sh and en_02.sh script will be created.

5. run the script in sequence

```
> bash zh_01.sh &> log
> bash zh_02.sh
```

Note: commands in zh_02.sh run in background mode. It will exit right away

