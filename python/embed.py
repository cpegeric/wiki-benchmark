import pymysql.cursors
import sys, os
import tempfile
from pathlib import Path
import subprocess
import wikitextparser as wtp
from docx import Document

# installation
# pip install wikitextparser
# pip install python-docx

# create database

# create tables

# load index file

# load wikidump pages

# convert into docx

wikidump_wasm="https://github.com/cpegeric/wikidump-wasm/raw/main/wikidump/wikidump.wasm"
ollama_wasm="https://github.com/cpegeric/ollama-wasm/raw/main/ollama/ollama.wasm"
dbname="test"
chunk_tbl = "wiki_chunk"
page_tbl = "wiki_page"
data_dir = "/tmp/wikidump"
model = "llama3.2"

def create_tables(cursor):
    create_page_table_sql = "create table %s (id bigint, chunkid int, text varchar, embed vecf32(3072))" % chunk_tbl
    cursor.execute(create_page_table_sql)

def embedding(cursor, model, fragid, nfrag):
    cfg = """{"model":"%s"}""" % model
    sql = "select src.id, json_unquote(json_extract(f.result, '$.id')), json_unquote(json_extract(f.result, '$.chunk')), \
            json_unquote(json_extract(f.result, '$.embedding')) from %s as src CROSS APPLY \
            moplugin_table('%s', 'embed', '%s', src.src) as f where mod(src.id, %d) = %d" % (page_tbl,  ollama_wasm, cfg, nfrag, fragid)
    print(sql)
    cursor.execute(sql)
    results = cursor.fetchall()

    sql = "insert into wiki_chunk values (%s, %s, %s, %s)"
    cursor.executemany(sql, results)


if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 4:
        print("usage: embed.py [create|load] fragid nfrag")
        sys.exit(1)

    cmd = sys.argv[1]
    fragid = int(sys.argv[2])
    nfrag = int(sys.argv[3])

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            if cmd == "create":
                create_tables(cursor)
            elif cmd == "load":
                embedding(cursor, model, fragid, nfrag)
            else:
                print("command not supported")

