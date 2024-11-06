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

wikidump_wasm="https://github.com/cpegeric/mojo/raw/main/plugin/wikidump/wikidump.wasm"
ollama_wasm="https://github.com/cpegeric/mojo/raw/main/plugin/ollama/ollama.wasm"
page_tbl = "wiki_page"
chunk_tbl = "wiki_chunk"

def create_tables(cursor):
    sql = "create table %s (id bigint primary key, title varchar, src datalink)" % page_tbl
    cursor.execute(sql)
    sql = "create table %s (id bigint, chunkid int, text varchar, embed vecf32(3072), primary key (id,chunkid))" % chunk_tbl

def drop_tables(cursor):
    sql = "drop table %s" % page_tbl
    cursor.execute(sql)
    sql = "drop table %s" % chunk_tbl
    cursor.execute(sql)


if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 2:
        print("usage: create.py [create|drop] dbname")

    cmd = sys.argv[1]
    dbname = sys.argv[2]

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            if cmd == "create":
                create_tables(cursor)
            elif cmd == "drop":
                drop_tables(cursor)

