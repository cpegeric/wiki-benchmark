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

wikihome = os.getenv("WIKI_HOME")
if wikihome is None:
    print("WIKI_HOME environment variable not found")
    sys.exit(10)


wikidump_wasm=Path(os.path.join(wikihome, "wasm/wikidump.wasm")).as_uri()
ollama_wasm=Path(os.path.join(wikihome, "wasm/ollama.wasm")).as_uri()
page_tbl = "wiki_page"
chunk_tbl = "wiki_chunk"

def create_tables(cursor):
    sql = "create table %s (id bigint primary key, title varchar, src datalink)" % page_tbl
    cursor.execute(sql)
    sql = "create table %s (id bigint, chunkid int, text varchar, embed vecf32(3072), primary key (id,chunkid))" % chunk_tbl
    cursor.execute(sql)

def drop_tables(cursor):
    sql = "drop table %s" % page_tbl
    cursor.execute(sql)
    sql = "drop table %s" % chunk_tbl
    cursor.execute(sql)


if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 3:
        print("usage: create.py [create|drop] dbname")
        sys.exit(1)

    cmd = sys.argv[1]
    dbname = sys.argv[2]

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            if cmd == "create":
                create_tables(cursor)
            elif cmd == "drop":
                drop_tables(cursor)

