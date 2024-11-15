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
text_tbl = "wiki_text"
json_tbl = "wiki_json"
docx_tbl = "wiki_docx"
pdf_tbl = "wiki_pdf"

def create_tables(cursor):
    # wiki text
    sql = "create table %s (id bigint primary key auto_increment, wiki_id bigint, title varchar, src text)" % text_tbl
    cursor.execute(sql)

    chunk_tbl = "%s_chunk" % text_tbl
    sql = "create table %s (chunkid bigint primary key auto_increment, wiki_id bigint, text varchar, embed vecf32(3072))" % chunk_tbl
    cursor.execute(sql)

    # wiki json
    sql = "create table %s (id bigint primary key auto_increment, wiki_id bigint, src json)" % json_tbl
    cursor.execute(sql)
    
    chunk_tbl = "%s_chunk" % json_tbl
    sql = "create table %s (chunkid bigint primary key auto_increment, wiki_id bigint, text varchar, embed vecf32(3072))" % chunk_tbl
    cursor.execute(sql)

    # wiki docx
    sql = "create table %s (id bigint primary key auto_increment, wiki_id bigint, title varchar, src datalink)" % docx_tbl
    cursor.execute(sql)

    chunk_tbl = "%s_chunk" % docx_tbl
    sql = "create table %s (chunkid bigint primary key auto_increment, wiki_id bigint, text varchar, embed vecf32(3072))" % chunk_tbl
    cursor.execute(sql)

    # wiki pdf
    sql = "create table %s (id bigint primary key auto_increment, wiki_id bigint, title varchar, src datalink)" % pdf_tbl
    cursor.execute(sql)

    chunk_tbl = "%s_chunk" % pdf_tbl
    sql = "create table %s (chunkid bigint primary key auto_increment, wiki_id bigint, text varchar, embed vecf32(3072))" % chunk_tbl
    cursor.execute(sql)


def drop_table(cursor, tblname):
    sql = "drop table %s" % tblname
    cursor.execute(sql)


def drop_tables(cursor):
    drop_table(cursor, text_tbl)
    drop_table(cursor, "%s_chunk" % text_tbl)
    drop_table(cursor, json_tbl)
    drop_table(cursor, "%s_chunk" % json_tbl)
    drop_table(cursor, docx_tbl)
    drop_table(cursor, "%s_chunk" % docx_tbl)
    drop_table(cursor, pdf_tbl)
    drop_table(cursor, "%s_chunk" % pdf_tbl)


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

