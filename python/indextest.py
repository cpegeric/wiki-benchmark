import pymysql.cursors
import sys, os
import tempfile
from pathlib import Path
import subprocess
import wikitextparser as wtp
from docx import Document
import numpy as np

# installation
# pip install wikitextparser
# pip install python-docx

wikihome = os.getenv("WIKI_HOME")
if wikihome is None:
    print("WIKI_HOME environment variable not found")
    sys.exit(10)


wikidump_wasm=Path(os.path.join(wikihome, "wasm/wikidump.wasm")).as_uri()
ollama_wasm=Path(os.path.join(wikihome, "wasm/ollama.wasm")).as_uri()

model = "llama3.2"

def create_table(cursor, tblname, dim):
    # hnsw table
    sql = "set experimental_hnsw_index = 1"
    print(sql)
    cursor.execute(sql)

    sql = "create table %s (id bigint primary key auto_increment, embed vecf32(%d))" % (tblname, dim)
    print(sql)
    cursor.execute(sql)



def gen_embed(dim, nitem):
    array = np.random.rand(nitem, dim)
    res = []
    i = 0
    for a in array:
        s = '[' + ','.join(str(x) for x in a) + ']'
        res.append((i, s))
        i+=1

    return res

def insert_embed(cursor, src_tbl, dim, nitem):

    res = gen_embed(dim, nitem)

    sql = "insert into %s (id, embed) values (%s)" % (src_tbl, "%s, %s")
    print(sql)
    #print(res)
    cursor.executemany(sql, res)

def create_hnsw_index(cursor, src_tbl, index_name):
    sql = "create index %s using hnsw on %s(embed) op_type \"vector_l2_ops\"" % (index_name, src_tbl)
    print(sql)
    cursor.execute(sql)


def select_embed(cursor, src_tbl, dim):
    array = np.random.rand(1, dim)
    s = '[' + ','.join(str(x) for x in array[0]) + ']'
    sql = "select id from %s order by l2_distance(embed, '%s') asc limit 1" % (src_tbl, s)
    print(sql)
    cursor.execute(sql)
    res = cursor.fetchall()
    print(res)


def drop_table(cursor, tblname):
    sql = "drop table if exists %s" % (tblname)
    print(sql)
    cursor.execute(sql)

def reindex_hnsw(cursor, src_tbl, index_name):
    sql = "alter table %s alter reindex %s hnsw" % (src_tbl, index_name)
    print(sql)
    cursor.execute(sql)


if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 7:
        print("usage: indextest.py [build|search] dbname src_tbl indexname dimension nitem")
        print("e.g python3 indextest.py zh wiki_docx_chunk idx 3072 100000")
        sys.exit(1)

    action = sys.argv[1]
    dbname = sys.argv[2]
    src_tbl = sys.argv[3]
    index_name = sys.argv[4]
    dimension = int(sys.argv[5])
    nitem = int(sys.argv[6])

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:

            if action == "build":
                drop_table(cursor, src_tbl)

                create_table(cursor, src_tbl, dimension)

                insert_embed(cursor, src_tbl, dimension, nitem)

                create_hnsw_index(cursor, src_tbl, index_name)

            else:
                select_embed(cursor, src_tbl, dimension)

