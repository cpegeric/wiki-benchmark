import pymysql.cursors
import sys, os
import tempfile
from pathlib import Path
import subprocess
import wikitextparser as wtp
from docx import Document
import numpy as np
import concurrent.futures
from timeit import default_timer as timer

# installation
# pip install wikitextparser
# pip install python-docx

wikihome = os.getenv("WIKI_HOME")
if wikihome is None:
    print("WIKI_HOME environment variable not found")
    sys.exit(10)

def set_env(cursor):
    # hnsw table
    sql = "set experimental_hnsw_index = 1"
    #print(sql)
    cursor.execute(sql)
    sql = "set experimental_ivf_index = 1"
    #print(sql)
    cursor.execute(sql)
    sql = "set @probe_limit = 5"
    print(sql)
    cursor.execute(sql)



def create_table(cursor, tblname, dim):
    # hnsw table
    sql = "set experimental_hnsw_index = 1"
    #print(sql)
    cursor.execute(sql)
    sql = "set experimental_ivf_index = 1"
    cursor.execute(sql)
    sql = "set @probe_limit = 10"
    cursor.execute(sql)
    sql = "set ivf_threads_build = 0"
    cursor.execute(sql)

    #sql = "create table %s (id bigint primary key auto_increment, embed vecf32(%d))" % (tblname, dim)
    sql = "create table %s (id bigint primary key auto_increment, embed vecf64(%d))" % (tblname, dim)
    print(sql)
    cursor.execute(sql)


def normalize(array):
    sum = 0
    for x in array:
        sum += x*x
    sum = math.sqrt(sum)
    # normalize
    for i in range(len(array)):
        array[i] = array[i] / sum
    return array

def gen_embed(rs, dim, nitem, start):
    array = normalize(rs.rand(nitem, dim))
    res = []
    i = start
    for a in array:
        s = '[' + ','.join(str(x) for x in a) + ']'
        res.append((i, s))
        i+=1

    return res

def insert_embed(cursor, src_tbl, dim, nitem, seek):
    rs = np.random.RandomState(seek)
    batchsz = 1000
    n = 0
    while n < nitem:
        if n + batchsz > nitem:
            batchsz = nitem - n
        dataset = gen_embed(rs, dim, batchsz, n)

        sql = "insert into %s (id, embed) values (%s)" % (src_tbl, "%s, %s")
        print(sql)
        #print(res)
        cursor.executemany(sql, dataset)

        n += batchsz


# For datasets with less than one million rows, use lists = rows / 1000.
# For datasets with more than one million rows, use lists = sqrt(rows).
# It is generally advisable to have at least 10 clusters.
# The recommended value for the probes parameter is probes = sqrt(lists).
def create_ivfflat_index(cursor, src_tbl, index_name, nitem):
    lists = 0
    if nitem < 1000000:
        lists = int(nitem/1000)
    else:
        lists = int(math.sqrt(nitem))
    if lists < 10:
        lists = 10
    sql = "create index %s using ivfflat on %s(embed) lists=%s op_type \"vector_l2_ops\"" % (index_name, src_tbl, lists)
    print(sql)
    start = timer()
    cursor.execute(sql)
    end = timer()
    print("create index time = ", end-start, " sec")



def create_hnsw_index(cursor, src_tbl, index_name):
    sql = "create index %s using hnsw on %s(embed) m 48 op_type \"vector_l2_ops\"" % (index_name, src_tbl)
    print(sql)
    start = timer()
    cursor.execute(sql)
    end = timer()
    print("create index time = ", end-start, " sec")


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


def thread_run(host, dbname, src_tbl, dim, nitem, segid, nseg, seek):
    rs = np.random.RandomState(seek)
    recall = 0
    conn = pymysql.connect(host=host, port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            set_env(cursor)
            i = 0
            batchsz = 1000
            n = 0
            while n < nitem:
                if n + batchsz > nitem:
                    batchsz = nitem - n
                dataset = gen_embed(rs, dim, batchsz, n)
                for row in dataset:
                    if i % nseg == segid:
                        rid = row[0]
                        v = row[1]
                        sql = "select id from %s order by l2_distance(embed, '%s') asc limit 1" % (src_tbl, v)
                        cursor.execute(sql)
                        res = cursor.fetchall()
                        resid = res[0][0]
                        if resid == rid:
                            recall += 1
                    i += 1

                n += batchsz

    return recall


def recall_run(host, dbname, src_tbl, dim, nitem, seek, nthread):
    total_recall = 0
    start = timer()
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthread) as executor:
        for index in range(nthread):
            future = executor.submit(thread_run, host, dbname, src_tbl, dim, nitem, index, nthread, seek)
        for index in range(nthread):
            total_recall += future.result()

    end = timer()
    rate = total_recall / nitem
    print("recall rate = ", rate, ", elapsed = ", end-start, " sec, ", (end-start)/nitem*1000, " ms/row, qps = ", nitem/(end-start))


if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 9:
        print("usage: indextest.py [build|recall|search] dbname src_tbl indexname dimension nitem [hnsw|ivf]")
        print("e.g python3 indextest.py [build|recall|search] localhost zh srctbl idx 3072 100000 hnsw")
        sys.exit(1)

    action = sys.argv[1]
    host = sys.argv[2]
    dbname = sys.argv[3]
    src_tbl = sys.argv[4]
    index_name = sys.argv[5]
    dimension = int(sys.argv[6])
    nitem = int(sys.argv[7])
    optype = sys.argv[8]

    # fix the seek to always generate the same sequence
    seek = 99
    conn = pymysql.connect(host=host, port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:

            if action == "build":
                drop_table(cursor, src_tbl)

                create_table(cursor, src_tbl, dimension)

                insert_embed(cursor, src_tbl, dimension, nitem, seek)

                if optype == "hnsw":
                    create_hnsw_index(cursor, src_tbl, index_name)
                else:
                    create_ivfflat_index(cursor, src_tbl, index_name, nitem)

            elif action == "recall":
                # concurrency thread count
                nthread = 8
                recall_run(host, dbname, src_tbl, dimension, nitem, seek, nthread)
            else:
                select_embed(cursor, src_tbl, dimension)

