import pymysql.cursors
import sys, os
import tempfile
from pathlib import Path
import subprocess
import numpy as np
import concurrent.futures
import math
from timeit import default_timer as timer

# installation
# pip install wikitextparser
# pip install python-docx

#wikihome = os.getenv("WIKI_HOME")

optype2distfn = {'vector_l2_ops':'l2_distance', 'vector_cosine_ops':'cosine_distance', 'vector_ip_ops':'inner_product'}

batch_test_size = 1

#if wikihome is None:
#    print("WIKI_HOME environment variable not found")
#    sys.exit(10)

def set_env(cursor):
    # hnsw table
    sql = "set experimental_hnsw_index = 1"
    #print(sql)
    cursor.execute(sql)
    sql = "set experimental_ivf_index = 1"
    #print(sql)
    cursor.execute(sql)
    sql = "set probe_limit = 5"
    #print(sql)
    cursor.execute(sql)

    if batch_test_size > 1:
        sql = "set @mo_batch_test_size = %d" % (batch_test_size)
        print(sql)
        cursor.execute(sql)



def create_table(cursor, tblname, dim):
    # hnsw table
    sql = "set experimental_hnsw_index = 1"
    #print(sql)
    cursor.execute(sql)
    sql = "set experimental_ivf_index = 1"
    cursor.execute(sql)
    sql = "set probe_limit = 5"
    cursor.execute(sql)
    sql = "set ivf_threads_build = 0"
    cursor.execute(sql)
    sql = "set kmeans_train_percent = 1"
    cursor.execute(sql)
    sql = "set kmeans_max_iteration = 20"
    cursor.execute(sql)

    #sql = "set hnsw_threads_build = 4"
    #cursor.execute(sql)
    sql = "set hnsw_max_index_capacity = 1000000"
    cursor.execute(sql)

    sql = "create table %s (id bigint primary key auto_increment, embed vecf32(%d))" % (tblname, dim)
    #sql = "create table %s (id bigint primary key auto_increment, embed vecf64(%d))" % (tblname, dim)
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

def gen_embed(rs, dim, nitem, start, optype):
    array = rs.rand(nitem, dim)
    #print(array)
    normalized = []
    for x in array:
        normalized.append(normalize(x))
    array = normalized
    #print(array)
    res = []
    i = start
    for a in array:
        s = '[' + ','.join(str(x) for x in a) + ']'
        res.append((i, s))
        i+=1

    return res

def insert_embed(cursor, rs, src_tbl, dim, nitem, seek, optype, start=0):
    #rs = np.random.RandomState(seek)
    batchsz = 1000
    n = start
    while n < start+nitem:
        if n + batchsz > start+nitem:
            batchsz = nitem - n
        dataset = gen_embed(rs, dim, batchsz, n, optype)

        sql = "insert into %s (id, embed) values (%s)" % (src_tbl, "%s, %s")
        #print(sql)
        #print(dataset)
        cursor.executemany(sql, dataset)
        print(batchsz, "rows inserted")

        n += batchsz


# For datasets with less than one million rows, use lists = rows / 1000.
# For datasets with more than one million rows, use lists = sqrt(rows).
# It is generally advisable to have at least 10 clusters.
# The recommended value for the probes parameter is probes = sqrt(lists).
def create_ivfflat_index(cursor, src_tbl, index_name, optype, nitem, asyncopt):
    lists = 0
    if nitem < 1000000:
        lists = int(nitem/1000)
    else:
        lists = int(math.sqrt(nitem))
    if lists < 10:
        lists = 10

    #lists = 3000
    asyncstr = ""
    if asyncopt:
        asyncstr  = "ASYNC"

    sql = "create index %s using ivfflat on %s(embed) lists=%s op_type \"%s\" %s" % (index_name, src_tbl, lists, optype, asyncstr)

    print(sql)
    start = timer()
    cursor.execute(sql)
    end = timer()
    print("create index time = ", end-start, " sec")



def create_hnsw_index(cursor, src_tbl, index_name, optype):
    sql = "create index %s using hnsw on %s(embed) m=64 ef_construction=200 op_type \"%s\"" % (index_name, src_tbl, optype)
    print(sql)
    start = timer()
    cursor.execute(sql)
    end = timer()
    print("create index time = ", end-start, " sec")


def select_random_embed(cursor, src_tbl, dim, optype):
    array = np.random.rand(1, dim)
    s = '[' + ','.join(str(x) for x in array[0]) + ']'
    sql = "select id from %s order by %s(embed, '%s') asc limit 1" % (src_tbl, optype2distfn[optype], s)
    print(sql)
    cursor.execute(sql)
    res = cursor.fetchall()
    print(res)

def select_embed(cursor, src_tbl, dim, optype, array):
    sql = "select id from %s order by %s(embed, '%s') asc limit 1" % (src_tbl, optype2distfn[optype], array)
    #print(sql)
    cursor.execute(sql)
    res = cursor.fetchall()
    #print(res)
    return res


def drop_table(cursor, tblname):
    sql = "drop table if exists %s" % (tblname)
    print(sql)
    cursor.execute(sql)

def reindex_hnsw(cursor, src_tbl, index_name):
    sql = "alter table %s alter reindex %s hnsw" % (src_tbl, index_name)
    print(sql)
    cursor.execute(sql)


def thread_run(host, dbname, src_tbl, dim, nitem, segid, nseg, seek, optype, dataset):
    recall = 0
    conn = pymysql.connect(host=host, port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            set_env(cursor)
            i = 0
            for row in dataset:
                if i % nseg == segid:
                    rid = row[0]
                    v = row[1]
                    sql = "select id from %s order by %s(embed, '%s') asc limit 1" % (src_tbl, optype2distfn[optype], v)
                    cursor.execute(sql)
                    res = cursor.fetchall()
                    if res is not None:
                        resid = res[0][0]
                        if resid == rid:
                            recall += 1
                i += 1

    return recall


def recall_run(host, dbname, src_tbl, dim, nitem, seek, nthread, optype):
    total_recall = 0
    # generate whole dataset in advance instead of batch by batch to make sure the search time don't include data generation
    print("start generate", nitem, "vectors")
    rs = np.random.RandomState(seek)
    dataset = gen_embed(rs, dim, nitem, 0, optype)
    print("dataset generated and start search.")
    # start timer after data generated
    start = timer()
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthread) as executor:
        futures = []
        for index in range(nthread):
            futures.append(executor.submit(thread_run, host, dbname, src_tbl, dim, nitem, index, nthread, seek, optype, dataset))

        for future in concurrent.futures.as_completed(futures):
            try:
                total_recall += future.result()
            except Exception as e:
                print(e)

    end = timer()
    rate = total_recall / nitem
    total_item = nitem * batch_test_size
    print("recall rate = ", rate, ", elapsed = ", end-start, " sec, ", (end-start)/total_item*1000, " ms/row, qps = ", total_item/(end-start))


def recall_run(host, dbname, src_tbl, dim, nitem, seek, nthread, optype):
    total_recall = 0
    # generate whole dataset in advance instead of batch by batch to make sure the search time don't include data generation
    print("start generate", nitem, "vectors")
    rs = np.random.RandomState(seek)
    dataset = gen_embed(rs, dim, nitem, 0, optype)
    print("dataset generated and start search.")
    # start timer after data generated
    start = timer()
    with concurrent.futures.ThreadPoolExecutor(max_workers=nthread) as executor:
        futures = []
        for index in range(nthread):
            futures.append(executor.submit(thread_run, host, dbname, src_tbl, dim, nitem, index, nthread, seek, optype, dataset))

        for future in concurrent.futures.as_completed(futures):
            try:
                total_recall += future.result()
            except Exception as e:
                print(e)

    end = timer()
    rate = total_recall / nitem
    total_item = nitem * batch_test_size
    print("recall rate = ", rate, ", elapsed = ", end-start, " sec, ", (end-start)/total_item*1000, " ms/row, qps = ", total_item/(end-start))

def sample_run(cursor, host, dbname, src_tbl, dim, nitem, seek, nthread, optype):
    rs = np.random.RandomState(seek)
    batchsz = 1000
    n = 0
    nmatch = 0
    nsample = 0
    while n < nitem:
        if n + batchsz > nitem:
            batchsz = nitem - n
        dataset = gen_embed(rs, dim, batchsz, n, optype)

        #pick first and last item
        #print(dataset[0])
        #print(dataset[len(dataset)-1])

        size = len(dataset)
        nsample += 2

        res = select_embed(cursor, src_tbl, dim, optype, dataset[0][1])
        if res[0][0] == dataset[0][0]:
            nmatch += 1
            #print("match ", dataset[0][0])
        res = select_embed(cursor, src_tbl, dim, optype, dataset[size-1][1])
        if res[0][0] == dataset[size-1][0]:
            nmatch += 1
            #print("match ", dataset[size-1][0])

        n += batchsz

    print("nsample= ", nsample , ", nmatch= ", nmatch, ", ratio= ", nmatch/nsample)

if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 10:
        print("usage: indextest.py [build|buildcdc|recall|search|sample] host dbname src_tbl indexname optype dimension nitem [hnsw|ivf]")
        print("e.g python3 indextest.py [build|buildcdc|recall|search|sample] localhost zh srctbl idx [vector_l2_ops|vector_cosine_ops|vector_ip_ops] 3072 100000 hnsw")
        sys.exit(1)

    action = sys.argv[1]
    host = sys.argv[2]
    dbname = sys.argv[3]
    src_tbl = sys.argv[4]
    index_name = sys.argv[5]
    optype = sys.argv[6]
    dimension = int(sys.argv[7])
    nitem = int(sys.argv[8])
    algo = sys.argv[9]

    try:
        ret = optype2distfn[optype]
    except KeyError:
        print("unknown optyp", optype)
        sys.exit(1)

    # fix the seek to always generate the same sequence
    seek = 99
    conn = pymysql.connect(host=host, port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:

            if action == "build":
                drop_table(cursor, src_tbl)

                create_table(cursor, src_tbl, dimension)

                rs = np.random.RandomState(seek)
                insert_embed(cursor, rs, src_tbl, dimension, nitem, seek, optype)

                if algo == "hnsw":
                    create_hnsw_index(cursor, src_tbl, index_name, optype)
                else:
                    create_ivfflat_index(cursor, src_tbl, index_name, optype, nitem, False)

            elif action == "buildcdc":
                drop_table(cursor, src_tbl)

                create_table(cursor, src_tbl, dimension)

                rs = np.random.RandomState(seek)
                if algo == "hnsw":
                    create_hnsw_index(cursor, src_tbl, index_name, optype)
                    insert_embed(cursor, rs, src_tbl, dimension, nitem, seek, optype)
                else:
                    # 50% data for training centroids
                    insert_embed(cursor, rs, src_tbl, dimension, int(nitem/2), seek, optype)
                    create_ivfflat_index(cursor, src_tbl, index_name, optype, nitem, True)

                    # 50% data simply assign to centoids
                    insert_embed(cursor, rs, src_tbl, dimension, int(nitem/2), seek, optype, int(nitem/2))


            elif action == "recall":
                # concurrency thread count
                nthread = 12
                recall_run(host, dbname, src_tbl, dimension, nitem, seek, nthread, optype)
            elif action == "sample":
                nthread = 12
                sample_run(cursor, host, dbname, src_tbl, dimension, nitem, seek, nthread, optype)
            else:
                select_random_embed(cursor, src_tbl, dimension, optype)

