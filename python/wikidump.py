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
index_tbl = "wiki_index"
page_tbl = "wiki_page"
data_dir = "/tmp/wikidump"


def create_tables(cursor):
    create_page_table_sql = "create table wiki_page (id bigint, title varchar, src datalink)"
    cursor.execute(create_page_table_sql)


def load_index(cursor, indexfile, streamfile):
    sql = "select concat('%s?offset=', json_unquote(json_extract(result, '$.offset')), '&size=', \
json_unquote(json_extract(result, '$.size'))) from moplugin_table('%s', 'get_index', null, \
cast('%s' as datalink)) as f" % (streamfile, wikidump_wasm, indexfile)
    #print(sql)
    cursor.execute(sql)
    results = cursor.fetchall()
    datalinks = []
    for row in results:
        datalinks.append(row[0])

    return datalinks


def load_wikidump_pages(cursor, datalinks):
    #print("load wikidump pages")

    sql = "select json_unquote(json_extract(result, '$.id')), json_unquote(json_extract(result, '$.title')), \
    json_unquote(json_extract(result, '$.revision.text')) from moplugin_table('%s', 'get_pages', null, \
    cast('%s' as datalink)) as f"

    pages = []
    for dl in datalinks:
        s = sql % (wikidump_wasm, dl)
        cursor.execute(s)
        results = cursor.fetchall()
        pages.extend(results)
        
    return pages

def wikitextparser(wikitext, outfile):
    parsed = wtp.parse(wikitext)
    text = parsed.plain_text()
    document = Document()
    document.add_paragraph(text)
    document.save(outfile)


def convert_docx(rootdir, pages):
    #print(rootdir)
    if not os.path.exists(rootdir):
        os.mkdir(rootdir)
    outfiles = []
    for p in pages:
        outfile = os.path.join(rootdir, "%s.docx" %(p[0]))
        #print("id=%s, title=%s" % (p[0], p[1]))
        #print(outfile)
        wikitextparser(p[2], outfile)
        outfiles.append("file://%s" % outfile)

    return outfiles

def save_wikidump_pages(cursor, pages, outfiles):
    sql = "insert into wiki_page values (%s, %s, %s)"
    val = []
    for p, f in zip(pages, outfiles):
        val.append((p[0], p[1], f))
    #print(val)

    cursor.executemany(sql, val)
        
if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 4:
        print("usage: wiki.py [create|load] wikistream-index.txt.bz2 wikistream.xml.bz2")

    cmd = sys.argv[1]
    index_file = sys.argv[2]
    stream_file = sys.argv[3]

    index_uri = Path(os.path.abspath(index_file)).as_uri()
    stream_uri = Path(os.path.abspath(stream_file)).as_uri()

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            if cmd == "create":
                create_tables(cursor)
            elif cmd == "load":
                datalinks = load_index(cursor, index_uri, stream_uri)
                pages = load_wikidump_pages(cursor, datalinks)
                stream_dir = Path(stream_uri).stem
                rootdir = os.path.join(data_dir, stream_dir)
                outfiles = convert_docx(rootdir, pages)
                save_wikidump_pages(cursor, pages, outfiles)
            else:
                print("command not supported")

