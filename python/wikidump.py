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
data_dir = "/tmp/wikidump"

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

def wiki2docx(wikitext, outfile):
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
        wiki2docx(p[2], outfile)
        outfiles.append(Path(outfile).as_uri())

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
    if nargv != 5:
        print("usage: wikidump.py dbname datadir wikistream-index.txt.bz2 wikistream.xml.bz2")

    dbname = sys.argv[1]
    data_dir = sys.argv[2]
    index_file = sys.argv[3]
    stream_file = sys.argv[4]

    index_uri = Path(os.path.abspath(index_file)).as_uri()
    stream_uri = Path(os.path.abspath(stream_file)).as_uri()

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            datalinks = load_index(cursor, index_uri, stream_uri)
            pages = load_wikidump_pages(cursor, datalinks)
            stream_dir = Path(stream_uri).stem
            rootdir = os.path.join(data_dir, stream_dir)
            outfiles = convert_docx(rootdir, pages)
            save_wikidump_pages(cursor, pages, outfiles)

