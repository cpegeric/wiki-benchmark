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

wikidump_wasm="https://github.com/cpegeric/mojo/raw/plugin_framework/plugin/wikidump/wikidump.wasm"
ollama_wasm="https://github.com/cpegeric/mojo/raw/plugin_framework/plugin/ollama/ollama.wasm"
chunk_tbl = "wiki_chunk"
page_tbl = "wiki_page"
model = "llama3.2"

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
        print("usage: embed.py dbname fragid nfrag")
        sys.exit(1)

    dbname = sys.argv[1]
    fragid = int(sys.argv[2])
    nfrag = int(sys.argv[3])

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            embedding(cursor, model, fragid, nfrag)

