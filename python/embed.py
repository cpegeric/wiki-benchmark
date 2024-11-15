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

model = "llama3.2"

def embedding(cursor, src_tbl, chunk_tbl, model, fragid, nfrag):
    cfg = """{"model":"%s"}""" % model
    sql = "select src.wiki_id, json_unquote(json_extract(f.result, '$.chunk')), \
            json_unquote(json_extract(f.result, '$.embedding')) from %s as src CROSS APPLY \
            moplugin_table('%s', 'embed', '%s', src.src) as f where mod(src.id, %d) = %d" % (src_tbl,  ollama_wasm, cfg, nfrag, fragid)
    print(sql)
    cursor.execute(sql)
    results = cursor.fetchall()

    sql = "insert into %s (wiki_id, text, embed) values (%s)" % (chunk_tbl, "%s, %s, %s, %s")
    cursor.executemany(sql, results)


if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 6:
        print("usage: embed.py dbname src_tbl chunk_tbl fragid nfrag")
        print("e.g python3 embed.py zh wiki_docx wiki_docx_chunk 0 3")
        sys.exit(1)

    dbname = sys.argv[1]
    src_tbl = sys.argv[2]
    chunk_tbl = sys.argv[3]
    fragid = int(sys.argv[4])
    nfrag = int(sys.argv[5])

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            embedding(cursor, src_tbl, chunk_tbl, model, fragid, nfrag)

