import pymysql.cursors
import sys, os
import tempfile
from pathlib import Path
import subprocess
import wikitextparser as wtp
from docx import Document
import fileinput

# installation
# pip install wikitextparser
# pip install python-docx

wikihome = os.getenv("WIKI_HOME")
if wikihome is None:
    print("WIKI_HOME environment variable not found")
    sys.exit(10)

#wikidump_wasm="https://github.com/cpegeric/wikidump-wasm/raw/main/wikidump/wikidump.wasm"
#ollama_wasm="https://github.com/cpegeric/ollama-wasm/raw/main/ollama/ollama.wasm"

wikidump_wasm=Path(os.path.join(wikihome, "wasm/wikidump.wasm")).as_uri()
ollama_wasm=Path(os.path.join(wikihome, "wasm/ollama.wasm")).as_uri()

model = "llama3.2"

def generate(cursor, chunk_tbl, model, line):
    cfg = """{"model":"%s"}""" % model
    sql = "select json_unquote(json_extract(result, '$.embedding')) from moplugin_table('%s', 'embed', '%s', '%s') as f" % (ollama_wasm, cfg, line)
    cursor.execute(sql)
    results = cursor.fetchall()
    #print(results[0][0])

    sql = "select text from %s order by l2_distance(embed, '%s') asc limit 2" % (chunk_tbl, results[0][0])
    cursor.execute(sql)
    results = cursor.fetchall()
    #print(results)

    prompt = "Questions: %s\n Please answer in Chinese with the context below.\n" % line
    number="{}. "
    i=1
    for r in results:
        prompt += number.format(i)
        prompt += r[0]
        prompt += "\n"
        i+=1

    #print(prompt)

    sql = "select json_unquote(result) from moplugin_table(%s, 'generate', %s, %s) as f"
    cursor.execute(sql, (ollama_wasm, cfg, prompt))
    results = cursor.fetchall()
    return results[0][0]

if __name__ == "__main__":
    nargv = len(sys.argv)
    if nargv != 3:
        print("usage: generate.py dbname chunk_tbl")
        sys.exit(1)

    dbname = sys.argv[1]
    chunk_tbl = sys.argv[2]

    conn = pymysql.connect(host='localhost', port=6001, user='root', password = "111", database=dbname, autocommit=True)
    with conn:
        with conn.cursor() as cursor:
            try:
                print(">> Please ask any question.\n>> ", end="")
                for line in fileinput.input(files=('-')):
                    if line == "quit\n":
                        sys.exit(0)
                    answer = generate(cursor, chunk_tbl, model, line)
                    print(answer.replace("\\n", "\n"))
                    print()
                    print(">> Please ask any question.\n>> ", end="")
            except KeyboardInterrupt:
                sys.exit(0)

