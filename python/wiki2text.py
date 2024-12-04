import wikitextparser as wtp
import os
import sys
import csv

# installation
# pip install wikitextparser
# pip install python-docx

wikihome = os.getenv("WIKI_HOME")
if wikihome is None:
    print("WIKI_HOME environment variable not found")
    sys.exit(10)


def wiki2txt(wikitext):
    parsed = wtp.parse(wikitext)
    text = parsed.plain_text()
    return text


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("usage: python wiki2text.py infile outfile")
        sys.exit(1)

    infile = sys.argv[1]
    outfile = sys.argv[2]

    csv.field_size_limit(sys.maxsize)
    with open(infile) as csvfile:
        r = csv.reader(csvfile)
        with open(outfile, 'w') as wfile:
            writer = csv.writer(wfile)
            for row in r:
                txt = wiki2txt(row[2])
                writer.writerow((row[0], row[1], txt))
