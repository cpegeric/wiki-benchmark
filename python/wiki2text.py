import wikitextparser as wtp
import os
import sys

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

    wiki = sys.stdin.read()
    text = wiki2txt(wiki)
    print(text, end="")
