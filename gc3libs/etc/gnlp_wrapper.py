#!/usr/bin/env python

from __future__ import absolute_import, print_function, unicode_literals
from builtins import str
import sys
import os
import subprocess

from xml.etree import cElementTree as ET
from xml.etree.ElementTree import Element, SubElement, Comment, tostring

SENTIMENTS = [
    'neutral',
    'negative',
    'very negative',
    'positive',
    'very positive']
FIELD = "pdid"
CONTENT = "content"


def Usage():
    print("Usage: wrapper.py <input_file> <output_file>")


def RunParser(input, output):
    if not os.path.isfile(input):
        print("Input file %s not found" % input)
        return 1

    print("Parsing input file... ")

    tree = ET.parse(input)
    root = tree.getroot()

    try:
        fd = open('input_contents.xml', 'w+')

        # Generate simplified version of the input XML file
        for row in list(root):
            field = row.find(FIELD).text
            content = (row.find(CONTENT).text).encode('utf8')
            fd.write("%s\n%s\n" % (field, content))

        fd.close()
    except OSError as osx:
        # Raise and exit
        print("Failed while preparing simplified Content file. Error: %s" % str(osx))
        return 1

    command = '/usr/bin/java -cp "$CORENLP/*" edu.stanford.nlp.sentiment.SentimentPipeline -file input_contents.xml'
    nlp = subprocess.Popen(
        [command],
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE)

    print("Running command %s" % command)
    (stdout, stderr) = nlp.communicate()

    if nlp.returncode != 0:
        print("Execution failed with exit code: %d" % nlp.returncode)
        try:
            res = open(output, 'w+')
            res.write(stderr)
            res.close()
        except Exception as ex:
            print("Failed writing output file %s. Error type %s. Message: %s" % (output, type(ex), str(ex)))
            print("Failed CoreNLP execution with: %s" % stderr)
        return nlp.returncode

    print("Parsing results... ")

    index = 0
    row = None
    sentiment = ""

    for line in [l.strip() for l in stdout.split('\n')]:
        if line.isdigit():
            if row:
                child = SubElement(row, "Sentiment")
                child.tail = "\n"
                child.text = sentiment[1:]
                sentiment = ""
            index = line
            row = root.find("./row/[%s='%s']" % (FIELD, index))
        elif line.lower() in SENTIMENTS:
            sentiment += ",%s" % line

    # Add last sentiment segment to the last ROW element
    child = SubElement(row, "Sentiment")
    child.tail = "\n"
    child.text = sentiment[1:]

    print("Writing results to output file... ")
    tree.write(output)

    print("Done")
    return 0

if __name__ == '__main__':
    if (len(sys.argv) != 3):
        sys.exit(Usage())
    sys.exit(RunParser(sys.argv[1], sys.argv[2]))
