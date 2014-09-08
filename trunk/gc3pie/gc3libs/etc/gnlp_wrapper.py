#!/usr/bin/env python

import sys
import os
import subprocess

from xml.etree import cElementTree as ET
from xml.etree.ElementTree import Element, SubElement, Comment, tostring

SENTIMENTS = ['neutral','negative','very negative','positive','very positive']

def Usage():
    print ("Usage: wrapper.py <input_file> <output_file>")

def RunParser(input,output):
    if not os.path.isfile(input):
        print "Input file %s not found" % input
        return 1

    print "Parsing input file... "

    tree = ET.parse(input)
    root = tree.getroot()

    try:
        fd = open('input_contents.xml','w+')

        # Generate simplified version of the input XML file
        for row in list(root):
            field = row.find('FIELD1').text
            content = row.find('Content').text
            fd.write("%s\n%s\n" % (field,content))

        fd.close()
    except OSError, osx:
        # Raise and exit
        print "Failed while preparing simplified Content file. Error: %s" % str(osx)

    command = '/usr/bin/java -cp "$CORENLP/*" edu.stanford.nlp.sentiment.SentimentPipeline -file input_contents.xml'
    # nlp = subprocess.Popen(['/usr/bin/java','-cp','"$CORENLP/*"','edu.stanford.nlp.sentiment.SentimentPipeline','-file','%s' % input], shell=True, stderr=subprocess.PIPE,stdout=subprocess.PIPE)
    nlp = subprocess.Popen([command], shell=True, stderr=subprocess.PIPE,stdout=subprocess.PIPE)

    print "Running command %s" % command
    (stdout,stderr) = nlp.communicate()

    if nlp.returncode != 0:
        print "Execution failed with exit code: %d" % nlp.returncode
        try:
            res = open(output,'w+')
            res.write(stderr)
            res.close()
        except Exception, ex:
            print "Failed writing output file %s. Error type %s. Message: %s" % (output,type(ex),str(ex))
            print "Failed CoreNLP execution with: %s" % stderr
        return nlp.returncode

        # "1 I like this.\n  Neutral\n2 I like this.\n  Neutral\n3 I don't like this.\n  Negative\n4 I dislike this.\n  Neutral\n5 I'm ambiguous.\n  Neutral\n6 parlo in italiano.\n  Neutral\ncome stai?\n  Neutral\n7 io bene, tu ?\n  Negative\n"



    print "Parsing results... "

    # sentiments = dict()
    index = 0
    row = None

    for line in [l.strip() for l in stdout.split('\n')]:
        if line.isdigit():
            index = line
            # Get the corresponding row
            # row = [row for row in root.findall("./ROW") if row.find('FIELD1').text == index][0]
            row = root.find("./ROW/[FIELD1='%s']" % index)

            # # This is an index
            # if sentiments.has_key(index):
            #     # Duplicate entry ?
            #     print "Duplicate entry %s" % index
            # else:
            #     print "Adding comment ID %s" % index
            #     sentiments[index] = ""
        elif line.lower() in SENTIMENTS:
            if index == 0 or not row:
                # raise an error and exit
                print "Inconsistent output file. index: %s row: %s sentiment: %s" % (index,str(row),line)
                raise Exception("Inconsistent output file. index: %s row: %s sentiment: %s" % (index,str(row),line))


            child = SubElement(row,"Sentiment")
            child.tail = "\n"
            child.text = line


    print "Writing results to output file... "
    # Write results
    # fd = open(output,'w+')
    tree.write(output)
    # for item in sentiments.keys():
    #     fd.write("%s@%s\n" % (item,sentiments[item][:-1]))
    # fd.close()
        
    print "Done"
    return 0

if __name__ == '__main__':
    if (len(sys.argv) != 3):
        sys.exit(Usage())
    sys.exit(RunParser(sys.argv[1],sys.argv[2]))
