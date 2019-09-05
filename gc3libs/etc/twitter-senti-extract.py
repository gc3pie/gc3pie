from __future__ import absolute_import, print_function, unicode_literals
from builtins import range
from past.builtins import basestring
import bz2
import json
import os
import re
import sys
import time
import tarfile
from multiprocessing import Manager, Pool, Process, current_process
from zipfile import ZipFile, is_zipfile

import pandas
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()


def parse_magic_ass_json(data):
    """
    The JSON format of twitter JSON files is JSONL-ish, parse it to lines of tweets (kv pairs)
    :param data: twitter json file
    :return: list of tweets (as kv pairs)
    """
    result = []
    print('parsing json (size: {0})'.format(len(data)))
    try:
        data = bz2.decompress(data)
        print('decompressed data to {0}'.format(len(data)))
    except IOError:
        pass
    for line in data.split('\n'):
        try:
            result.append(json.loads(line))
        except ValueError:
            pass
    return result


def extract_tweets(lines):
    """
    Extract the sentiment from a bunch of tweets
    :param lines: buch of lines containing kv pairs of twitter data
    :return: mean of negative, positive, neutral and compound sentiment in the lines
    """
    skipped = 0
    result = []
    for tweet in lines:
        if 'text' in tweet:
            scores = analyzer.polarity_scores(tweet.get('text'))
        elif 'body' in tweet:
            scores = analyzer.polarity_scores(tweet.get('body'))
        else:
            skipped += 1
            continue
        result.append([scores.get('neg'), scores.get('pos'), scores.get('neu'), scores.get('compound')])
    print('{0} tweets with no text/body field ({1} total), they have been skipped'.format(skipped, len(lines)))
    df = pandas.DataFrame(result, columns=['negative', 'positive', 'neutral', 'compound'])
    return df['negative'].mean(), df['positive'].mean(), df['neutral'].mean(), df['compound'].mean()


def process_twitter_json_file(archive_path, y, m, fq, wq):
    """
    Worker that processes a single twitter json file from the archive   
    :param archive_path: name of the archive
    :param y: cached year value
    :param m: cached month value
    :param fq: queue to read from
    :param wq: queue to write to
    """

    print('Starting process_twitter_json_file named {0}'.format(current_process().name))

    while True:
        archive_file = fq.get()
        if isinstance(archive_file, basestring) and archive_file == 'EOP':
            break
        else:
            print('going to extract archive file {0} from {1}'.format(archive_file.name, archive_path))
            with tarfile.open(archive_path) as tf:
                extracted_archive_file = tf.extractfile(archive_file)
                if not extracted_archive_file.closed:
                    json_data = extracted_archive_file.read()
                    data = parse_magic_ass_json(json_data)
                    extracted_archive_file.close()
                    neg, pos, neu, com = extract_tweets(data)
                    if len(archive_file.name.split('/')) == 5:
                        yr, mt, day, hour, p = archive_file.name.split('/')
                    elif len(archive_file.name.split('/')) == 4 and year:
                        yr = y
                        mt, day, hour, p = archive_file.name.split('/')
                    elif len(archive_file.name.split('/')) == 3 and year and month:
                        yr = y
                        mt = m
                        day, hour, p = archive_file.name.split('/')
                    else:
                        raise ValueError('cannot infer date from filename and/or directory structure')
                    wq.put('{y}.{m}.{d}.{h}.{p};{neg};{pos};{neu};{com};\n'.format(y=yr,
                                                                                   m=mt,
                                                                                   d=day,
                                                                                   p=p.split('.')[0],
                                                                                   h=hour,
                                                                                   neg=neg,
                                                                                   pos=pos,
                                                                                   neu=neu,
                                                                                   com=com))
                else:
                    print('file pointer not opened, something went wrong for {0} in {1}, skipping..,'.format(archive_file.name, archive_path))


def mp_writer(o, wq):
    """
    Multiprocess listener. Listens to message on Queue and writes them to file
    :param o: name of the output file
    :param wq: queue to read from 
    """
    with open(o, 'wb') as fp:
        while True:
            m = wq.get()
            if m == 'EOP':
                fp.flush()
                break
            fp.write(m)
            fp.flush()


def queue_archive_files(archive_path, fq):
    """
    Extract all data and process everything :)
    :param archive_path: name of the archive
    :param fq: puts the file names on the queue 
    """
    if is_zipfile(archive_path):
        with ZipFile(archive_path, 'r') as archive:
            for archive_file in archive.namelist():
                if not archive_file.endswith('/') and ZipFile.getinfo(archive, archive_file).file_size > 0:
                    fq.put(archive_file)
    elif tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as archive:
            for archive_file in archive:
                if archive_file.isreg() and archive_file.size > 0:
                    fq.put(archive_file)


if __name__ == "__main__":
    # wait {timeout} seconds for our file to become available
    timeout = 360
    if len(sys.argv) == 3:
        while not os.path.exists(sys.argv[1]):
            timeout -= 1
            if timeout > 0:
                print('file {0} not available yet, waiting {1} more seconds'.format(sys.argv[1], timeout))
            else:
                print('timout exceeded, terminating program, please check configuration for issues')
                exit(-2)
            time.sleep(1)
        year = None
        month = None
        match = re.search(r'([0-9]{4})-([0-9]{2})', sys.argv[1])
        if len(match.groups()) == 2:
            year = match.group(1)
            month = match.group(2)
            print('found something resembling a year/month thingy ({0}-{1})in the file name, caching it.'.format(year, month))
        print('setting up multiprocessing')
        manager = Manager()
        file_queue = manager.Queue()
        writer_queue = manager.Queue()
        print('Setup our file indexer')
        indexer = Process(target=queue_archive_files, args=(sys.argv[1], file_queue,))
        indexer.start()
        print('Setup our output writer')
        writer = Process(target=mp_writer, args=('{fn}.csv'.format(fn=os.path.basename(sys.argv[1])), writer_queue,))
        writer.start()
        print('Setup file processors and start crunching')
	jobs = []
	for index in range(0,int(sys.argv[2])):
	    jobs.append(Process(target=process_twitter_json_file, args=(sys.argv[1], year, month, file_queue, writer_queue,)))
   	for job in jobs:
	    job.start()
	    print('Started process {0} in pid {1}'.format(job.name,job.pid))
	print('wait for our indexer to finish')
        indexer.join()
        print('add terminators to index file queue (as last element)')
        for job in jobs:
            file_queue.put('EOP')
        print('wait for our processors to finnish')
        for job in jobs:
            job.join()
        print('and now add terminator for writer and wait to finish')
        writer_queue.put('EOP')
        writer.join()
        print('Elvis has left the building')
    else:
        print('Invalid argument count, should only be one single archive and a cpu core count.')
        exit(-1)


