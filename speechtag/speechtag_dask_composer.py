#!/usr/bin/env python
# coding: utf8
"""Example of multi-processing with Joblib. Here, we're exporting
part-of-speech-tagged, true-cased, (very roughly) sentence-separated text, with
each "sentence" on a newline, and spaces between tokens. Data is loaded from
the IMDB movie reviews dataset and will be loaded automatically via Thinc's
built-in dataset loader.

Compatible with: spaCy v2.0.0+
Last tested with: v2.1.0
Prerequisites: pip install joblib


Adapted from https://github.com/explosion/spaCy/blob/master/examples/pipeline/multi_processing.py
"""

import plac
import sys
import spacy
from spacy.util import minibatch
import thinc.extra.datasets
import time
from dask.distributed import Client, get_worker
import parser
import argparse
import os

sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer/")
sys.path.append("/home/sakaban/split-annotations/python/lib")
sys.path.append("/home/sakaban/split-annotations/python/pycomposer/")

from pycomposer import *

# @plac.annotations(
#     model=("Model name (needs tagger)", "positional", None, str),
#     n_jobs=("Number of workers", "option", "n", int),
#     batch_size=("Batch-size for each process", "option", "b", int),
#     limit=("Limit of entries from the dataset", "option", "l", int),
# )

def run(threads):
    print(f"========================================={os.getcwd()}")
    # print(sys.path)
    # total_time = plac.call(main, threads)
    s = time.time()
    total_time = main(threads)
    return time.time() - s

# @plac.annotations(
#     n_jobs=("Number of workers", "option", "n", int),
# )
def main(n_jobs=4):
    #def main(model="en_core_web_sm", n_jobs=4, batch_size=1000, limit=10000):
    model="en_core_web_sm"
    batch_size=1000
    limit=10000
    nlp = spacy.load(model)  # load spaCy model
    print("Loaded model '%s'" % model)

    # load and pre-process the IMDB dataset
    sys.stdout.write("Loading IMDB data...")
    data, _ = thinc.extra.datasets.imdb()
    print("done.")
    texts, _ = zip(*data[-limit:])

    start = time.time()
    process(nlp, texts)
    evaluate(workers=n_jobs, batch_size=batch_size)
    end = time.time()
    print("Total:", end - start)
    total_time = end - start
    return total_time #, texts
    

class TextBatchSplit(SplitType):
    def combine(self, values):
        """ No need to combine text batches"""
        pass

    def split(self, start, end, texts):
        return minibatch(texts, size=(end-start))

@sa((Broadcast(), TextBatchSplit()), {}, Broadcast())
def process(nlp, texts):
    print(nlp.pipe_names)
    for doc in nlp.pipe(texts):
        sentence = " ".join(represent_word(w) for w in doc if not w.is_space)
        sentence += "\n"

def represent_word(word):
    text = word.text
    # True-case, i.e. try to normalize sentence-initial capitals.
    # Only do this if the lower-cased form is more probable.
    if (
        text.istitle()
        and is_sent_begin(word)
        and word.prob < word.doc.vocab[text.lower()].prob
    ):
        text = text.lower()
    return text + "|" + word.tag_

def is_sent_begin(word):
    if word.i == 0:
        return True
    elif word.i >= 2 and word.nbor(-1).text in (".", "!", "?", "..."):
        return True
    else:
        return False

if __name__ == "__main__":
    # plac.call(main)
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer/")
    # sys.path.append("/home/sakaban/split-annotations/python/lib")
    # sys.path.append("/home/sakaban/split-annotations/python/pycomposer/")
    sys.path.append('../../../split-annotations/python/pycomposer/')
    from pycomposer import *
    print(os.getcwd())
    parser = argparse.ArgumentParser(
        description="Speech tag."
    )
    parser.add_argument('-t', "--threads", type=int, default=1, help="Number of threads.")
    parser.add_argument('-v', "--verbosity", type=str, default="none", help="Log level (debug|info|warning|error|critical|none)")
    args = parser.parse_args()
    
    threads = args.threads
    loglevel = args.verbosity
    
    
    client = Client(address='tcp://192.168.1.102:8786')
    print(client)

    model="en_core_web_sm"
    batch_size = 1000
    limit = 10000
    
    start = time.time()
    
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer/")
    sys.path.append("/home/sakaban/split-annotations/python/lib")
    sys.path.append("/home/sakaban/split-annotations/python/pycomposer/")

    from pycomposer import *
    
    future1 = client.submit(run, threads, workers='w1')
    # future2 = client.submit(run, threads)
    result1 = future1.result()
    # result2 = future2.result()
    print(result1)
    # print(result2)
    print(f"total time: {time.time() - start}")
    
    # main(model="en_core_web_sm", n_jobs=threads, batch_size=1000, limit=10000)
    # main(n_jobs=threads)
    
    