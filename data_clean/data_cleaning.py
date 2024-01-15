#!/usr/bin/python

# The usual preamble
import numpy as np
import time
import argparse

from dask.distributed import Client

import composer_pandas as pd

def gen_data(size):
    values = ["1234567" for  _ in range(size)]
    return pd.Series(data=values)

def datacleaning_pandas(size):
    start = time.time()
    requests = gen_data(size)
    data_gen_time = time.time() - start
    start = time.time()
    requests = requests.str.slice(0, 5)
    zero_zips = requests == "00000"
    requests = requests.mask(zero_zips, np.nan)
    requests = requests.unique()
    compute_time = time.time() - start
    return (requests, data_gen_time, compute_time)

def datacleaning_composer(size, threads):
    start = time.time()
    requests = gen_data(size)
    data_gen_time = time.time() - start
    start = time.time()
    # Fix requests with extra digits
    requests = pd.series_str_slice(requests, 0, 5)
    requests.dontsend = True

    # Fix requests with 00000 zipcodes
    zero_zips = pd.equal(requests, "00000")
    zero_zips.dontsend = True
    requests = pd.mask(requests, zero_zips, np.nan)
    requests.dontsend = True
    requests = pd.unique(requests)
    pd.evaluate(workers=threads)
    requests = requests.value
    compute_time = time.time() - start
    return (requests, data_gen_time, compute_time)

def run():
    parser = argparse.ArgumentParser(
        description="Data Cleaning"
    )
    parser.add_argument('-s', "--size", type=int, default=26, help="Size of each array")
    parser.add_argument('-p', "--piece_size", type=int, default=16384*2, help="Size of each piece.")
    parser.add_argument('-t', "--threads", type=int, default=1, help="Number of threads.")
    parser.add_argument('-v', "--verbosity", type=str, default="none", help="Log level (debug|info|warning|error|critical|none)")
    parser.add_argument('-m', "--mode", type=str, required=True, help="Mode (composer|naive)")
    args = parser.parse_args()

    size = (1 << args.size)
    piece_size = args.piece_size
    threads = args.threads
    loglevel = args.verbosity
    mode = args.mode.strip().lower()

    assert mode == "composer" or mode == "naive"
    assert threads >= 1

    print("Size:", size)
    print("Piece Size:", piece_size)
    print("Threads:", threads)
    print("Log Level", loglevel)
    print("Mode:", mode)

    client = Client(address='tcp://<scheduler-address>')
    print(client)
    client.restart()
    
    dask_size = 31 - args.size
    dask_size = (1 << dask_size)
    
    size_list = [size] * dask_size
    threads_list = [threads] * dask_size

    start = time.time()
    if mode == "composer":
        results = []
        for i in range(0, dask_size, 2):
            futures = client.map(datacleaning_composer, size_list[i:i+1], threads_list[i:i+1])
            results += client.gather(futures)
    elif mode == "naive":
        results = []
        for i in range(0, dask_size, 2):
            futures = client.map(datacleaning_pandas, size_list[i:i+1])
            results += client.gather(futures)
    end = time.time()
    
    data_gen_time_list = []
    compute_time_list = []
    for result in results:
        _, data_gen_time, compute_time = result
        data_gen_time_list.append(data_gen_time)
        compute_time_list.append(compute_time)
    
    print(f"Average Data Gen:     {np.mean(data_gen_time_list)}")
    print(f"Average Compute Time: {np.mean(compute_time_list)}")
    print(f"Total time:           {end - start}")
    client.close()
    

if __name__ == "__main__":
    run()

