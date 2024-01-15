#!/usr/bin/python

import argparse
import sys

import numpy as np
import time
from dask.distributed import Client, get_worker

import composer_pandas as pd

def gen_data(total_population1, adult_population1, num_robberies1, size):
    total_population = np.ones(size, dtype="float64") * total_population1
    adult_population = np.ones(size, dtype="float64") * adult_population1
    num_robberies = np.ones(size, dtype="float64") * num_robberies1
        
    return pd.Series(total_population), pd.Series(adult_population), pd.Series(num_robberies)

def crime_index_composer(total_population1, adult_population1, num_robberies1, size, threads):
    # Get all city information with total population greater than 500,000
    total_population, adult_population, num_robberies = gen_data(total_population1, adult_population1, num_robberies1, size)
    start = time.time()
    big_cities = pd.greater_than(total_population, 500000.0)
    big_cities.dontsend = True
    big_cities = pd.mask(total_population, big_cities, 0.0)
    big_cities.dontsend = True

    double_pop = pd.multiply(adult_population, 2.0)
    double_pop.dontsend = True
    double_pop = pd.add(big_cities, double_pop)
    double_pop.dontsend = True
    multiplied = pd.multiply(num_robberies, 2000.0)
    multiplied.dontsend = True
    double_pop = pd.subtract(double_pop, multiplied)
    double_pop.dontsend = True
    crime_index = pd.divide(double_pop, 100000.0)
    crime_index.dontsend = True


    gt = pd.greater_than(crime_index, 0.02)
    gt.dontsend = True
    crime_index = pd.mask(crime_index, gt, 0.032)
    crime_index.dontsend = True
    lt = pd.less_than(crime_index, 0.01)
    crime_index = pd.mask(crime_index, lt, 0.005)
    crime_index.dontsend = True

    result = pd.pandasum(crime_index)
    pd.evaluate(workers=threads)
    exe_time = time.time() - start
    worker_address = get_worker().worker_address
    return (result.value, exe_time, worker_address)

def crime_index_pandas(total_population1, adult_population1, num_robberies1, size, threads):
    total_population, adult_population, num_robberies = gen_data(total_population1, adult_population1, num_robberies1, size)
    start = time.time()
    print(len(total_population))
    big_cities = total_population > 500000
    big_cities = total_population.mask(big_cities, 0.0)
    double_pop = adult_population * 2 + big_cities - (num_robberies * 2000.0)
    crime_index = double_pop / 100000
    crime_index = crime_index.mask(crime_index > 0.02, 0.032)
    crime_index = crime_index.mask(crime_index < 0.01, 0.005)
    criem_index_sum = crime_index.sum()
    exe_time = time.time() - start
    worker_address = get_worker().worker_address
    return (criem_index_sum, exe_time, worker_address)

def run():
    parser = argparse.ArgumentParser(description="Crime Index")
    parser.add_argument('-s', "--size", type=int, default=26, help="Size of each array")
    parser.add_argument('-p', "--piece_size", type=int, default=16384*2, help="Size of each piece.")
    parser.add_argument('-t', "--threads", type=int, default=1, help="Number of threads.")
    parser.add_argument('-m', "--mode", type=str, required=True, help="Mode (composer|naive)")
    args = parser.parse_args()

    size = (1 << args.size)
    piece_size = args.piece_size
    threads = args.threads
    mode = args.mode.strip().lower()
    
    dask_size = 30 - args.size
    dask_size = (1 << dask_size)

    assert mode == "composer" or mode == "naive"
    assert threads >= 1

    print("Size:", size)
    print("Piece Size:", piece_size)
    print("Threads:", threads)
    print("Mode:", mode)

    sys.stdout.write("Generating data...")
    sys.stdout.flush()
    print("done.")

    total_population_list = np.random.randint(0, 500001, size=dask_size)
    adult_population_list = np.random.randint(0, 500001, size=dask_size)
    num_robberies_list = np.random.randint(0, 2000, size=dask_size)
    threads_list = [threads] * dask_size
    size_list = [size] * dask_size

    client = Client(address='tcp://<scheduler-address>')
    print(client)

    start = time.time()
    if mode == "composer":
        results = []
        for i in range(0, dask_size, 2):
            future = client.map(crime_index_composer, total_population_list[i:i+1], adult_population_list[i:i+1], num_robberies_list[i:i+1], size_list[i:i+1], threads_list[i:i+1])
            results += (client.gather(future))
        
    elif mode == "naive":
        results = []
        for i in range(0, dask_size, 2):
            future = client.map(crime_index_pandas, total_population_list[i:i+1], adult_population_list[i:i+1], num_robberies_list[i:i+1], size_list[i:i+1], threads_list[i:i+1])
            results += (client.gather(future))
        
    end = time.time()

    sa_time_list = []
    worker_address_list = []
    for result in results:
        _, sa_time, worker_address = result
        sa_time_list.append(sa_time)
        worker_address_list.append(worker_address)
        
    print(f"Total Time: {end - start}")
    print(f"Worker Average Time: {np.mean(sa_time_list)}")
    print(f"worker joined: {worker_address_list}")
    client.close()

if __name__ == "__main__":
    run()

