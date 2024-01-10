"""
ENV: sa36 on sakaban

For scheduler, run: 
dask-scheduler

For worker, run: 
dask-worker tcp://192.168.1.102:8786 --no-nanny --worker-port 21000
dask-worker tcp://192.168.1.102:8786 --no-nanny --worker-port 20000
"""

from dask.distributed import Client, get_worker
from dask_jobqueue import SLURMCluster, PBSCluster
import time
import argparse
import math
import sys

def run(composer, size, threads):
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer")
    sys.path.append("/home/sakaban/split-annotations/python/lib")
    sys.path.append("/home/sakaban/split-annotations/python/pycomposer")
    if composer:
        import composer_numpy as np
    else:
        import numpy as np

    start = time.time()
    lat2 = np.ones(size, dtype="float64") * 0.0698132
    lon2 = np.ones(size, dtype="float64") * 0.0698132
    
    lat1 = 0.70984286
    lon1 = 1.23892197
    MILES_CONST = 3959.0
    a = np.zeros(len(lat2), dtype="float64")
    dlat = np.zeros(len(lat2), dtype="float64")
    dlon = np.zeros(len(lat2), dtype="float64")
    data_gen_time = time.time() - start

    start = time.time()
    np.subtract(lat2, lat1, out=dlat)
    np.subtract(lon2, lon1, out=dlon)

    # dlat = sin(dlat / 2.0) ** 2.0
    np.divide(dlat, 2.0, out=dlat)
    np.sin(dlat, out=dlat)
    np.multiply(dlat, dlat, out=dlat)

    # a = cos(lat1) * cos(lat2)
    lat1_cos = math.cos(lat1)
    np.cos(lat2, out=a)
    np.multiply(a, lat1_cos, out=a)

    # a = a + sin(dlon / 2.0) ** 2.0
    np.divide(dlon, 2.0, out=dlon)
    np.sin(dlon, out=dlon)
    np.multiply(dlon, dlon, out=dlon)
    np.multiply(a, dlon, out=a)
    np.add(dlat, a, out=a)
    
    c = a
    np.sqrt(a, out=a)
    np.arcsin(a, out=a)
    np.multiply(a, 2.0, out=c)

    mi = c
    np.multiply(c, MILES_CONST, out=mi)

    if composer:
        np.evaluate(workers=threads)
    
    end = time.time()
    exe_time = end - start
    worker_id = get_worker().id
    return mi, exe_time, data_gen_time, worker_id

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Haversine distance computation."
    )
    parser.add_argument('-s', "--size", type=int, default=26, help="Size of each array")
    parser.add_argument('-p', "--piece_size", type=int, default=16384, help="Size of each piece.")
    parser.add_argument('-t', "--threads", type=int, default=1, help="Number of threads.")
    parser.add_argument('-v', "--verbosity", type=str, default="none", help="Log level (debug|info|warning|error|critical|none)")
    parser.add_argument('-m', "--mode", type=str, required=True, help="Mode (composer|naive)")
    args = parser.parse_args()

    size = (1 << args.size)
    piece_size = args.piece_size
    threads = args.threads
    loglevel = args.verbosity
    mode = args.mode.strip().lower()

    print("Size:", size)
    print("Piece Size:", piece_size)
    print("Threads:", threads)
    print("Log Level", loglevel)
    print("Mode:", mode)
    
    if mode == "composer":
        composer = True
    elif mode == "naive":
        composer = False
    else:
        raise ValueError("unknown mode", mode)
    
    client = Client(address='tcp://192.168.1.102:8786')
    print(client)
    
    dask_size = 28 - args.size
    dask_size = (1 << dask_size)
    
    composer_list = [composer] * dask_size
    size_list = [size] * dask_size
    threads_list = [threads] * dask_size
    # worker_list = ['tcp://192.168.1.102:21000', 'tcp://192.168.1.104:20000',]
    # worker_list = ['w1', 'w2',]
    # print(client.run(lambda: get_worker().name))
    
    start = time.time()
    future = client.map(run, composer_list, size_list,threads_list)
    results = client.gather(future)
    
    print('\n::: start user output :::')
    print(client)
    print('::: end user output :::\n')
    
    
    # futures = []
    # for i in range(int(dask_size / 2)):
    #     # composer_scattered = client.scatter(composer_list[i])
    #     # size_scattered = client.scatter(size_list[i])
    #     # threads_scattered = client.scatter(threads_list[i])
    #     # if i >= dask_size / 2:
    #     #     w = 'w1'
    #     #     print('w1')
    #     #     future = client.submit(run, composer_list[i], size_list[i], threads_list[i], workers='w1') #, workers=[worker_list[i % 2]]
    #     # else:
    #     #     w = 'w2'
    #     #     print('w2')
    #     #     future = client.submit(run, composer_list[i], size_list[i], threads_list[i], workers='w2') #, workers=[worker_list[i % 2]]
    #     # futrue = client.submit(run, composer_scattered, size_scattered, threads_scattered, i, workers=w) #, workers=[worker_list[i % 2]]
    #     future = client.submit(run, composer_list[i], size_list[i], threads_list[i], workers='w1') #, workers=[worker_list[i % 2]]
    #     futures.append(future)
    #     result = future.result()
    #     out, runtime, data_gen_time, worker_id = result
    #     print(f"worker id: {worker_id}, data gen: {data_gen_time}, runtime: {runtime}")
    # results1 = [future.result() for future in futures]
    
    # futures = []
    # for i in range(int(dask_size / 2)):
    #     # composer_scattered = client.scatter(composer_list[i])
    #     # size_scattered = client.scatter(size_list[i])
    #     # threads_scattered = client.scatter(threads_list[i])
    #     # if i >= dask_size / 2:
    #     #     w = 'w1'
    #     # else:
    #     #     w = 'w2'
    #     w = 'w2'
    #     # future = client.submit(run, composer_scattered, size_scattered, threads_scattered, workers='w2') #, workers=[worker_list[i % 2]]
    #     futrue = client.submit(run, composer_list[i], size_list[i], threads_list[i], workers='w2') #, workers=[worker_list[i % 2]]
    #     futures.append(future)
    #     result = future.result()
    #     out, runtime, data_gen_time, worker_id = result
    #     print(f"worker id: {worker_id}, data gen: {data_gen_time}, runtime: {runtime}")
    # results2 = [future.result() for future in futures]
    
    
    end = time.time()
    exe_time = end - start


    length = 0
    worker_num = 0
    total_data_gen_time = 0
    total_runtime = 0
    for i, result in enumerate(results):
        out, runtime, data_gen_time, worker_id = result
        length += len(out)
        worker_num += 1
        total_data_gen_time += data_gen_time
        total_runtime += runtime
        print(f"worker id: {worker_id}, data gen: {data_gen_time}, runtime: {runtime}, {i}/{len(results)}")
    # for i, result in enumerate(results1):
    #     out, runtime, data_gen_time, worker_id = result
    #     length += len(out)
    #     worker_num += 1
    #     total_data_gen_time += data_gen_time
    #     total_runtime += runtime
    #     print(f"worker id: {worker_id}, data gen: {data_gen_time}, runtime: {runtime}, {i}/{len(results1)}")
    # for i, result in enumerate(results2):
    #     out, runtime, data_gen_time, worker_id = result
    #     length += len(out)
    #     worker_num += 1
    #     total_data_gen_time += data_gen_time
    #     total_runtime += runtime
    #     print(f"worker id: {worker_id}, data gen: {data_gen_time}, runtime: {runtime}, {i}/{len(results2)}")
    
    print(f"total time: {exe_time}, ave_data_gen: {total_data_gen_time/worker_num}, ave_runtime: {total_runtime/worker_num}, len: {length}, worker_num: {worker_num}")

    client.close()