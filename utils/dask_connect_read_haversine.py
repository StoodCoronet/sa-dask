"""
ENV: sa36 on sakaban

For scheduler, run: 
dask-scheduler

For worker, run: 
dask-worker tcp://192.168.1.102:8786 --no-nanny --worker-port 21000
dask-worker tcp://192.168.1.102:8786 --no-nanny --worker-port 20000



"""

from dask.distributed import Client, get_worker
# import numpy as np
# import sa.annotated.numpy as np
# import sa_source.composer_numpy as np
import time
import argparse
import math
import sys

sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer")
sys.path.append("/home/sakaban/split-annotations/python/lib")
sys.path.append("/home/sakaban/split-annotations/python/pycomposer")

def run(composer, size, threads, tmp): #, lat2, lon2):
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer")
    sys.path.append("/home/sakaban/split-annotations/python/lib")
    sys.path.append("/home/sakaban/split-annotations/python/pycomposer")
    if composer:
        # import sa.annotated.numpy as np
        import composer_numpy as np
    else:
        import numpy as np
        
    # lat2 = np.ones(size, dtype="float64") * 0.0698132
    lat2 = np.ones(size, dtype="float64") * tmp
    lon2 = np.ones(size, dtype="float64") * 0.0698132
    
    if composer:
        np.evaluate(workers=threads)
    
    lat1 = 0.70984286
    lon1 = 1.23892197
    MILES_CONST = 3959.0
    # start = time.time()
    a = np.zeros(len(lat2), dtype="float64")
    dlat = np.zeros(len(lat2), dtype="float64")
    dlon = np.zeros(len(lat2), dtype="float64")
    # end = time.time()
    # print("Allocation time:", end-start)

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
    print(f"worker_id: {worker_id}")
    # print(f"threads: {threads}, time: {exe_time}, c: {c}")
    return (mi, exe_time)

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
    
    if composer:
        # import sa.annotated.numpy as np
        import composer_numpy as np
    else:
        import numpy as np
    # lats = np.ones(size, dtype="float64") * 0.0698132
    # lons = np.ones(size, dtype="float64") * 0.0698132

    client = Client('tcp://192.168.1.102:8786')
    print(client)
    start = time.time()
    
    dask_size = 6
    dask_size = (1 << dask_size)
    
    step = size / dask_size
    
    
    composer_list = [composer] * dask_size
    size_list = [size] * dask_size
    threads_list = [threads] * dask_size
    start_list = [i for i in range(0, size)]
    step_list = [step] * dask_size
    
    # future = client.map(run, composer_list, size_list, threads_list, num_list)
    future = client.map(run, composer_list, size_list, threads_list, start_list, step_list)
    
    results = client.gather(future)
    
    
    # result, t = client.submit(run, composer, size, threads).result()
    end = time.time()
    exe_time = end - start
    
    num_list = []
    for result in results:
        # result1, result2, result3 = result
        # num_list.append(result3)
        result1, result2= result
        print(f"exe_time: {exe_time}, t: {result2}, result: {len(result1)}")
    
    print(len(results))
    print(num_list)
    # print(t)

    client.close()