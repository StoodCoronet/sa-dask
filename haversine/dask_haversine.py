from dask.distributed import Client, get_worker
import time
import argparse
import math

def run(composer, size, randval, threads):

    import numpy as np
    start = time.time()
    lat2 = np.ones(size, dtype="float64") * randval
    lon2 = np.ones(size, dtype="float64") * randval
    

    lat1 = 0.70984286
    lon1 = 1.23892197
    MILES_CONST = 3959.0
    a = np.zeros(len(lat2), dtype="float64")
    dlat = np.zeros(len(lat2), dtype="float64")
    dlon = np.zeros(len(lat2), dtype="float64")
    data_gen_time = time.time() - start

    if composer:
        import composer_numpy as np
    else:
        import numpy as np

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
    worker_address = get_worker().address
    return mi, exe_time, data_gen_time, worker_address

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
    
    client = Client(address='tcp://<scheduler-address>')
    print(client)
    
    dask_size = 29 - args.size
    dask_size = (1 << dask_size)
    
    composer_list = [composer] * dask_size
    size_list = [size] * dask_size
    threads_list = [threads] * dask_size

    import random
    randval_list = [random.random() for _ in range(dask_size)]

    start = time.time()
    results = []
    for i in range(0, dask_size, 2):
        future = client.map(run, composer_list[i:i+1], size_list[i:i+1], randval_list[i:i+1], threads_list[i:i+1])
        results += client.gather(future)
    
    end = time.time()
    exe_time = end - start


    length = 0
    worker_num = 0
    total_data_gen_time = 0
    total_runtime = 0
    for i, result in enumerate(results):
        out, runtime, data_gen_time, worker_address = result
        length += len(out)
        worker_num += 1
        total_data_gen_time += data_gen_time
        total_runtime += runtime
        print(f"worker id: {worker_address}, data gen: {data_gen_time}, runtime: {runtime}, {i}/{len(results)}")    
    # print(f"total time: {exe_time}, ave_data_gen: {total_data_gen_time/worker_num}, ave_runtime: {total_runtime/worker_num}, len: {length}, worker_num: {worker_num}")
    print(f"Total time: {exe_time}")
    print(f"Average Data Gen: {total_data_gen_time/worker_num}")
    print(f"Average Runtime: {total_runtime/worker_num}")
    
    client.close()