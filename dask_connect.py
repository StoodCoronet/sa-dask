"""
ENV: sa36 on sakaban

For scheduler, run: dask-scheduler

For worker, run: dask-worker tcp://192.168.1.102:8786 --no-nanny
"""

from dask.distributed import Client
import time
import argparse

def run(composer, size, threads, a, b, c):
    if composer:
        import sa.annotated.numpy as np
    else:
        import numpy as np
    start = time.time()
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)

    if composer:
        np.evaluate(workers=threads)
    
    end = time.time()
    exe_time = end - start
    
    # print(f"threads: {threads}, time: {exe_time}, c: {c}")
    return c, exe_time

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
        import sa.annotated.numpy as np
    else:
        import numpy as np
    a = np.random.random(size)
    b = np.random.random(size)
    c = np.random.random(size)

    client = Client('tcp://192.168.1.102:8786')
    print(client)
    start = time.time()
    # result, t = client.submit(run, composer, size, threads, a, b, c, workers='tcp://192.168.1.102:36551').result()
    result, t = client.submit(run, composer, size, threads, a, b, c, workers='tcp://192.168.1.104:37385').result()
    end = time.time()
    exe_time = end - start
    print(result)
    print(f"exe_time: {exe_time}, ret t: {t}")
    
    # print(t)

    client.close()