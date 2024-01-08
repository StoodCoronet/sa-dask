import sa.annotated.numpy as np
import time
from dask.distributed import LocalCluster, Client, SSHCluster

def run():
    size = 16
    threads = 2
    threads_list = [1, 2, 4, 8]
    size = (1 << size)
    a = np.random.random(size)
    b = np.random.random(size)
    c = np.random.random(size)
    
    for threads in threads_list:
        start = time.time()
        np.add(a, b, out=b)
        np.multiply(a, b, out=c)
        np.add(a, b, out=b)
        np.multiply(a, b, out=c)
        np.add(a, b, out=b)
        np.multiply(a, b, out=c)
        np.add(a, b, out=b)
        np.multiply(a, b, out=c)

        np.evaluate(workers=threads)
        end = time.time()
        exe_time = end - start
        
        print(f"threads: {threads}, time: {exe_time}, c: {c}")
        # return c
    
def run2(size, threads):
    size = (1 << size)
    a = np.random.random(size)
    b = np.random.random(size)
    c = np.random.random(size)
    
    start = time.time()
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)
    np.add(a, b, out=b)
    np.multiply(a, b, out=c)

    np.evaluate(workers=threads)
    end = time.time()
    exe_time = end - start
    
    # print(f"threads: {threads}, time: {exe_time}, c: {c}")
    return c

if __name__ == "__main__":
    cluster = LocalCluster(n_workers=4, threads_per_worker=2)
    client = Client(cluster)

    # run()
    size = 16
    threads_list = [1, 2, 4, 8]
    for threads in threads_list:
        start = time.time()
        # out = client.submit(run2, size, threads)
        # out = client.submit(run)
        # result = out.result()
        end = time.time()
        exe_time = end - start
        # print(f"thread: {threads}, time: {exe_time}, result: {result}")
    out = client.submit(run)
        
    
    client.close()
    cluster.close()