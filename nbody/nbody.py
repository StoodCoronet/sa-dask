from dask.distributed import Client, get_worker
from dask_jobqueue import SLURMCluster, PBSCluster
import time
import argparse
import math
import sys

def fill_diagonal(a, val):
    """ Set diagonal of 2D matrix a to val in-place. """
    d, _ = a.shape
    a.shape = d * d
    a[::d + 1] = val
    a.shape = (d, d)

        
def random_galaxy(N, composer, threads):
    if composer:
        import composer_numpy as np
        addreduce = np.addreduce
    else:
        import numpy as np
        addreduce = np.add.reduce
        
    """ Generate a galaxy of random bodies """
    m =  (np.arange(0, 1, step=1.0 / N) + np.float64(10)) * np.float64(m_sol/10)
    x =  (np.arange(0, 1, step=1.0 / N) - np.float64(0.5)) * np.float64(r_ly/100)
    y =  (np.arange(0, 1, step=1.0 / N) - np.float64(0.5)) * np.float64(r_ly/100)
    z =  (np.arange(0, 1, step=1.0 / N) - np.float64(0.5)) * np.float64(r_ly/100)
    vx = np.zeros(N, dtype=np.float64)
    vy = np.zeros(N, dtype=np.float64)
    vz = np.zeros(N, dtype=np.float64)
    
    assert len(m) == N
    
    if composer:
        # wao such memory
        import sharedmem
        m_mem = sharedmem.empty(len(m))
        m_mem[:] = m[:]
        x_mem = sharedmem.empty(len(m))
        x_mem[:] = x[:]
        y_mem = sharedmem.empty(len(m))
        y_mem[:] = y[:]
        z_mem = sharedmem.empty(len(m))
        z_mem[:] = z[:]
        return m_mem, x_mem, y_mem, z_mem, vx, vy, vz
    else:
        return m, x, y, z, vx, vy, vz
    # =========================
def calc_force(m, x, y, z, vx, vy, vz, dt, temporaries):
    """Calculate forces between bodies

    F = ((G m_a m_b)/r^2)/((x_b-x_a)/r)

    """

    start = time.time()

    tmp, dx, dy, dz, pm, r, Fx, Fy, Fz = temporaries

    dxl = np.subtract(x, x[:,None], out=dx)
    dyl = np.subtract(y, y[:,None], out=dy)
    dzl = np.subtract(z, z[:,None], out=dz)
    pml = np.multiply(m, m[:,None], out=pm)

    if composer:
        dxl.annotation.kwarg_types['out'].slice_col = True
        dyl.annotation.kwarg_types['out'].slice_col = True
        dzl.annotation.kwarg_types['out'].slice_col = True
        pml.annotation.kwarg_types['out'].slice_col = True
        np.evaluate(workers=threads, batch_size=2048)

    end = time.time()
    print("Step 0:", end - start)
    step_0 = end - start
    start = end

    tmp = np.reshape(tmp, dx.shape)
    end = time.time()
    print("Step 0.5:", end - start)
    step_05 = end - start
    start = end

    # r = np.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
    np.multiply(dx, dx, out=r)
    np.multiply(dy, dy, out=tmp)
    np.add(r, tmp, out=r)
    np.multiply(dz, dz, out=tmp)
    np.add(r, tmp, out=r)
    np.sqrt(r, out=r)

    # Fx = G * pm / r ** 2 * (dx / r)
    # Fy = G * pm / r ** 2 * (dy / r)
    # Fz = G * pm / r ** 2 * (dz / r)
    np.multiply(r, r, out=tmp)
    np.divide(pm, tmp, out=tmp)
    np.multiply(G, tmp, out=tmp)

    np.divide(dx, r, out=Fx)
    np.multiply(tmp, Fx, out=Fx)

    np.divide(dy, r, out=Fy)
    np.multiply(tmp, Fy, out=Fy)

    np.divide(dz, r, out=Fz)
    np.multiply(tmp, Fz, out=Fz)

    if composer:
        np.evaluate(workers=threads, batch_size=1)
    end = time.time()
    print("Step 1:", end - start)
    step_1 = end - start
    start = end

    fill_diagonal(Fx, 0.0)
    fill_diagonal(Fy, 0.0)
    fill_diagonal(Fz, 0.0)

    end = time.time()
    print("Step 2:", end - start)
    step_2 = end - start
    start = end

    # m * dt
    tmp = tmp.reshape((len(vx), len(vx)))
    tmp = tmp[:,0]

    np.divide(m, dt, out=tmp)

    Fx2 = Fx[:,0]
    addreduce(Fx, axis=1, out=Fx2)

    Fy2 = Fy[:,0]
    addreduce(Fy, axis=1, out=Fy2)

    Fz2 = Fz[:,0]
    addreduce(Fz, axis=1, out=Fz2)

    np.divide(Fx2, tmp, out=Fx2)
    np.add(vx, Fx2, out=vx)

    np.divide(Fy2, tmp, out=Fy2)
    np.add(vy, Fy2, out=vy)

    np.divide(Fz2, tmp, out=Fz2)
    np.add(vz, Fz2, out=vz)

    if composer:
        np.evaluate(workers=threads)

    end = time.time()
    print("Step 3:", end - start)
    step_3 = end - start
    start = end
    return step_0, step_05, step_1, step_2, step_3

def move(m, x, y, z, vx, vy, vz, dt, temporaries):
    """ Move the bodies.

    first find forces and change velocity and then move positions.
    """

    step_0, step_05, step_1, step_2, step_3 = calc_force(m, x, y, z, vx, vy, vz, dt, temporaries)

    start = time.time()

    tmp = temporaries[0]
    tmp_slice = tmp[:len(x)]

    # Update the positions based on the computed velocities.
    np.multiply(vx, dt, out=tmp_slice)
    np.add(x, tmp_slice, out=x)

    np.multiply(vy, dt, out=tmp_slice)
    np.add(y, tmp_slice, out=y)

    np.multiply(vz, dt, out=tmp_slice)
    np.add(z, tmp_slice, out=z)

    # Force evaluation here because the real workload would draw/write out
    # these results.
    if composer:
        np.evaluate(workers=threads)

    end = time.time()
    print("Step 4:", end - start)
    step_4 = end - start
    start = end
    return step_0, step_05, step_1, step_2, step_3, step_4

def simulate(m, x, y, z, vx, vy, vz, timesteps):
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
    sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer")
    sys.path.append("/home/sakaban/split-annotations/python/lib")
    sys.path.append("/home/sakaban/split-annotations/python/pycomposer")
    if composer:
        import composer_numpy as np
        addreduce = np.addreduce
    else:
        import numpy as np
        addreduce = np.add.reduce
        
    size = len(m)
    temporaries = (
        np.ones(size * size, dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64"),
        np.ones((size, size), dtype="float64")
    )
    
    start = time.time()
    for i in range(timesteps):
        step_0, step_05, step_1, step_2, step_3, step_4 = move(m, x, y, z, vx, vy, vz, dt, temporaries)

    end = time.time()
    print("Simulation time:", end - start)
    simulation_time = end - start
    return (simulation_time, step_0, step_05, step_1, step_2, step_3, step_4)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="N-Body benchmark."
    )
    parser.add_argument('-s', "--size", type=int, default=10, help="Size of each array")
    parser.add_argument('-i', "--iterations", type=int, default=1, help="Iterations of simulation")
    parser.add_argument('-b', "--batch_size", type=int, default=16384, help="Size of each piece.")
    parser.add_argument('-t', "--threads", type=int, default=1, help="Number of threads.")
    parser.add_argument('-v', "--verbosity", type=str, default="none",\
            help="Log level (debug|info|warning|error|critical|none)")
    parser.add_argument('-m', "--mode", type=str, required=False, help="Mode (composer|naive)")
    args = parser.parse_args()

    size = (1 << args.size)
    iterations = args.iterations 
    batch_size = args.batch_size
    threads = args.threads
    loglevel = args.verbosity
    mode = args.mode.strip().lower()

    assert threads >= 1

    print("Size:", size)
    print("Batch Size:", batch_size)
    print("Threads:", threads)
    print("Log Level", loglevel)
    print("Mode:", mode)

    if mode == "composer":
        composer = True
    elif mode == "naive":
        composer = False
    else:
        raise ValueError("invalid mode", mode)
    
    client = Client(address='tcp://<scheduler-address>')
    print(client)
    client.restart()

    if composer:
        import composer_numpy as np
        addreduce = np.addreduce
    else:
        import numpy as np
        addreduce = np.add.reduce    

    
    np.seterr(divide='ignore', invalid='ignore')
    
    start = time.time()

    # Constants
    G     = np.float64(6.67384e-11)     # m/(kg*s^2)
    dt    = np.float64(60*60*24*365.25) # Years in seconds
    r_ly  = np.float64(9.4607e15)       # Lightyear in m
    m_sol = np.float64(1.9891e30)       # Solar mass in kg

    sys.stdout.write("Generating data...")
    sys.stdout.flush()
    
    dask_size = (1 << 4)
    size_list = [size] * dask_size
    composer_list = [composer] * dask_size
    threads_list = [threads] * dask_size
    
    results = []
    for i in range(0, dask_size, 2):
        future = client.map(random_galaxy, size_list[i:i+1], composer_list[i:i+1], threads_list[i:i+1])
        results += client.gather(future)
    
    m_list = []
    x_list = []
    y_list = []
    z_list = []
    vx_list = []
    vy_list = []
    vz_list = []
        
    for result in results:
        m, x, y, z, vx, vy, vz = result
        m_list.append(m)
        x_list.append(x)
        y_list.append(y)
        z_list.append(z)
        vx_list.append(vx)
        vy_list.append(vy)
        vz_list.append(vz)
        
    
    
    # print(result)
    
    print("done")
    
    future = client.submit(simulate, m, x, y, z, vx, vy, vz, iterations)
    result = future.result()
    simulation_time, step_0, step_05, step_1, step_2, step_3, step_4 = result
    
    iterations_list = [iterations] * len(x)
    
    results = []
    for i in range(0, len(x), 2):
        future = client.map(simulate, m_list[i:i+1], x_list[i:i+1], y_list[i:i+1], z_list[i:i+1], vx_list[i:i+1], vy_list[i:i+1], vz_list[i:i+1], iterations_list[i:i+1])
        results += client.gather(future)
    
    end = time.time()
        
    exe_time = end - start
    
    simulation_time_list = []
    step_0_list = []
    step_05_list = []
    step_1_list = []
    step_2_list = []
    step_3_list = []
    step_4_list = []
    for result in results:
        simulation_time, step_0, step_05, step_1, step_2, step_3, step_4 = result
        simulation_time_list.append(simulation_time)
        step_0_list.append(step_0)
        step_05_list.append(step_05)
        step_1_list.append(step_1)
        step_2_list.append(step_2)
        step_3_list.append(step_3)
        step_4_list.append(step_4)
    
    import statistics
    print(f"Simulation time: {statistics.mean(simulation_time_list)}")
    print(f"step 0:          {statistics.mean(step_0_list)}")
    print(f"step 0.5:        {statistics.mean(step_05_list)}")
    print(f"step 1:          {statistics.mean(step_1_list)}")
    print(f"step 2:          {statistics.mean(step_2_list)}")
    print(f"step 3:          {statistics.mean(step_3_list)}")
    print(f"Total time:      {exe_time}")

    client.close()