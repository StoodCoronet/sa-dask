import sys

from dask.distributed import Client, get_worker
import argparse
import composer_pandas as pd
import time

def analyze(top1000):
    import composer_numpy as np
    import composer_pandas as pd
    start1 = time.time()
    all_names = pd.Series(top1000.name.unique())
    lesley_like = all_names[all_names.str.lower().str.contains('lesl')]
    filtered = top1000[top1000.name.isin(lesley_like)]
    table = filtered.pivot_table('births', index='year',
                                 columns='sex', aggfunc='sum')

    table = table.div(table.sum(1), axis=0)
    end1 = time.time()
    print("Analysis:", end1 - start1)
    analyze_time = end1-start1
    return table, analyze_time

def get_top1000(group):
    return group.sort_values(by='births', ascending=False)[0:1000]

def run(filename, threads, composer):
    import composer_numpy as np
    import composer_pandas as pd
        
    years = range(1880, 2011)
    columns = ['year', 'sex', 'name', 'births']
    
    # reading data
    names = pd.read_csv(filename, names=columns)
    
    print("Size of names:", len(names))
    size_of_names = len(names)
    
    e2e_start = time.time()
    
    start0 = time.time()
    grouped = pd.dfgroupby(names, ['year', 'sex'])
    top1000 = pd.gbapply(grouped, get_top1000)
    pd.evaluate(workers=threads)
    top1000 = top1000.value
    top1000.reset_index(inplace=True, drop=True)
    print(len(top1000))
    top1000_len = len(top1000)
    
    """
    grouped: Dag Operation
    GBApply Takes a DAG operation and stores it in its type. The operation must be a GroupBy
    GBApply has type ApplySplit. It's combiner:
        1. Combines the results of the dataFrame
        2. Resets index
        3. Gets the keys from the DAG operation
        4. Calls groupBy again
        5. Calls apply again.
    """
    localreduce_start = time.time()
    top1000 = top1000.groupby(['year', 'sex']).apply(get_top1000)
    localreduce_end = time.time()
    print("Local reduction:", localreduce_end - localreduce_start)
    local_reduction = localreduce_end - localreduce_start
    top1000.reset_index(inplace=True, drop=True)
    end0 = time.time()
    
    print("Apply:", end0-start0)
    apply_time = end0 - start0
    print("Elements in top1000:", len(top1000))
    elememts_num_in_top1000 = len(top1000)

    result, analyze_time = analyze(top1000)

    e2e_end = time.time()
    print("Total time:", e2e_end - e2e_start)
    total_time = e2e_end - e2e_start

    print(top1000['births'].sum())
    top1000_birth_sum = top1000['births'].sum()
    
    return (size_of_names, 
            top1000_len, 
            local_reduction, 
            apply_time, 
            elememts_num_in_top1000, 
            analyze_time, 
            total_time, top1000_birth_sum)

def main():
    parser = argparse.ArgumentParser(
        description="Birth Analysis with Composer."
    )
    parser.add_argument('-f', "--filename", type=str, default="babynames.txt", help="Input file")
    parser.add_argument('-t', "--threads", type=int, default=1, help="Number of threads.")
    parser.add_argument('-m', "--mode", type=str, required=True, help="Mode (composer|naive)")
    args = parser.parse_args()

    filename = args.filename
    threads = args.threads
    mode = args.mode.strip().lower()

    print("File:", filename)
    print("Threads:", threads)
    if mode == "composer":
        composer = True
    elif mode == "naive":
        composer = False
    else:
        raise ValueError("unknown mode", mode)
    
    if composer:
        import composer_numpy as np
        import composer_pandas as pd
    else:
        import numpy as np
        import pandas as pd
    
    client = Client('tcp://<scheduler-address>')
    print(client)

    start = time.time()
    future = client.submit(run, filename, threads, composer, workers=['w2'])
    size_of_names, top1000_len, local_reduction, apply_time, elememts_num_in_top1000, analyze_time, total_time, top1000_birth_sum = future.result()
    
    end = time.time()
    exe_time = end - start
    
    print("========================================================")
    print(f"Size of names: {size_of_names}")
    print(f"Top1000 length: {top1000_len}")
    print(f"Local Reduction: {local_reduction}")
    print(f"Apply: {apply_time}")
    print(f"Elemts in top1000: {elememts_num_in_top1000}")
    print(f"Analysis: {analyze_time}")
    print(f"SA total time: {total_time}")
    print(f"Top1000 birth sum: {top1000_birth_sum}")
    print(f"Dask Time: {exe_time}")
    print("========================================================")


if __name__ == "__main__":
    main()