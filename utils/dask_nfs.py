import time
# import pandas as pd
import sys

start = time.time()

# df = dd.read_csv('/mnt/nfs/lat22.csv', blocksize=25e6)
# subset = df.loc[100:200]
# print(subset.compute())

sys.path.append("/home/robye/workspace/sa/split-annotations/python/lib")
sys.path.append("/home/robye/workspace/sa/split-annotations/python/pycomposer")
sys.path.append("/home/sakaban/split-annotations/python/lib")
sys.path.append("/home/sakaban/split-annotations/python/pycomposer")
# import composer_pandas as pd
import pandas as pd
import dask.dataframe as dd

size = 30
size = (1 << size)
start_row = size - 100
end_row = size
start_row = 100000
end_row = 100000 + 100
# df = pd.read_csv('/mnt/nfs/lat2.csv', skiprows=range(1, start_row), nrows=end_row-start_row)
# df = pd.read_csv('/mnt/nfs/lat22.csv')

# df = dd.read_csv('/mnt/nfs/lat2.csv').iloc[:,1,].loc[1000:2000]
# df = dd.read_csv('/mnt/nfs/lat1_1.csv')#.rename(columns={'Unnamed: 0': 'num', 'val': 'val'})
df = dd.read_csv('/home/sakaban/workspace/dask-playground/data/lat_1.csv/*.part')

# df = df.iloc[:, 1:2]
# print(len(df))
# df.groupby('num')
# df = df.set_index('num')#.loc[100000000: 100000000 + 1000]
# dask_array = df.to_dask_array(lengths=True)
result = df.compute()
print(result)
print(len(result))
# result.to_csv('/home/sakaban/workspace/shared_dir/lat1_1.csv', index=False)


# import numpy as np
# lat = df.values[:,1:]
# pd.evaluate(workers=6)
# print(lat[0])

# import pandas as pd
# print(f"time: {time.time() - start}")

# start = time.time()
# filename = '/mnt/nfs/lat2.csv'
# with open(filename, 'r') as file:
#     total_rows = sum(1 for _ in file)

# df = pd.read_csv(filename, nrows=1)
# total_rows = df
    
# print(total_rows)
# print(f"time: {time.time() - start}")