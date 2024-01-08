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

df = dd.read_csv('/mnt/nfs/haversine/lon2/*.part')

# subset = df.loc[1343]
# print(subset.compute())

size = 26
size = (1 << size)

# start = size * ((1 << 4) - 1)
# end = size * (1 << 4)

start = size
end = size * 2

# part = df.get_partation(1)
# print(type(df))
print(start)
print(end)
start_time = time.time()
# subset = df.head(end).tail(start)
# subset = df.skip(start).take(end-start).compute()
# print(subset)
# print(len(subset))
print(df.get_partition(df.npartitions-1).compute())
print(len(df.get_partition(df.npartitions-2).compute()))
print(f"time: {time.time() - start_time}")

# print(df.compute())

# print(len(df))
