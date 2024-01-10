#!/bin/bash

set -x

size=26
tasks=( naive composer )
threads=( 1 2 4 8 16 )
# threads=( 4 )

for task in "${tasks[@]}"; do 
    rm -f $task.stdout $task.stderr
    > $task.stderr
    > $task.stdout
done

for nthreads in "${threads[@]}"; do 
    python dask_connect_haversine_v2.py -m composer -t $nthreads -s $size >> composer.stdout 2>> composer.stderr
done

python dask_connect_haversine_v2.py -m naive -s $size >> naive.stdout 2>> naive.stderr

# python dask_connect_haversine.py -m composer -s 20
# python dask_connect_haversine.py -m composer -t 4 -s 26