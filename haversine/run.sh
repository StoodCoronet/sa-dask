#!/bin/bash

set -x

size=28
tasks=( naive composer )
threads=( 1 2 4 8 16 )

for task in "${tasks[@]}"; do 
    rm -f $task.stdout $task.stderr
    > $task.stderr
    > $task.stdout
done

for nthreads in "${threads[@]}"; do 
    python dask_haversine.py -m composer -t $nthreads -s $size >> composer.stdout 2>> composer.stderr
done

python dask_haversine.py -m naive -s $size >> naive.stdout 2>> naive.stderr