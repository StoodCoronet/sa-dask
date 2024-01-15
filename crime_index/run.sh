#!/bin/bash

set -x

#source ../benchmarks/bin/activate

size=26
tasks=( naive composer )
threads=( 1 2 4 8 16 )
# threads=( 1 )
runs=${1:-1}

for task in "${tasks[@]}"; do 
  rm -f $task.stdout $task.stderr
  > $task.stderr
  > $task.stdout
done

for i in {1..$runs}; do
  python crime_index_dask.py -m naive -s $size >> naive.stdout 2>> naive.stderr
  for nthreads in "${threads[@]}"; do 
    python crime_index_dask.py -m composer -t $nthreads -s $size >> $task.stdout 2>> $task.stderr
  done
done