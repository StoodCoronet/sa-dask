#!/bin/bash

set -x

#source ../benchmarks/bin/activate

# File to use. babynames.txt is for testing. babynames-xlarge.txt is for benchmark.
filename="/mnt/nfs/birth_analysis/_data/babynames-xlarge.txt"
runs=${1:-1}

tasks=( composer naive )
threads=( 1 2 4 8 16 )
# threads=( 16 )

for task in "${tasks[@]}"; do 
  rm -f $task.stdout $task.stderr
  > $task.stderr
  > $task.stdout
done

for i in {1..$runs}; do
  # python birth_analysis_dask_naive.py -f $filename -m naive >> naive.stdout 2>> naive.stderr
  for nthreads in "${threads[@]}"; do 
    python birth_analysis_dask.py -f $filename -t $nthreads -m composer >> composer.stdout 2>> composer.stderr
  done
done
