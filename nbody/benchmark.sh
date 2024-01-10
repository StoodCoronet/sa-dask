#!/bin/bash

set -x

#source ../benchmarks/bin/activate

runs=${1:-1}
size=13
iterations=5
# tasks=( numba naive composer bohrium )
tasks=( naive composer )
threads=( 1 2 4 8 16 )
# threads=( 4 8 )

for task in "${tasks[@]}"; do 
  rm -f $task.stdout $task.stderr
  > $task.stderr
  > $task.stdout
done

# for i in {1..$runs}; do
#   for nthreads in "${threads[@]}"; do 
#     NUMBA_NUM_THREADS=$nthreads python nbody_numba.py -s $size -i $iterations >> numba.stdout 2>> numba.stderr
#   done
# done

for i in {1..$runs}; do
  for nthreads in "${threads[@]}"; do 
    python nbody.py -m composer -s $size -i $iterations -t $nthreads >> composer.stdout 2>> composer.stderr
    python ../haversine/dask_connect_haversine.py -m composer -t 4 -s 26
  done
done

for i in {1..$runs}; do
  python nbody.py -m naive -s $size -i $iterations -t $nthreads >> naive.stdout 2>> naive.stderr
done

# for i in {1..$runs}; do
#   for nthreads in "${threads[@]}"; do 
#     OMP_NUM_THREADS=$nthreads python nbody_boh.py -s $size -i $iterations -t $nthreads >> bohrium.stdout 2>> bohrium.stderr
#   done
# done


# python dask_connect_haversine.py -m composer -t 4 -s 26
# python nbody.py -m composer -s 14 -i 5 -t 1 >> composer.stdout 2>> composer.stderr
# python nbody.py -m composer -s 14 -i 5 -t 2 >> composer.stdout 2>> composer.stderr
# python nbody.py -m composer -s 14 -i 5 -t 4 >> composer.stdout 2>> composer.stderr
# python nbody.py -m composer -s 14 -i 5 -t 8 >> composer.stdout 2>> composer.stderr
# python nbody.py -m composer -s 14 -i 5 -t 16 >> composer.stdout 2>> composer.stderr
# python nbody.py -m naive -s 14 -i 5 >> naive.stdout 2>> naive.stderr
# TODO naive version of nbody

