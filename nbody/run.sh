#!/bin/bash

set -x


runs=${1:-1}
size=13
iterations=5
tasks=( naive composer )
threads=( 1 2 4 8 16 )

for task in "${tasks[@]}"; do 
  rm -f $task.stdout $task.stderr
  > $task.stderr
  > $task.stdout
done

for i in {1..$runs}; do
  for nthreads in "${threads[@]}"; do 
    python nbody.py -m composer -s $size -i $iterations -t $nthreads >> composer.stdout 2>> composer.stderr
  done
done

for i in {1..$runs}; do
  python nbody.py -m naive -s $size -i $iterations -t $nthreads >> naive.stdout 2>> naive.stderr
done