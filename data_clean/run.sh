#!/bin/bash

set -x

size=27
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
  for nthreads in "${threads[@]}"; do 
    python data_cleaning.py -m composer -t $nthreads -s $size >> composer.stdout 2>> composer.stderr
  done
done

python data_cleaning.py -m naive -t $nthreads -s $size >> naive.stdout 2>> naive.stderr
# done
