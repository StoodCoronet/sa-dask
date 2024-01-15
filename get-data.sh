#!/bin/bash

set -x

cd datasets
TOP=`pwd`

# Birth Analysis
cd $TOP
cd birth_analysis
gunzip -k babynames.txt.gz
rm -rf _data
mkdir -p _data
mv babynames.txt _data
./replicate-csv -i _data/babynames.txt -o  _data/babynames-xlarge.txt -r 80
