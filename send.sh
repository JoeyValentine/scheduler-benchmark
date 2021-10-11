#!/usr/bin/env bash

scp -r pods pv intervals.txt logs.sh rmdir.sh start.sh stop.sh master:~/scheduler-bench

nodes=("slave1" "slave3" "slave4")

for n in ${nodes[*]}
do
  scp fio-jobs/* $n:~/fio-benchmark/jobs
done