#!/usr/bin/env bash

N_POD=50
nodes=("slave1" "slave3" "slave4")

date

for n in ${nodes[*]}
do
  for ((i=1;i<=N_POD;++i))
  do
    kubectl apply -f pv/$n/$i-local-pv.yaml &
  done
done

i=1

for line in `cat intervals.txt`
do
  sleep $line
  if [ -f pods/pod$i/fio-pvc.yaml ]
  then
    kubectl apply -f pods/pod$i/fio-pvc.yaml
    kubectl apply -f pods/pod$i/fio-pod.yaml
  else
    kubectl apply -f pods/pod$i/sysbench-pod.yaml
  fi
  ((++i))
done