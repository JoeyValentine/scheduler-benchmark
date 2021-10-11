#!/usr/bin/env bash

N_POD=50
nodes=("slave1" "slave3" "slave4")

for ((i=1;i<=N_POD;++i))
do
  if [ -f pods/pod$i/fio-pvc.yaml ]
  then
    kubectl delete -f pods/pod$i/fio-pvc.yaml &
    kubectl delete -f pods/pod$i/fio-pod.yaml &
  else
    kubectl delete -f pods/pod$i/sysbench-pod.yaml &
  fi
done

for n in ${nodes[*]}
do
  for ((i=1;i<=N_POD;++i))
  do
    kubectl delete -f pv/$n/$i-local-pv.yaml
  done
done
