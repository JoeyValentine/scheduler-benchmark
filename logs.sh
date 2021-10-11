#!/usr/bin/env bash

LOG_DIR_NAME=results
namespaces=("sysbench" "fio")

mkdir $LOG_DIR_NAME

for ns in ${namespaces[*]}
do
  mkdir -p $LOG_DIR_NAME/$ns/logs
  mkdir -p $LOG_DIR_NAME/$ns/get

  if [ "$ns" == "sysbench" ]; then
    for podname in $(kubectl get pods -n $ns --no-headers=true | cut -d" " -f1)
    do
      kubectl logs -n $ns $podname > $LOG_DIR_NAME/$ns/logs/$podname.txt
      kubectl get pods -n $ns $podname -o json > $LOG_DIR_NAME/$ns/get/$podname.json
    done
  elif [ "$ns" == "fio" ]; then
    for podname in $(kubectl get pods -n $ns --no-headers=true | cut -d" " -f1)
    do
      kubectl logs -n $ns $podname > $LOG_DIR_NAME/$ns/logs/$podname.json
      kubectl get pods -n $ns $podname -o json > $LOG_DIR_NAME/$ns/get/$podname.json
    done
  fi

done
