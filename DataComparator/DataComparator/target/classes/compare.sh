#!/usr/bin/env bash
export time=`date +"+%Y%m%d%H%M%S"`
spark-submit --class com.company.comparator.Main --master yarn --deploy-mode client --num-executors 2 --executor-cores 4 --executor-memory 1G DataComparator-1.0-SNAPSHOT.jar > ./output_$time.log