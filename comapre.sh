#!/bin/bash

export USE_CONTAINERS=1
export SPARK_CONTAINER=project3-spark
export HADOOP_CONTAINER=project3-hadoop
export MONGO_CONTAINER=project3-mongodb

REPEATS=3
VARIANTS=(experiments/data_small.csv data/data.csv experiments/data_large.csv)
FRAMEWORKS=(hadoop mongo spark)
WORKLOADS=(A B C)

for variant in "${VARIANTS[@]}"; do
  for wf in "${WORKLOADS[@]}"; do
    for fw in "${FRAMEWORKS[@]}"; do
      for r in $(seq 1 $REPEATS); do
        # compute basename without extension for compact run id
        _varbase=$(basename "$variant")
        _varbase="${_varbase%.*}"
        ./experiments/run_experiment.sh "$fw" "$wf" "$variant" "${r}_$_varbase"
      done
    done
  done
done
