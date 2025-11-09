#!/usr/bin/env bash
set -euo pipefail
# Simple experiment runner
# Usage: ./run_experiment.sh <framework> <workload> <dataset_path> <run_id>

FRAMEWORK=${1:?}
WORKLOAD=${2:?}
DATASET=${3:?}
RUNID=${4:?}

OUTDIR=experiments/logs
mkdir -p "$OUTDIR"

PREFIX="$OUTDIR/${FRAMEWORK}_${WORKLOAD}_run${RUNID}"

echo "=== Experiment: $FRAMEWORK $WORKLOAD run $RUNID ==="
# echo "dataset: $DATASET"

# Container mode configuration (optional)
USE_CONTAINERS=${USE_CONTAINERS:-0}
MONGO_CONTAINER=${MONGO_CONTAINER:-project3-mongodb}
HADOOP_CONTAINER=${HADOOP_CONTAINER:-project3-hadoop}
CONTAINER_WORKDIR=${CONTAINER_WORKDIR:-/tmp/experiment}

# start dstat for system metrics
if command -v dstat >/dev/null 2>&1; then
  dstat -cdnmpty --output ${PREFIX}_dstat.csv 1 > /dev/null 2>&1 &
  DSTAT_PID=$!
else
  DSTAT_PID=""
fi

# Function to stop dstat
function stop_dstat() {
  if [ -n "${DSTAT_PID}" ] && kill -0 "$DSTAT_PID" 2>/dev/null; then
    kill "$DSTAT_PID" || true
    wait "$DSTAT_PID" 2>/dev/null || true
  fi
}

TIMELOG=${PREFIX}.time.txt
STDOUTLOG=${PREFIX}.out.txt
ERRLOG=${PREFIX}.err.txt

echo "Start: $(date --iso-8601=seconds)" > ${PREFIX}.meta.txt

case "$FRAMEWORK" in
  spark)
    SCRIPT="experiments/commands/spark_${WORKLOAD}.sh"
    if [ ! -x "$SCRIPT" ]; then
      echo "Missing or non-executable $SCRIPT" >&2
      stop_dstat
      exit 2
    fi
    if [ "${USE_CONTAINERS}" = "1" ]; then
      SPARK_CONTAINER=${SPARK_CONTAINER:-project3-spark}
      echo "Running Spark workload inside container: ${SPARK_CONTAINER}" >> ${PREFIX}.meta.txt
      # ensure container workdir and data dir
      docker exec -it ${SPARK_CONTAINER} mkdir -p "${CONTAINER_WORKDIR}" "${CONTAINER_WORKDIR}/data" "${CONTAINER_WORKDIR}/events" || true
      # copy dataset and script into container
      # docker cp "$DATASET" ${SPARK_CONTAINER}:"${CONTAINER_WORKDIR}/data/data.csv"
      # docker cp "$SCRIPT" ${SPARK_CONTAINER}:"${CONTAINER_WORKDIR}/$(basename $SCRIPT)"
      # also copy the covid_spread repo (spark scripts) into the container so relative paths resolve
          if [ -d "covid_spread" ]; then
            # ensure destination parent exists before docker cp
            docker exec -it ${SPARK_CONTAINER} mkdir -p "${CONTAINER_WORKDIR}/covid_spread" 2>/dev/null || true
            # try docker cp; if it fails (some docker versions/paths), fall back to tar stream
            if ! docker cp covid_spread ${SPARK_CONTAINER}:"${CONTAINER_WORKDIR}/covid_spread" 2>/dev/null; then
              echo "docker cp of covid_spread failed, falling back to tar stream" >&2
              tar -C covid_spread -cf - . | docker exec -it ${SPARK_CONTAINER} tar -C "${CONTAINER_WORKDIR}/covid_spread" -xf -
            fi
          fi
      docker exec -it ${SPARK_CONTAINER} chmod +x "${CONTAINER_WORKDIR}/$(basename $SCRIPT)" || true
      # Check if /usr/bin/time exists inside the container
      TIME_IN_CONTAINER=$(docker exec -it ${SPARK_CONTAINER} sh -lc 'if [ -x /usr/bin/time ]; then echo yes; fi' || true)
      if [ "${TIME_IN_CONTAINER}" = "yes" ]; then
        docker exec -it ${SPARK_CONTAINER} /usr/bin/time -v bash -lc "${CONTAINER_WORKDIR}/$(basename $SCRIPT) '${CONTAINER_WORKDIR}/data/data.csv'" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
      else
        echo "Warning: /usr/bin/time not found inside spark container; falling back to host /usr/bin/time (RSS may be inaccurate)" >> ${PREFIX}.meta.txt
        /usr/bin/time -v docker exec -it ${SPARK_CONTAINER} bash -lc "${CONTAINER_WORKDIR}/$(basename $SCRIPT) '${CONTAINER_WORKDIR}/data/data.csv'" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
      fi
    else
      # copy dataset to expected location so existing bin scripts read it
      mkdir -p data
      DEST="data/data.csv"
      # if destination exists, only copy when it's a different file (avoid "are the same file" cp error)
      if [ -e "$DEST" ]; then
        # use realpath to compare canonical paths
        if command -v realpath >/dev/null 2>&1; then
          SRC_REAL=$(realpath "$DATASET")
          DST_REAL=$(realpath "$DEST")
          if [ "$SRC_REAL" != "$DST_REAL" ]; then
            cp "$DATASET" "$DEST"
          else
            echo "Dataset source and destination are the same; skipping copy"
          fi
        else
          # fallback: attempt copy and ignore the specific error
          cp "$DATASET" "$DEST" || true
        fi
      else
        cp "$DATASET" "$DEST"
      fi
      /usr/bin/time -v bash -c "$SCRIPT" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
    fi
    ;;

  mongo)
    SCRIPT_JS="experiments/commands/mongo_${WORKLOAD}.js"
    if [ ! -f "$SCRIPT_JS" ]; then
      echo "Missing $SCRIPT_JS" >&2
      stop_dstat
      exit 2
    fi
    if [ "${USE_CONTAINERS}" = "1" ]; then
      echo "Importing and running inside container: ${MONGO_CONTAINER}" >> ${PREFIX}.meta.txt
      # ensure container workdir exists
      docker exec -it ${MONGO_CONTAINER} mkdir -p "${CONTAINER_WORKDIR}"
      # copy dataset and script into container
    #   docker cp "$DATASET" ${MONGO_CONTAINER}:"${CONTAINER_WORKDIR}/data.csv"
      # docker cp "$SCRIPT_JS" ${MONGO_CONTAINER}:"${CONTAINER_WORKDIR}/$(basename $SCRIPT_JS)"
      CONTAINER_JS_PATH="${CONTAINER_WORKDIR}/$(basename $SCRIPT_JS)"
      # determine which shell exists inside container (use sh -lc to ensure a shell builtin is available)
      MONGOSH_PATH=$(docker exec -i ${MONGO_CONTAINER} sh -lc 'command -v mongosh 2>/dev/null || true' || true)
      MONGO_PATH=$(docker exec -i ${MONGO_CONTAINER} sh -lc 'command -v mongo 2>/dev/null || true' || true)
      if [ -n "${MONGOSH_PATH}" ]; then
        echo "Using mongosh inside container: ${MONGOSH_PATH}" >> ${PREFIX}.meta.txt
        INNER_CMD="${MONGOSH_PATH} --quiet --file '${CONTAINER_JS_PATH}'"
      elif [ -n "${MONGO_PATH}" ]; then
        echo "Using mongo inside container: ${MONGO_PATH}" >> ${PREFIX}.meta.txt
        INNER_CMD="${MONGO_PATH} --quiet covid '${CONTAINER_JS_PATH}'"
      else
        echo "Container ${MONGO_CONTAINER} has neither mongosh nor mongo installed" >&2
        stop_dstat
        exit 3
      fi
      # Check if /usr/bin/time exists inside the container. If not, fall back to running time on host
      TIME_IN_CONTAINER=$(docker exec -it ${MONGO_CONTAINER} sh -lc 'if [ -x /usr/bin/time ]; then echo yes; fi' || true)
      if [ "${TIME_IN_CONTAINER}" = "yes" ]; then
        # run time inside the container (preferred — measures in-container process RSS)
        docker exec -it ${MONGO_CONTAINER} bash -lc "/usr/bin/time -v ${INNER_CMD}" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
      else
        # fallback: run /usr/bin/time on the host around docker exec; note this measures the docker exec process on the host
        echo "Warning: /usr/bin/time not found inside container; falling back to host /usr/bin/time (RSS may be inaccurate)" >> ${PREFIX}.meta.txt
        /usr/bin/time -v docker exec -it ${MONGO_CONTAINER} bash -lc "${INNER_CMD}" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
      fi
    else
      # local import then run
      echo "Importing dataset into MongoDB (collection: records)..."
      mongoimport --db covid --collection records --type csv --headerline --drop --file "$DATASET" >/dev/null 2>&1 || true
      # prefer mongosh, fall back to legacy mongo if available
      if command -v mongosh >/dev/null 2>&1; then
        SHELL_CMD=(mongosh --quiet --file "$SCRIPT_JS")
      elif command -v mongo >/dev/null 2>&1; then
        SHELL_CMD=(mongo --quiet covid "$SCRIPT_JS")
      else
        echo "Neither 'mongosh' nor 'mongo' found in PATH. Install mongosh (recommended) or the mongo shell." >&2
        stop_dstat
        exit 3
      fi
      /usr/bin/time -v bash -c "${SHELL_CMD[*]}" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
    fi
    ;;

  hadoop)
    # Hadoop command script should handle pushing the dataset to HDFS and running the job
    SCRIPT="experiments/commands/hadoop_${WORKLOAD}.sh"
    if [ ! -x "$SCRIPT" ]; then
      echo "Missing or non-executable $SCRIPT" >&2
      stop_dstat
      exit 2
    fi
    if [ "${USE_CONTAINERS}" = "1" ]; then
      echo "Running Hadoop workload inside container: ${HADOOP_CONTAINER}" >> ${PREFIX}.meta.txt
      docker exec -it ${HADOOP_CONTAINER} mkdir -p "${CONTAINER_WORKDIR}"
      # docker cp "$DATASET" ${HADOOP_CONTAINER}:"${CONTAINER_WORKDIR}/data.csv"
      # docker cp "$SCRIPT" ${HADOOP_CONTAINER}:"${CONTAINER_WORKDIR}/$(basename $SCRIPT)"
      # copy hadoop helper scripts (if present) into the container workdir
      if [ -d "experiments/hadoop_scripts" ]; then
        # docker cp experiments/hadoop_scripts ${HADOOP_CONTAINER}:"${CONTAINER_WORKDIR}/hadoop_scripts"
        # make helper scripts executable inside container (if shell supports it)
        docker exec -it ${HADOOP_CONTAINER} bash -lc "chmod -R a+rx '${CONTAINER_WORKDIR}/hadoop_scripts'" || true
      fi
      docker exec -it ${HADOOP_CONTAINER} chmod +x "${CONTAINER_WORKDIR}/$(basename $SCRIPT)" || true
      # Check if /usr/bin/time exists inside the container
      TIME_IN_CONTAINER=$(docker exec -it ${HADOOP_CONTAINER} sh -lc 'if [ -x /usr/bin/time ]; then echo yes; fi' || true)
      if [ "${TIME_IN_CONTAINER}" = "yes" ]; then
        docker exec -it ${HADOOP_CONTAINER} /usr/bin/time -v bash -lc "${CONTAINER_WORKDIR}/$(basename $SCRIPT) '${CONTAINER_WORKDIR}/data.csv'" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
      else
        echo "Warning: /usr/bin/time not found inside hadoop container; falling back to host /usr/bin/time (RSS may be inaccurate)" >> ${PREFIX}.meta.txt
        /usr/bin/time -v docker exec -it ${HADOOP_CONTAINER} bash -lc "${CONTAINER_WORKDIR}/$(basename $SCRIPT) '${CONTAINER_WORKDIR}/data.csv'" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
      fi
    else
      /usr/bin/time -v bash -c "$SCRIPT '$DATASET'" 1> ${STDOUTLOG} 2> ${TIMELOG} || true
    fi
    ;;

  *)
    echo "Unknown framework: $FRAMEWORK" >&2
    stop_dstat
    exit 2
    ;;
esac

echo "End: $(date --iso-8601=seconds)" >> ${PREFIX}.meta.txt
stop_dstat

echo "Logs written to: ${PREFIX}.*"
