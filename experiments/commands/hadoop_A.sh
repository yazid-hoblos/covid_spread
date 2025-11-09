#!/usr/bin/env bash
# Placeholder Hadoop script for Workload A (total cases per country)
# The script should copy dataset to HDFS and run the streaming job.
# Edit this script to match your Hadoop streaming mapper/reducer filenames and HDFS paths.

DATASET_PATH="$1"
HDFS_DIR="/covid/expA"
HADOOP_PREFIX="/usr/local/hadoop"

# helper scripts directory (prefer local workdir copy created by the runner)
if [ -n "${CONTAINER_WORKDIR:-}" ] && [ -d "${CONTAINER_WORKDIR}/hadoop_scripts" ]; then
  HS_DIR="${CONTAINER_WORKDIR}/hadoop_scripts"
elif [ -d "./hadoop_scripts" ]; then
  HS_DIR="./hadoop_scripts"
elif [ -d "experiments/hadoop_scripts" ]; then
  HS_DIR="experiments/hadoop_scripts"
elif [ -d "/tmp/experiment/hadoop_scripts" ]; then
  HS_DIR="/tmp/experiment/hadoop_scripts"
else
  HS_DIR="experiments/hadoop_scripts"
fi

echo "Putting dataset into HDFS at $HDFS_DIR"
/usr/local/hadoop/bin/hdfs dfs -mkdir -p "$HDFS_DIR"
/usr/local/hadoop/bin/hdfs dfs -put -f "$DATASET_PATH" "$HDFS_DIR/covid_data.csv"

# Try to locate a Hadoop streaming jar in common absolute locations
STREAMING_JAR=""
for PREFIX in "${HADOOP_PREFIX}" "/usr/local/hadoop" "/opt/hadoop" "${HADOOP_HOME:-}"; do
  [ -z "$PREFIX" ] && continue
  cand=$(ls "$PREFIX/share/hadoop/tools/lib/hadoop-streaming"*.jar 2>/dev/null | head -n1 || true)
  if [ -n "$cand" ]; then
    STREAMING_JAR="$cand"
    break
  fi
done
if [ -z "$STREAMING_JAR" ]; then
  STREAMING_JAR=$(ls /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar 2>/dev/null | head -n1 || true)
fi

if [ -n "$STREAMING_JAR" ]; then
  echo "Found Hadoop streaming jar: $STREAMING_JAR"
  # ensure output path does not already exist (avoid job failure)
  if [ -x "${HADOOP_PREFIX}/bin/hadoop" ]; then
    echo "Removing existing HDFS output path if present: $HDFS_DIR/output"
    "${HADOOP_PREFIX}/bin/hadoop" fs -rm -r -skipTrash "$HDFS_DIR/output" 2>/dev/null || true
  elif command -v hdfs >/dev/null 2>&1; then
    echo "Removing existing HDFS output path if present via hdfs: $HDFS_DIR/output"
    hdfs dfs -rm -r -f "$HDFS_DIR/output" 2>/dev/null || true
  fi
  # prefer hadoop wrapper if available
  HADOOP_CMD="${HADOOP_PREFIX}/bin/hadoop"
  if [ -x "$HADOOP_CMD" ]; then
    "$HADOOP_CMD" jar "$STREAMING_JAR" \
      -files ${HS_DIR}/mapper_A.py,${HS_DIR}/reducer_A.py \
      -input "$HDFS_DIR/covid_data.csv" \
      -output "$HDFS_DIR/output" \
      -mapper "python3 mapper_A.py" \
      -reducer "python3 reducer_A.py"
  else
    echo "hadoop wrapper not found at $HADOOP_CMD; attempting java -jar (may fail)"
    java -jar "$STREAMING_JAR" \
      -files ${HS_DIR}/mapper_A.py,${HS_DIR}/reducer_A.py \
      -input "$HDFS_DIR/covid_data.csv" \
      -output "$HDFS_DIR/output" \
      -mapper "python3 ${HS_DIR}/mapper_A.py" \
      -reducer "python3 ${HS_DIR}/reducer_A.py"
  fi
else
  echo "Hadoop streaming jar not found; running local fallback processor"
  if [ -f "${HS_DIR}/process_A.py" ]; then
    python3 "${HS_DIR}/process_A.py" "$DATASET_PATH"
  else
    echo "Error: fallback processor ${HS_DIR}/process_A.py not found" >&2
    exit 2
  fi
fi
