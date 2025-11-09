#!/usr/bin/env bash
# Workload A: total cases per country using Spark
# This script assumes `data/data.csv` exists (runner copies dataset there).
echo "Running Spark workload A: top countries by total cases"
mkdir -p experiments/events
chmod a+rwx experiments/events || true

# prefer explicit non-plotting script path from covid_spread if present
if [ -f "covid_spread/bin/spark_scripts/spark_top_countries_noplot.py" ]; then
	SCRIPT="covid_spread/bin/spark_scripts/spark_top_countries_noplot.py"
elif [ -f "covid_spread/bin/spark_scripts/spark_top_countries.py" ]; then
	SCRIPT="covid_spread/bin/spark_scripts/spark_top_countries.py"
else
	SCRIPT="/tmp/experiment/scripts/spark_top_countries_noplot.py"
fi

# If requested, run inside the spark container
if [ "${USE_CONTAINERS:-0}" = "1" ] || [ -n "${SPARK_CONTAINER:-}" ]; then
	CONTAINER="${SPARK_CONTAINER:-project3-spark}"
	# copy the script into the container workdir
	docker cp "$SCRIPT" "$CONTAINER":/tmp/experiment/ 2>/dev/null || true
	docker exec "$CONTAINER" mkdir -p /tmp/experiment/events 2>/dev/null || true
	docker exec "$CONTAINER" bash -lc "cd /tmp/experiment && spark-submit --master local[4] --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/experiment/events $SCRIPT"
else
	spark-submit --master local[4] --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=experiments/events "$SCRIPT"
fi