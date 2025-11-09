#!/usr/bin/env bash
# Workload C: monthly trends using Spark
echo "Running Spark workload C: monthly trends"
# ensure event log dir exists (avoids FileNotFoundException in some spark versions)
mkdir -p experiments/events
chmod 777 experiments/events || true

# prefer explicit non-plotting script path from covid_spread if present
if [ -f "covid_spread/bin/spark_scripts/spark_trends_noplot.py" ]; then
	SCRIPT="covid_spread/bin/spark_scripts/spark_trends_noplot.py"
elif [ -f "covid_spread/bin/spark_scripts/spark_trends.py" ]; then
	SCRIPT="covid_spread/bin/spark_scripts/spark_trends.py"
else
	SCRIPT="/tmp/experiment/scripts/spark_trends_noplot.py"
fi

if [ "${USE_CONTAINERS:-0}" = "1" ] || [ -n "${SPARK_CONTAINER:-}" ]; then
	CONTAINER="${SPARK_CONTAINER:-project3-spark}"
	docker cp "$SCRIPT" "$CONTAINER":/tmp/experiment/ 2>/dev/null || true
	docker exec "$CONTAINER" mkdir -p /tmp/experiment/events 2>/dev/null || true
	docker exec "$CONTAINER" bash -lc "cd /tmp/experiment && spark-submit --master local[4] --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/experiment/events $(basename $SCRIPT)"
else
	spark-submit --master local[4] --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=experiments/events "$SCRIPT"
fi
