#!/bin/bash

CMD=(ingest_pubnub.py)

if [[ -n "${DBHOST}" ]]; then
	CMD+=(--host "${DBHOST}")
fi

if [[ -n "${DBPORT}" ]]; then
	CMD+=(--port "${DBPORT}")
fi

if [[ -n "${DBNAME}" ]]; then
	CMD+=(--dbname "${DBNAME}")
fi

if [[ -n "${DBUSER}" ]]; then
	CMD+=(--username "${DBUSER}")
fi

if [[ -n "${DBPASS}" ]]; then
	CMD+=(--password "${DBPASS}")
fi

CMD+=(--repeats ${REPEATS:-1})

OUTPUTPATH="${OUTPUTDIR:-/home/ingest}ingest_stats_${HOSTNAME}_$(date '+%T').json"
CMD+=(--output "${OUTPUTPATH}")

echo "Running command: ${CMD[@]}" >&2

"${CMD[@]}"
