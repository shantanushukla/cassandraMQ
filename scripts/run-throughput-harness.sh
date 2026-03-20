#!/usr/bin/env bash
set -euo pipefail

messages="${PERF_MESSAGES:-100}"
producers="${PERF_PRODUCERS:-2}"
consumers="${PERF_CONSUMERS:-2}"
total_shards="${PERF_TOTAL_SHARDS:-16}"
print_every="${PERF_PRINT_EVERY:-0}"
max_wait_seconds="${PERF_MAX_WAIT_SECONDS:-120}"
queue="${PERF_QUEUE:-perf-$(date +%s)}"

bundle_path="${CASSANDRA_SECURE_CONNECT_BUNDLE_PATH:-/secrets/secure-connect-bundle.zip}"
client_id="${CASSANDRA_CLIENT_ID:-}"
client_secret="${CASSANDRA_CLIENT_SECRET:-}"
consistency="${CASSANDRA_CONSISTENCY:-LOCAL_QUORUM}"
execution_threads="${CASSANDRAMQ_EXECUTION_THREAD_POOL_SIZE:-8}"

if [[ -z "${client_id}" ]]; then
  echo "CASSANDRA_CLIENT_ID is required" >&2
  exit 1
fi

if [[ -z "${client_secret}" ]]; then
  echo "CASSANDRA_CLIENT_SECRET is required" >&2
  exit 1
fi

if [[ ! -f "${bundle_path}" ]]; then
  echo "Secure connect bundle not found at ${bundle_path}" >&2
  exit 1
fi

exec mvn -q -DskipTests test-compile exec:java \
  -Dexec.mainClass=org.cassandramq.perf.QueueThroughputHarness \
  -Dexec.classpathScope=test \
  -Dcassandramq.perf.messages="${messages}" \
  -Dcassandramq.perf.producers="${producers}" \
  -Dcassandramq.perf.consumers="${consumers}" \
  -Dcassandramq.perf.total-shards="${total_shards}" \
  -Dcassandramq.perf.print-every="${print_every}" \
  -Dcassandramq.perf.max-wait-seconds="${max_wait_seconds}" \
  -Dcassandramq.perf.queue="${queue}" \
  -Dcassandramq.cassandra.secure-connect-bundle-path="${bundle_path}" \
  -Dcassandramq.cassandra.client-id="${client_id}" \
  -Dcassandramq.cassandra.client-secret="${client_secret}" \
  -Dcassandramq.cassandra.consistency="${consistency}" \
  -Dcassandramq.execution.thread-pool-size="${execution_threads}"
