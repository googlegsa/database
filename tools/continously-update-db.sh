#!/bin/bash

set -e

# make and push new data to db every some seconds until CTRL-C

SLEEP_SECS=5

function log {
  >&2 echo "$@"
}


PROG_DIR=$(cd "$(dirname -- "$0")" && pwd)
TMP_SQL_FILE="$PROG_DIR/sql-inserts"
LOG_PROPS_FILE="$PROG_DIR/../src/logging.properties"
INSERT_SQL_PROG_DIR="$PROG_DIR/../dist"
cd "$PROG_DIR"


if [ -z "$JAVA_HOME" ]; then
  log error: need JAVA_HOME be set
  exit 2
fi

if [ -z "$DB_JAR" ]; then
  log error: need DB_JAR be set
  exit 2
fi

if [ ! -d "$INSERT_SQL_PROG_DIR" ]; then
  log error: "$INSERT_SQL_PROG_DIR" needs to exist
  exit 2
fi

ADAPTOR_MATCHES=$(echo "$INSERT_SQL_PROG_DIR"/adaptor-database-*-withlib.jar)
if [ ! -f "$ADAPTOR_MATCHES" ]; then
  log error: need single approprite adaptor jar
  exit 2
fi
ADAPTOR_JAR="$ADAPTOR_MATCHES"

if [ -z "$DB_CLASS" ]; then
  log error: need DB_CLASS be set
  exit 2
fi

if [ -z "$DB_URL" ]; then
  log error: need DB_URL be set
  exit 2
fi

if [ -z "$DB_USER" ]; then
  log error: need DB_USER be set
  exit 2
fi

if [ -z "$DB_PASSWORD" ]; then
  log error: need DB_PASSWORD be set
  exit 2
fi

while true; do
  # remember to set make-sql-inserts.py to appropriate output size
  python "$PROG_DIR"/make-sql-inserts.py | sed "s/\"/'/g" > "$TMP_SQL_FILE"
  log info: made update file

  cd "$INSERT_SQL_PROG_DIR"
  "$JAVA_HOME/bin/java" \
      -cp "$ADAPTOR_JAR:$DB_JAR" \
      -Djava.util.logging.config.file="$LOG_PROPS_FILE" \
      com.google.enterprise.adaptor.database.InsertSql \
      "$DB_CLASS" "$DB_URL" "$DB_USER" "$DB_PASSWORD" "$TMP_SQL_FILE"
  log info: ran sql inserts with rc $?
  cd "$PROG_DIR"
  rm "$TMP_SQL_FILE"

  log info: about to sleep "$SLEEP_SECS"
  sleep "$SLEEP_SECS"
  log info: awoke from sleep

done
