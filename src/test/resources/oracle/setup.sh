#!/bin/sh

# Notes:
# -> ORACLE_PASSWORD is set (not tested with ORACLE_RANDOM_PASSWORD set)
# -> XE -> Oracle Instance (ORACLE_SID variable)
# -> XEPDB1 -> Default pluggable database (ORACLE_DATABASE variable)
mkdir -p /opt/oracle/oradata/recovery_area

echo "[SETUP] Set Archive Mode"
sqlplus -S /nolog @/sql/enable-archive-mode.sql

echo "[SETUP] Enable LogMiner required database features/settings"
sqlplus -S sys/${ORACLE_PASSWORD}@//localhost:1521 as sysdba @/sql/enable-database-logging.sql

# Tablespace must be created in all pluggable databases!
echo "[SETUP] Create Log Miner Tablespace"
sqlplus -S sys/${ORACLE_PASSWORD}@//localhost:1521 as sysdba @/sql/create-xe-tablespace.sql
sqlplus -S sys/${ORACLE_PASSWORD}@//localhost:1521/XEPDB1 as sysdba @/sql/create-xepdb1-tablespace.sql

echo "[SETUP] Create Log Miner user"
sqlplus -S sys/${ORACLE_PASSWORD}@//localhost:1521 as sysdba @/sql/create-logminer-user.sql

echo "[SETUP] Create tables for testing"
sqlplus -S debezium/dbz@//localhost:1521/XEPDB1 @/sql/create-test-tables.sql