#!/bin/bash

set -e

docker exec -it --user root mssql_sqlserver_1 /opt/mssql/bin/mssql-conf set sqlagent.enabled true
docker restart mssql_sqlserver_1

echo "[+] Waiting 5s for mssql restart"
sleep 5

docker exec -i mssql_tools_1 /opt/mssql-tools/bin/sqlcmd -S sqlserver -U sa -P 'Password!42' -d master -i /tmp/after.sql
