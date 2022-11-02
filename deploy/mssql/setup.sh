#!/bin/bash

set -e

/opt/mssql-tools/bin/sqlcmd -S sqlserver -U sa -P 'Password!42' -d master -i /tmp/setup.sql

sleep infinity
