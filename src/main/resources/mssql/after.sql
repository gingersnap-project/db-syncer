use debezium
go

EXEC sys.sp_cdc_enable_db;
go

EXEC sys.sp_cdc_enable_table @source_schema = N'debezium', @source_name = N'customer', @role_name = NULL, @supports_net_changes = 0
go

