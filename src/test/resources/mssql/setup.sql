create login gingersnap_login with password = 'Password!42';

grant VIEW SERVER STATE to gingersnap_login;

create database debezium;

use debezium;

create schema debezium;

create user gingersnap_user for login gingersnap_login;

alter role db_owner add member gingersnap_user;

EXEC sp_addsrvrolemember 'gingersnap_login', 'sysadmin';

create table debezium.customer(id int primary key, fullname varchar(255), email varchar(255));
create table debezium.car_model(id int primary key, model varchar(255), brand varchar(255));

EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = N'debezium', @source_name = N'customer', @role_name = NULL, @supports_net_changes = 0;
