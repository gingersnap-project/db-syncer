create login gingersnap_login with password = 'Password!42'
GO

grant VIEW SERVER STATE to gingersnap_login
go

create database debezium;
go

use debezium;
go

create schema debezium;
go

create user gingersnap_user for login gingersnap_login
GO

alter role db_owner add member gingersnap_user
go

EXEC sp_addsrvrolemember 'gingersnap_login', 'sysadmin'
go

create table debezium.customer(id int primary key, fullname varchar(255), email varchar(255));
go
create table debezium.car_model(id int primary key, model varchar(255), brand varchar(255));
go

