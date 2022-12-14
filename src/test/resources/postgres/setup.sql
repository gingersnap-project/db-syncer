create schema debezium;

create table debezium.customer (id int primary key, fullname varchar(255), email varchar(255));
create table debezium.car_model (id int primary key, model varchar(255), brand varchar(255));

alter user gingersnap_user REPLICATION LOGIN;

grant create on database debeziumdb to gingersnap_user;
grant usage, create on schema debezium to gingersnap_user;

alter table debezium.customer owner to gingersnap_user;
