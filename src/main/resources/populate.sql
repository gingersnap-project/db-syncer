-- Populate the database.

use debezium;

-- All these insert/update/delete will be captured.

insert into customer values (1, 'Jon Doe', 'jd@example.com');
insert into customer values (3, 'Bob', 'bob@example.com');
insert into customer values (4, 'Alice', 'alice@example.com');

update customer set fullname = 'Jane Doe' where id = 1;

delete from customer where id = 3;

-- These events are ignored.

insert into car_model values (1, 'QQ', 'Chery');
insert into car_model values (2, 'Beetle', 'VW');

-- Then captured events again.

insert into customer values (5, 'Mallory', 'mallory@example.com');

-- The transaction events are received only after commit.
start transaction;

insert into customer values (6, 'Fulano', 'fulano@example.com');
insert into customer values (7, 'Ciclano', 'ciclano@example.com');
insert into car_model values (3, 'Truck', 'Truck Company');
insert into customer values (8, 'Beltrano', 'beltrano@example.com');

-- No events issued with rollback.
rollback;

-- Receiving multiple deletes in a single batch;

delete from customer where id = 4;
