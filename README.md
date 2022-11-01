# debezium-poc Project

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: https://quarkus.io/ .

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:
```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at http://localhost:8080/q/dev/.

## Packaging and running the application

The application can be packaged using:
```shell script
./mvnw package
```
It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:
```shell script
./mvnw package -Dquarkus.package.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using: 
```shell script
./mvnw package -Pnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using: 
```shell script
./mvnw package -Pnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/debezium-poc-0.1-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult https://quarkus.io/guides/maven-tooling.

## Provided Code

### RESTEasy Reactive

Easily start your Reactive RESTful Web Services

[Related guide section...](https://quarkus.io/guides/getting-started-reactive#reactive-jax-rs-resources)

# Running with MySQL

Requirements:

1. Make sure docker is running and you have docker-compose available
2. Run `docker-compose -f src/main/resources/mysql/mysql.yml up` to start mysql exposed on 3306 and adminer on 8090
  Adminer can be looked at via a browser at localhost:8090 as an administration tool. Login is user: root, password: root. You may need to refresh the schema to see the debezium one.
3. Run `docker exec -i <container id> mysql -uroot -proot < src/main/resources/mysql/setup.sql` to create the debezium schema, two tables and give permissions to the gingersnap user
4. Run `docker run -it -p 11222:11222 -e USER="admin" -e PASS="password" infinispan/server` to start ISPN server
  Server console is available at localhost:11222
5. Run `quarkus dev` from the poc base directory.
  This will run with the quarkus endpoint on 8080 and remote JVM debug on 5005. Our client creates the cache it uses `debezium-cache`
6. Perform inserts and updates to the database
  Running `docker exec -i <container id> mysql -uroot -proot < src/main/resources/populate.sql` will execute some operations in the database.
  To exit, press q in the terminal running the application, so the offset is flushed.

# Running with Postgres

Requirements:

1. Make sure docker is running and you have docker-compose available
2. Run `docker-compose -f src/main/resources/pgsql/postgres.yml up` to start Postgres exposed on 5432
   * This will create the database, user, schema, and tables necessary for testing.
3. Run `docker run -it -p 11222:11222 -e USER="admin" -e PASS="password" infinispan/server` to start ISPN server 
   * Server console is available at localhost:11222
4. Run `quarkus dev -Dquarkus.profile=pgsql` from the poc base directory.
   * This will run with the quarkus endpoint on 8080 and remote JVM debug on 5005. Our client creates the cache it uses `debezium-cache`.
5. Perform inserts and updates to the database
   * Running `docker exec -i <container id> psql -U root -d debeziumdb -a < src/main/resources/populate.sql` will execute some operations in the database.
   * To exit, press q in the terminal running the application, so the offset is flushed.

# Running with SQL Server

To run with SQL server some additional setup is required.

1. Make sure docker is running and you have docker-compose available
2. Run `docker-compose -f src/main/resources/mssql/mssql.yml up` to start Postgres exposed on 5432
    * This will create the database, user, schema, and tables necessary for testing.
3. Run `docker run -it -p 11222:11222 -e USER="admin" -e PASS="password" infinispan/server` to start ISPN server
    * Server console is available at localhost:11222

Now is necessary to enable the SQL Server Agent and CDC for the tables:

1. Run `sh src/main/resources/mssql/after.sh`
   * This will start the agent and configure CDC for the `customer` table

Now with everything setup:

1. Run `quarkus dev -Dquarkus.profile=mssql` from the poc base directory.
    * This will run with the quarkus endpoint on 8080 and remote JVM debug on 5005.
2. Perform inserts and updates to the database. Use container with MS tools
    * Execute `docker exec -it mssql_tools_1 /bin/bash`
    * Inside the container execute `sqlcmd -S sqlserver -U sa -P 'Password!42' -d debezium`
    * Use T-SQL to issue commands
