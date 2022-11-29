package io.gingersnapproject.cdc.connector;

import static io.debezium.connector.sqlserver.SqlServerConnectorConfig.DATABASE_NAMES;

import java.util.Properties;
import java.util.Random;

import io.gingersnapproject.cdc.configuration.Connector;
import io.gingersnapproject.cdc.configuration.Database;

import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.sqlserver.SqlServerConnector;

public enum DatabaseProvider {
   MYSQL {
      @Override
      public Properties databaseProperties(Connector connector, Database database) {
         Properties properties = new Properties();
         properties.setProperty("connector.class", MySqlConnector.class.getCanonicalName());
         properties.setProperty("table.include.list", String.format("%s.%s", connector.schema(), connector.table()));

         // MySQL requires a unique id, this can change between restarts.
         properties.setProperty("database.server.id", String.valueOf(Math.abs(new Random().nextInt())));

         return properties;
      }
   },
   POSTGRESQL {
      @Override
      public Properties databaseProperties(Connector connector, Database database) {
         Properties properties = new Properties();
         properties.setProperty("connector.class", PostgresConnector.class.getCanonicalName());
         properties.setProperty("database.dbname", database.database().orElseThrow(() -> new IllegalStateException("Postgres requires database name.")));
         properties.setProperty("schema.include.list", connector.schema());
         properties.setProperty("table.include.list", String.format("%s.%s", connector.schema(), connector.table()));

         // Using `pgoutput` to parse the output should use a filtered publication.
         // This will create a new publication for the filtered tables, which debezium listens.
         // The filter is based on `schema.include/exclude.list` and `table.include/exclude.list`.
         properties.setProperty("publication.autocreate.mode", "filtered");
         properties.setProperty("plugin.name", "pgoutput");

         return properties;
      }
   },
   SQLSERVER {
      @Override
      public Properties databaseProperties(Connector connector, Database database) {
         Properties properties = new Properties();
         String databaseName = database.database().orElseThrow(() -> new IllegalStateException("SQL Server requires database name."));
         properties.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());
         properties.setProperty(DATABASE_NAMES.name(), databaseName);
         properties.setProperty("table.include.list", String.format("%s.%s", connector.schema(), connector.table()));
         properties.setProperty("database.encrypt", "false");

         // SQL Server has a slightly different naming
         String schemaRegex = String.format("/.*\\.%s\\.%s\\.%s\\..*/", databaseName, connector.schema(), connector.table());
         properties.setProperty("transforms.filter.condition", "value.source.table == 'customer' && valueSchema.name ==~ " + schemaRegex);
         return properties;
      }
   };

   public abstract Properties databaseProperties(Connector connector, Database database);
}
