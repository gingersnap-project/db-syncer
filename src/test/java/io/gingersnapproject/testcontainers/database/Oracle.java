package io.gingersnapproject.testcontainers.database;

import static org.junit.Assert.assertEquals;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

import java.util.List;
import java.util.Map;

import io.gingersnapproject.testcontainers.DatabaseProvider;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;

/**
 * Starts and manages an Oracle Database container.
 * <p>
 * The database is pre-configured and ready to use by debzium connector.
 */
public class Oracle implements DatabaseProvider {

   private static final String IMAGE = "gvenzl/oracle-xe:21-slim";

   @Override
   public JdbcDatabaseContainer<?> createDatabase() {
      OracleContainer oracle = new OracleContainer(IMAGE) {

         @Override
         protected String constructUrlForConnection(String queryString) {
            // We override this so we can define the database in the URL.
            return String.format("jdbc:oracle:thin:@//%s:%d/%s", getHost(), getOraclePort(), getDatabaseName());
         }
      };

      oracle = oracle.usingSid() // for setup.sh be able to connect with "sys" username
            .withPassword("Password!42")
            .withCopyFileToContainer(forClasspathResource("oracle/setup.sh"), "/docker-entrypoint-initdb.d/setup.sh");
      addSqlFilesToContainer(oracle, List.of(
            "create-logminer-user.sql",
            "create-test-tables.sql",
            "create-xe-tablespace.sql",
            "create-xepdb1-tablespace.sql",
            "enable-archive-mode.sql",
            "enable-database-logging.sql"));
      // withDatabase() image has already a database set (XEPDB1)
      assertEquals("xe", oracle.getSid());
      assertEquals("xepdb1", oracle.getDatabaseName());
      return oracle;
   }

   @Override
   public Map<String, String> properties() {
      return Map.of(
            "gingersnap.database.type", "ORACLE",
            "gingersnap.database.database", "XE.XEPDB1",
            "gingersnap.database.username", "debezium",
            "gingersnap.database.password", "dbz"
      );
   }

   private static void addSqlFilesToContainer(OracleContainer container, List<String> sqlFiles) {
      for (String sqlFile : sqlFiles) {
         container.withCopyFileToContainer(forClasspathResource("oracle/" + sqlFile), "/sql/" + sqlFile);
      }
   }

}
