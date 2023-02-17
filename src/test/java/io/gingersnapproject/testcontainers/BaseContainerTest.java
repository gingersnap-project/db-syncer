package io.gingersnapproject.testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import io.gingersnapproject.testcontainers.annotation.ContainerInject;
import io.gingersnapproject.testcontainers.hotrod.HotRodContainer;

import io.restassured.response.ValidatableResponse;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class BaseContainerTest {

   @ContainerInject JdbcDatabaseContainer<?> jdbc;
   @ContainerInject HotRodContainer<?> hotRod;

   protected final void executeSqlStatements(String ... sql) {
      try {
         Connection conn = jdbc.createConnection("");
         conn.setAutoCommit(false);
         try (Statement stmt = conn.createStatement()) {
            for (String s : sql) {
               stmt.execute(s);
            }

            conn.commit();
         } catch (SQLException e) {
            conn.rollback();
            throw e;
         }
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public final ValidatableResponse hotRodGetById(String rule, String id) {
      return hotRod.getById(rule, id);
   }
}
