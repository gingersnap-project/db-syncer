package io.gingersnapproject.cdc;

import static io.gingersnapproject.cdc.PublishChangesTest.RULE_NAME;
import static io.gingersnapproject.util.Utils.eventually;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import io.gingersnapproject.cdc.event.NotificationManager;
import io.gingersnapproject.testcontainers.BaseContainerTest;
import io.gingersnapproject.testcontainers.annotation.WithDatabase;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@QuarkusTest
@WithDatabase(rules = RULE_NAME)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PublishChangesTest extends BaseContainerTest {
   static final String RULE_NAME = "cdctest";

   @Inject NotificationManager realNotification;

   private NotificationManager spyNotification;

   @BeforeAll
   void beforeAll() {
      spyNotification = spy(realNotification);
      QuarkusMock.installMockForInstance(spyNotification, realNotification);
      RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
   }

   @Test
   public void testWritingEventsToBackend() {
      executeSqlStatements("INSERT INTO debezium.customer VALUES (1, 'name', 'email@redhat.com')");
      eventually(() -> "Value never written to backend", () -> hotRodGetById(RULE_NAME, "name")
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(1)
                        && readCaseInsensitive(data, "fullname").equals("name")
                        && readCaseInsensitive(data, "email").equals("email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);

      verify(spyNotification, never()).connectorFailed(any(), any());

      // Now we update the email to something new, the name must be the same.
      executeSqlStatements("UPDATE debezium.customer SET email = 'new.email@redhat.com' WHERE id = 1");
      eventually(() -> "Value never updated on backend", () -> hotRodGetById(RULE_NAME, "name")
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(1)
                        && readCaseInsensitive(data, "fullname").equals("name")
                        && readCaseInsensitive(data, "email").equals("new.email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);

      verify(spyNotification, never()).connectorFailed(any(), any());

      // Finally, we delete the entry. No way to tell if operation was applied correctly.
      // See: https://github.com/gingersnap-project/cache-manager/issues/65
      executeSqlStatements("DELETE FROM debezium.customer WHERE id = 1");
      eventually(() -> "Value changed after deletion", () -> hotRodGetById(RULE_NAME, "name")
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(1)
                        && readCaseInsensitive(data, "fullname").equals("name")
                        && readCaseInsensitive(data, "email").equals("new.email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);
      verify(spyNotification, never()).connectorFailed(any(), any());
   }

   @Test
   public void testMultipleUpdatesInSameKey() {
      executeSqlStatements("INSERT INTO debezium.customer VALUES (2, 'testMultipleUpdatesInSameKey', 'email@redhat.com')");
      eventually(() -> "Value never written to backend", () -> hotRodGetById(RULE_NAME, "testMultipleUpdatesInSameKey")
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(2)
                        && readCaseInsensitive(data, "fullname").equals("testMultipleUpdatesInSameKey")
                        && readCaseInsensitive(data, "email").equals("email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);

      verify(spyNotification, never()).connectorFailed(any(), any());

      // We use a tx, this means Debezium captures events of committed entries only.
      // We apply multiple operations to the user 2, internally we issue only the last.
      executeSqlStatements(
            "UPDATE debezium.customer SET email = 'mid.update@redhat.com' WHERE id = 2",
            "UPDATE debezium.customer SET email = 'another.update@redhat.com' WHERE id = 2",
            "UPDATE debezium.customer SET email = 'new.email@redhat.com' WHERE id = 2"
      );
      eventually(() -> "Value never updated on backend", () -> hotRodGetById(RULE_NAME, "testMultipleUpdatesInSameKey")
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(2)
                        && readCaseInsensitive(data, "fullname").equals("testMultipleUpdatesInSameKey")
                        && readCaseInsensitive(data, "email").equals("new.email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);

      verify(spyNotification, never()).connectorFailed(any(), any());
   }

   @Test
   public void testBatchOperationsMultipleKeys() {
      executeSqlStatements(
            "INSERT INTO debezium.customer VALUES (3, 'customer0', 'email@redhat.com')",
            "INSERT INTO debezium.customer VALUES (4, 'customer1', 'email@redhat.com')",
            "INSERT INTO debezium.customer VALUES (5, 'customer2', 'email@redhat.com')"
      );

      for (int i = 0; i < 3; i++) {
         String name = "customer" + i;
         int id = i + 3;
         eventually(() -> "Customer " + name + " never written", () -> hotRodGetById(RULE_NAME, name)
               .statusCode(200)
               .body("", new BaseMatcher<Map<String, Object>>() {
                  @Override
                  public boolean matches(Object actual) {
                     if (!(actual instanceof Map)) return false;

                     var data = (Map<String, ?>) actual;
                     return readCaseInsensitive(data, "id").equals(id)
                           && readCaseInsensitive(data, "fullname").equals(name)
                           && readCaseInsensitive(data, "email").equals("email@redhat.com");
                  }

                  @Override
                  public void describeTo(Description description) { }
               }), 15, TimeUnit.SECONDS);
      }
      verify(spyNotification, never()).connectorFailed(any(), any());

      // Single statement will change multiple rows.
      // This will remove the `email` column from the projection.
      executeSqlStatements("UPDATE debezium.customer SET email = NULL WHERE id IN (3, 4, 5)");
      for (int i = 0; i < 3; i++) {
         String name = "customer" + i;
         int id = i + 3;
         eventually(() -> "Customer " + name + " never updated to remove email", () -> hotRodGetById(RULE_NAME, name)
               .statusCode(200)
               .body("", new BaseMatcher<Map<String, Object>>() {
                  @Override
                  public boolean matches(Object actual) {
                     if (!(actual instanceof Map)) return false;

                     var data = (Map<String, ?>) actual;
                     return readCaseInsensitive(data, "id").equals(id)
                           && readCaseInsensitive(data, "fullname").equals(name)
                           && readCaseInsensitive(data, "email") == null;
                  }

                  @Override
                  public void describeTo(Description description) { }
               }), 15, TimeUnit.SECONDS);
      }
      verify(spyNotification, never()).connectorFailed(any(), any());
   }

   @Test
   public void testEventsToOtherTable() {
      // We insert to the customer, which we capture events, and for ca_model.
      executeSqlStatements(
            "INSERT INTO debezium.customer VALUES (6, 'testEventsToOtherTable', 'email@redhat.com')",
            "INSERT INTO debezium.car_model VALUES (1, 'QQ', 'Chery')"
      );

      // Customer is created!
      String name = "testEventsToOtherTable";
      eventually(() -> "Customer never written", () -> hotRodGetById(RULE_NAME, name)
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(6)
                        && readCaseInsensitive(data, "fullname").equals(name)
                        && readCaseInsensitive(data, "email").equals("email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);
      verify(spyNotification, never()).connectorFailed(any(), any());
   }

   @Test
   public void testRolledBackNotPublished() {
      String name = "testRolledBackNotPublished";
      executeSqlStatements("INSERT INTO debezium.customer VALUES (7, 'testRolledBackNotPublished', 'email@redhat.com')");
      eventually(() -> "Customer never written", () -> hotRodGetById(RULE_NAME, name)
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(7)
                        && readCaseInsensitive(data, "fullname").equals(name)
                        && readCaseInsensitive(data, "email").equals("email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);
      verify(spyNotification, never()).connectorFailed(any(), any());

      // Last statement fails, tx rollback. We check that these do not affect the engine.
      assertThrows(RuntimeException.class, () -> executeSqlStatements(
            "UPDATE debezium.customer SET email = 'mid.update@redhat.com' WHERE id = 7",
            "UPDATE debezium.customer SET email = 'another.update@redhat.com' WHERE id = 7",
            "UPDATE debezium.customer SET email = 'new.email@redhat.com' WHERE id = 7",
            "UPDATE debezium.customer SET unknown_column = 'hello@redhat.com' WHERE id = 7"
      ));
      executeSqlStatements("UPDATE debezium.customer SET email = 'updated.email@redhat.com' WHERE id = 7");
      eventually(() -> "Customer never written", () -> hotRodGetById(RULE_NAME, name)
            .statusCode(200)
            .body("", new BaseMatcher<Map<String, Object>>() {
               @Override
               public boolean matches(Object actual) {
                  if (!(actual instanceof Map)) return false;

                  var data = (Map<String, ?>) actual;
                  return readCaseInsensitive(data, "id").equals(7)
                        && readCaseInsensitive(data, "fullname").equals(name)
                        && readCaseInsensitive(data, "email").equals("updated.email@redhat.com");
               }

               @Override
               public void describeTo(Description description) { }
            }), 15, TimeUnit.SECONDS);
      verify(spyNotification, never()).connectorFailed(any(), any());
   }

   private Object readCaseInsensitive(Map<String, ?> data, String key) {
      Object d = data.get(key.toLowerCase());
      if (d == null) return data.get(key.toUpperCase());
      return d;
   }
}
