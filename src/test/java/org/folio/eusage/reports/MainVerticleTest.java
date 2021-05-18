package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.eusage.reports.postgres.TenantPgPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  private static PostgreSQLContainer<?> postgresSQLContainer;
  private static PgConnectOptions pgConnectOptions = new PgConnectOptions();

  static Vertx vertx;
  static int port = 9230;

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine");
    postgresSQLContainer.start();
    pgConnectOptions.setHost(postgresSQLContainer.getHost());
    pgConnectOptions.setPort(postgresSQLContainer.getFirstMappedPort());
    pgConnectOptions.setUser(postgresSQLContainer.getUsername());
    pgConnectOptions.setPassword(postgresSQLContainer.getPassword());
    pgConnectOptions.setDatabase(postgresSQLContainer.getDatabaseName());
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = port;
    DeploymentOptions deploymentOptions = new DeploymentOptions();
    deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(port)));
    vertx.deployVerticle(new MainVerticle(), deploymentOptions).onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    postgresSQLContainer.close();
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testAdminHealth(TestContext context) {
    RestAssured.given()
        .get("/admin/health")
        .then().statusCode(200)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testEUsageVersionOK(TestContext context) {
    RestAssured.given()
        .get("/eusage/version")
        .then().statusCode(200)
        .body(containsString("0.0"));
  }
}
