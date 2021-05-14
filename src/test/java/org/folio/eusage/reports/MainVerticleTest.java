package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgConnectOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.eusage.reports.postgres.TenantPgPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticle");

  private static PostgreSQLContainer<?> postgresSQLContainer;

  Vertx vertx;
  WebClient webClient;
  int port = 9230;

  @BeforeClass
  public static void beforeClass(TestContext context) {
    postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine");
    postgresSQLContainer.start();
    PgConnectOptions pgConnectOptions = new PgConnectOptions();
    pgConnectOptions.setHost(postgresSQLContainer.getHost());
    pgConnectOptions.setPort(postgresSQLContainer.getFirstMappedPort());
    pgConnectOptions.setUser(postgresSQLContainer.getUsername());
    pgConnectOptions.setPassword(postgresSQLContainer.getPassword());
    pgConnectOptions.setDatabase(postgresSQLContainer.getDatabaseName());
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    postgresSQLContainer.close();
  }

  @Before
  public void setup(TestContext context) {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = port;
    vertx = Vertx.vertx();
    webClient = WebClient.create(vertx);

    DeploymentOptions deploymentOptions = new DeploymentOptions();
    deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(port)));
    vertx.deployVerticle(new MainVerticle(), deploymentOptions).onComplete(context.asyncAssertSuccess());
  }

  @After
  public void after(TestContext context) {
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
  public void testPostTenantOK(TestContext context) {
    String tenant = "testlib";
    ExtractableResponse<Response> response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location)
        .then().statusCode(200);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(404);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location)
        .then().statusCode(404);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\", \"purge\":true}")
        .post("/_/tenant")
        .then().statusCode(204);
  }

  @Test
  public void testPostMissingTenant(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetMissingTenant(TestContext context){
    String id = UUID.randomUUID().toString();
    RestAssured.given()
        .get("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetBadId(TestContext context){
    String id = "1234";
    RestAssured.given()
        .header("X-Okapi-Tenant", "testlib")
        .get("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Invalid UUID string"));
  }

  @Test
  public void testDeleteMissingTenant(TestContext context){
    String id = UUID.randomUUID().toString();
    RestAssured.given()
        .delete("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testDeleteBadId(TestContext context){
    String id = "1234";
    RestAssured.given()
        .header("X-Okapi-Tenant", "testlib")
        .delete("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Invalid UUID string"));
  }

  @Test
  public void testPostTenantBadJson(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"")
        .post("/_/tenant")
        .then().statusCode(400);
  }

  @Test
  public void testPostTenantBadType(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : true}")
        .post("/_/tenant")
        .then().statusCode(400);
  }

  @Test
  public void testPostTenantAdditional(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\", \"extra\":true}")
        .post("/_/tenant")
        .then().statusCode(400);
  }

  @Test
  public void testEUsageVersionOK(TestContext context) {
    RestAssured.given()
        .get("/eusage/version")
        .then().statusCode(200)
        .body(containsString("0.0"));
  }
}
