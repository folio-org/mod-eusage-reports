package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.TenantPgPoolContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  static Vertx vertx;
  static int port = 9230;

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = port;
    DeploymentOptions deploymentOptions = new DeploymentOptions();
    deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(port)));
    vertx.deployVerticle(new MainVerticle(), deploymentOptions).onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
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

  @Test
  public void testPostTenantOK(TestContext context) {
    String tenant = "testlib";
    log.info("AD: POST begin");
    ExtractableResponse<Response> response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    log.info("AD: POST completed");
    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location + "?wait=10000")
        .then().statusCode(200)
        .extract();

    context.assertTrue(response.path("complete"));

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);


    response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_from\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    log.info("AD: POST completed");
    location = response.header("Location");
    tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location + "?wait=10000")
        .then().statusCode(200)
        .extract();

    context.assertTrue(response.path("complete"));

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_from\" : \"mod-eusage-reports-1.0.0\", \"purge\":true}")
        .post("/_/tenant")
        .then().statusCode(204);
  }


}
