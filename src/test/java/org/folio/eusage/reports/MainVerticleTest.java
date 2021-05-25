package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
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
  public void testGetTitlesNoInit(TestContext context) {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("testlib_mod_eusage_reports.te_table"));
  }

  String tenantOp(TestContext context, String tenant, JsonObject tenantAttributes, String expectedError) {
    ExtractableResponse<Response> response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(tenantAttributes.encode())
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .get(location + "?wait=10000")
        .then().statusCode(200)
        .extract();

    context.assertTrue(response.path("complete"));
    context.assertEquals(expectedError, response.path("error"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .delete(location)
        .then().statusCode(204);
    return location;
  }

  @Test
  public void testPostTenantOK(TestContext context) {
    String tenant = "testlib";
    tenantOp(context, tenant, new JsonObject()
            .put("module_to", "mod-eusage-reports-1.0.0"), null);

    ExtractableResponse<Response> response;

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/eusage-reports/report-titles")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    JsonObject res = new JsonObject(response.body().asString());
    context.assertEquals(0, res.getJsonArray("titles").size());

    tenantOp(context, tenant, new JsonObject()
        .put("module_from", "mod-eusage-reports-1.0.0")
        .put("module_to", "mod-eusage-reports-1.0.1")
        .put("parameters", new JsonArray()
            .add(new JsonObject()
                .put("key", "loadReference")
                .put("value", "true")
            )
            .add(new JsonObject()
                .put("key", "loadSample")
                .put("value", "true")
            )
        ), null);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/eusage-reports/report-titles")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    res = new JsonObject(response.body().asString());
    context.assertEquals(100, res.getJsonArray("titles").size());

    // disable
    tenantOp(context, tenant,
        new JsonObject().put("module_from", "mod-eusage-reports-1.0.0"), null);

    // purge
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(
            new JsonObject()
                .put("module_from", "mod-eusage-reports-1.0.0")
                .put("purge", true).encode())
        .post("/_/tenant")
        .then().statusCode(204);
  }
}
