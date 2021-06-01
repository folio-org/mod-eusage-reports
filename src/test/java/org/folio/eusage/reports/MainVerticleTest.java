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
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  static Vertx vertx;
  static final int MODULE_PORT = 9230;
  static final int MOCK_PORT = 9231;

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  static void getCounterReportsChunk(RoutingContext ctx, AtomicInteger cnt, int max) {
    if (cnt.incrementAndGet() >= max) {
      ctx.response().end("], \"totalRecords\": " + max + "}");
      return;
    }
    String lead = cnt.get() > 1 ? "," : "";
    JsonObject counterReport = new JsonObject();
    counterReport.put("id", UUID.randomUUID().toString());
    counterReport.put("yearMonth", "2021-01");
    JsonObject report = new JsonObject();
    counterReport.put("report", report);
    report.put("vendor", new JsonObject()
        .put("id", "This is take vendor")
        .put("contact", new JsonArray())
    );
    report.put("name", "JR1");
    report.put("title", "Journal Report " + cnt.get());
    report.put("customer", new JsonArray()
        .add(new JsonObject()
            .put("id", "fake customer id")
            .put("reportedItems", new JsonArray()
                .add(new JsonObject()
                    .put("itemName", "The cats journal")
                    .put("itemDataType", "JOURNAL")
                    .put("itemIdentifier", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "DOI")
                            .put("value", "10.10XX")
                        )
                        .add(new JsonObject()
                            .put("type", "PRINT_ISSN")
                            .put("value", "1111-2222")
                        )
                        .add(new JsonObject()
                            .put("type", "ONLINE_ISSN")
                            .put("value", "3333-4444")
                        )
                    )
                )
                .add(new JsonObject()
                    .put("itemName", "The dogs journal")
                    .put("itemDataType", "JOURNAL")
                    .put("itemIdentifier", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "DOI")
                            .put("value", "10.10YY")
                        )
                        .add(new JsonObject()
                            .put("type", "PRINT_ISSN")
                            .put("value", "1111-2222")
                        )
                        .add(new JsonObject()
                            .put("type", "ONLINE_ISSN")
                            .put("value", "3333-4444")
                        )
                    )
                )
                .add(new JsonObject()
                    .put("itemName", "Best " + cnt.get() + " pets of all time")
                    .put("itemDataType", "JOURNAL")
                    .put("itemIdentifier", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "DOI")
                            .put("value", "10.10ZZ")
                        )
                        .add(new JsonObject()
                            .put("type", "PRINT_ISSN")
                            .put("value", "1111-2222")
                        )
                        .add(new JsonObject()
                            .put("type", "ONLINE_ISSN")
                            .put("value", "3333-4444")
                        )
                    )
                )
            )
        )
    );
    ctx.response().write(lead + counterReport.encode())
        .onComplete(x -> getCounterReportsChunk(ctx, cnt, max));
  }

  static void getCounterReports(RoutingContext ctx) {
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");

    ctx.response().write("{ \"counterReports\": [ ")
        .onComplete(x ->
            getCounterReportsChunk(ctx, new AtomicInteger(0), 5));
  }

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = MODULE_PORT;

    Router router = Router.router(vertx);
    router.get("/counter-reports").handler(MainVerticleTest::getCounterReports);
    vertx.createHttpServer()
        .requestHandler(router)
        .listen(MOCK_PORT)
        .compose(x -> {
          DeploymentOptions deploymentOptions = new DeploymentOptions();
          deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(MODULE_PORT)));
          return vertx.deployVerticle(new MainVerticle(), deploymentOptions);
        })
        .onComplete(context.asyncAssertSuccess());
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
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("testlib_mod_eusage_reports.te_table"));
  }

  @Test
  public void testGetTitlesNoOkapiUrl(TestContext context) {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .get("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Missing " + XOkapiHeaders.URL));
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
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
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
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
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
