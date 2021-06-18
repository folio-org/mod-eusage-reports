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

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
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
  static final int MODULE_PORT = 9230;
  static final int MOCK_PORT = 9231;
  static final UUID goodKbTitleId = UUID.randomUUID();
  static final UUID goodCounterReportId = UUID.randomUUID();
  static final UUID badJsonCounterReportId = UUID.randomUUID();
  static final UUID badStatusCounterReportId = UUID.randomUUID();
  static final UUID goodAgreementId = UUID.randomUUID();
  static final UUID badJsonAgreementId = UUID.randomUUID();
  static final UUID badStatusAgreementId = UUID.randomUUID();
  static final UUID badStatusAgreementId2 = UUID.randomUUID();
  static final UUID usageProviderId = UUID.randomUUID();
  static final UUID agreementLineIds[] = {
      UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID poLineIds[] = {
      UUID.randomUUID(), UUID.randomUUID()
  };

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  static JsonObject getCounterReportMock(UUID id, int cnt) {
    JsonObject counterReport = new JsonObject();
    counterReport.put("id", id);
    counterReport.put("providerId", usageProviderId);
    counterReport.put("yearMonth", "2021-01");
    JsonObject report = new JsonObject();
    counterReport.put("report", report);
    report.put("vendor", new JsonObject()
        .put("id", "This is take vendor")
        .put("contact", new JsonArray())
    );
    report.put("providerId", "not_inspected");
    report.put("yearMonth", "not_inspected");
    report.put("name", "JR1");
    report.put("title", "Journal Report " + cnt);
    report.put("customer", new JsonArray()
        .add(new JsonObject()
            .put("id", "fake customer id")
            .put(cnt > 1 ? "reportItems" : "Report_Items", new JsonArray()
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
                            .put("value", "1000-1001")
                        )
                        .add(new JsonObject()
                            .put("type", "ONLINE_ISSN")
                            .put("value", "1000-1002")
                        )
                    )
                    .put("itemPerformance", new JsonArray()
                        .add(new JsonObject()
                            .put("category", "REQUESTS")
                            .put("instance", new JsonArray()
                                .add(new JsonObject()
                                    .put("count", 5)
                                    .put("metricType", "FT_TOTAL")
                                )
                                .add(new JsonObject()
                                    .put("count", 3)
                                    .put("metricType", "FT_PDF")
                                )
                            )
                        )
                    )
                )
                .add(new JsonObject()
                    .put("Title", "The dogs journal")
                    .put("itemDataType", "JOURNAL")
                    .put("Item_ID", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "DOI")
                            .put("value", "10.10YY")
                        )
                        .add(null)
                        .add(new JsonObject()
                            .put("type", "Print_ISSN")
                            .put("value", "1001-1001")
                        )
                        .add(new JsonObject()
                            .put("type", "Online_ISSN")
                            .put("value", "1001-1002")
                        )
                    )
                    .put("Performance", new JsonArray()
                        .add(null)
                        .add(new JsonObject()
                            .put("category", "REQUESTS")
                            .put("instance", new JsonArray()
                                .add(new JsonObject()
                                    .put("Count", 5)
                                    .put("metricType", "FT_TOTAL")
                                )
                                .add(null)
                                .add(new JsonObject()
                                    .put("Count", 3)
                                    .put("metricType", "FT_PDF")
                                )
                            )
                        )
                    )
                )
                .add(new JsonObject()
                    .put("itemName", "Best " + cnt + " pets of all time")
                    .put("itemDataType", "JOURNAL")
                    .put("itemIdentifier", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "DOI")
                            .put("value", "10.10ZZ")
                        )
                        .add(new JsonObject()
                            .put("type", "PRINT_ISSN")
                            .put("value", "1002-" + String.format("%04d", cnt))
                        )
                        .add(new JsonObject()
                            .put("type", "ONLINE_ISSN")
                            .put("value", "1003-" + String.format("%04d", cnt))
                        )
                    )
                    .put("itemPerformance", new JsonArray()
                        .add(new JsonObject()
                            .put("category", "REQUESTS")
                            .put("instance", new JsonArray()
                                .add(new JsonObject()
                                    .put("count", 5)
                                    .put("metricType", "FT_TOTAL")
                                )
                                .add(new JsonObject()
                                    .put("count", 3)
                                    .put("metricType", "FT_PDF")
                                )
                            )
                        )
                    )
                )
                .add(new JsonObject()
                    .put("itemName", "No match")
                    .put("itemDataType", "JOURNAL")
                    .put("itemIdentifier", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "DOI")
                            .put("value", "10.10ZZ")
                        )
                        .add(new JsonObject()
                            .put("type", "PRINT_ISSN")
                            .put("value", "1002-" + String.format("%04d", cnt))
                        )
                    )
                    .put("itemPerformance", new JsonArray())
                )
            )
        )
    );
    return counterReport;
  }


  static void getCounterReportsChunk(RoutingContext ctx, AtomicInteger cnt, int max) {
    if (cnt.incrementAndGet() > max) {
      ctx.response().end("], \"totalRecords\": " + max + "}");
      return;
    }
    String lead = cnt.get() > 1 ? "," : "";
    JsonObject counterReport = getCounterReportMock(UUID.randomUUID(), cnt.get());
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

  static void getCounterReport(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    if (id.equals(goodCounterReportId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      ctx.response().end(getCounterReportMock(id, 0).encode());
    } else  if (id.equals(badStatusCounterReportId)) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(403);
      ctx.response().end("forbidden");
    } else  if (id.equals(badJsonCounterReportId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      ctx.response().end("{");
    } else {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("not found");
    }
  }

  static void getErmResource(RoutingContext ctx) {
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    String term = ctx.request().getParam("term");
    JsonArray ar = new JsonArray();

    // return a known kbTitleId for "The cats journal"
    UUID kbTitleId = "1000-1002".equals(term) ? goodKbTitleId : UUID.randomUUID();
    if (!"1001-1002".equals(term)) { // for "The dogs journal" , no kb match
      ar.add(new JsonObject()
          .put("id", kbTitleId)
          .put("name", "fake kb title instance name")
      );
    }
    ctx.response().end(ar.encode());
  }

  static void getErmResourceEntitlement(RoutingContext ctx) {
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    String term = ctx.request().getParam("term");
    JsonArray ar = new JsonArray();
    if ("org.olf.kb.Pkg".equals(term)) {
      ar.add(new JsonObject()
          .put("id", UUID.randomUUID())
          .put("name", "fake kb package name")
      );
    }
    ctx.response().end(ar.encode());
  }

  static void getAgreement(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    if (id.equals(goodAgreementId) || id.equals(badStatusAgreementId2)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      ctx.response().end(new JsonObject().encode());
    } else if (id.equals(badStatusAgreementId)) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(403);
      ctx.response().end("forbidden");
    } else  if (id.equals(badJsonAgreementId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      ctx.response().end("{");
    } else {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("not found");
    }
  }

  static void getEntitlements(RoutingContext ctx) {
    String filters = ctx.request().getParam("filters");
    if (filters == null) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(400);
      ctx.response().end("filters missing");
      return;
    }
    UUID agreementId = badStatusAgreementId;
    if (filters.startsWith("owner=")) {
      agreementId = UUID.fromString(filters.substring(6));
    }
    if (agreementId.equals(badJsonAgreementId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      ctx.response().end("[{]");
      return;
    }
    if (agreementId.equals(badStatusAgreementId2)) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(500);
      ctx.response().end("internal error");
      return;
    }
    JsonArray ar = new JsonArray();
    if (agreementId.equals(goodAgreementId)) {
      for (int i = 0; i < agreementLineIds.length; i++) {
        JsonArray poLinesAr = new JsonArray();
        for (int j = 0; j < i && j < poLineIds.length; j++) {
          poLinesAr.add(new JsonObject()
              .put("poLineId", poLineIds[j])
          );
        }
        ar.add(new JsonObject()
            .put("id", agreementLineIds[i])
            .put("owner", new JsonObject()
                .put("id", goodAgreementId)
                .put("name", "Good agreement"))
            .put("resource", new JsonObject()
                .put("_object", new JsonObject()
                    .put("pti", new JsonObject()
                        .put("titleInstance", new JsonObject()
                            .put("id", i < 2 ? goodKbTitleId : UUID.randomUUID())
                            .put("publicationType", new JsonObject()
                                .put("value", "serial")
                            )
                        )
                    )
                )
            )
            .put("poLines", poLinesAr)
        );
      }
    }
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().end(ar.encode());
  }

  static List<String> orderLinesCurrencies = new LinkedList<>();

  static void getOrderLines(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    for (int i = 0; i < poLineIds.length; i++) {
      if (id.equals(poLineIds[i])) {
        ctx.response().setChunked(true);
        ctx.response().putHeader("Content-Type", "application/json");
        JsonObject orderLine = new JsonObject();
        orderLine.put("id", id);
        String currency = i < orderLinesCurrencies.size() ? orderLinesCurrencies.get(i) : "USD";
        orderLine.put("cost", new JsonObject()
            .put("currency", currency)
            .put("listUnitPriceElectronic", 100.0 + (i * i))
            );
        ctx.response().end(orderLine.encode());
        return;
      }
    }
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().setStatusCode(404);
    ctx.response().end("Order line not found");
  }

  static void getInvoiceLines(RoutingContext ctx) {
    String query = ctx.request().getParam("query");
    if (query == null || !query.startsWith("poLineId==")) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(400);
      ctx.response().end("query missing");
      return;
    }
    UUID poLineId = UUID.fromString(query.substring(10));

    JsonArray ar = new JsonArray();
    for (int i = 0; i < poLineIds.length; i++) {
      if (poLineId.equals(poLineIds[i])) {
        ar.add(new JsonObject()
            .put("poLineId", poLineId)
            .put("quantity", 1 + i)
            .put("subTotal", 10.0 + i * 5)
            .put("total", 12.0 + i * 6)
        );
      }
    }
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().end(ar.encode());
  }

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = MODULE_PORT;

    Router router = Router.router(vertx);
    router.getWithRegex("/counter-reports").handler(MainVerticleTest::getCounterReports);
    router.getWithRegex("/counter-reports/[-0-9a-z]*").handler(MainVerticleTest::getCounterReport);
    router.getWithRegex("/erm/resource").handler(MainVerticleTest::getErmResource);
    router.getWithRegex("/erm/resource/[-0-9a-z]*/entitlementOptions").handler(MainVerticleTest::getErmResourceEntitlement);
    router.getWithRegex("/erm/sas/[-0-9a-z]*").handler(MainVerticleTest::getAgreement);
    router.getWithRegex("/erm/entitlements").handler(MainVerticleTest::getEntitlements);
    router.getWithRegex("/orders/order-lines/[-0-9a-z]*").handler(MainVerticleTest::getOrderLines);
    router.getWithRegex("/invoice-storage/invoice-lines").handler(MainVerticleTest::getInvoiceLines);
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
  public void testAdminHealth() {
    RestAssured.given()
        .get("/admin/health")
        .then().statusCode(200)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetTitlesNoInit() {
    String tenant = "testlib";
    for (int i = 0; i < 5; i++) { // would hang wo connection close
      RestAssured.given()
          .header(XOkapiHeaders.TENANT, tenant)
          .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
          .get("/eusage-reports/report-titles")
          .then().statusCode(400)
          .header("Content-Type", is("text/plain"))
          .body(containsString("testlib_mod_eusage_reports.te_table"));
    }
  }

  @Test
  public void testGetTitlesNoOkapiUrl() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body("{}")
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Missing " + XOkapiHeaders.URL));
  }

  @Test
  public void testPostTitlesNoInit() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject().put("titles", new JsonArray()).encode()) // no titles
        .post("/eusage-reports/report-titles")
        .then().statusCode(204);
  }

  @Test
  public void testPostTitlesNoInit2() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject().put("titles", new JsonArray()
            .add(new JsonObject()
                .put("id", UUID.randomUUID().toString())
                .put("kbTitleName", "kb title name")
                .put("kbTitleId", UUID.randomUUID().toString())
            )
        ).encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .body(containsString("testlib_mod_eusage_reports.te_table"));
  }

  void tenantOp(TestContext context, String tenant, JsonObject tenantAttributes, String expectedError) {
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
  }

  @Test
  public void testFromCounterMissingOkapiUrl(TestContext context) {
    String tenant = "testlib";

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", goodCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Missing X-Okapi-Url"));
  }

  @Test
  public void testFromAgreementNoId(TestContext context) {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject().encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Missing agreementId property"));
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
    JsonObject resObject = new JsonObject(response.body().asString());
    context.assertEquals(0, resObject.getJsonArray("titles").size());

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
        .header("Content-Type", "application/json")
        .body("{}")
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    JsonArray titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(7, titlesAr.size());
    int noDefined = 0;
    int noUndefined = 0;
    JsonObject unmatchedTitle = null;
    for (int i = 0; i < titlesAr.size(); i++) {
      if (titlesAr.getJsonObject(i).containsKey("kbTitleName")) {
        noDefined++;
        context.assertEquals("fake kb title instance name", titlesAr.getJsonObject(i).getString("kbTitleName"));
      } else {
        unmatchedTitle = titlesAr.getJsonObject(i);
        context.assertEquals("The dogs journal", unmatchedTitle.getString("counterReportTitle"));
        noUndefined++;
      }
    }
    context.assertEquals(6, noDefined);
    context.assertEquals(1, noUndefined);

    unmatchedTitle.put("kbTitleName", "correct kb title name");
    unmatchedTitle.put("kbTitleId", UUID.randomUUID().toString());
    JsonObject postTitleObject = new JsonObject();
    postTitleObject.put("titles", new JsonArray().add(unmatchedTitle));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(postTitleObject.encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(204);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(7, titlesAr.size());
    int noManual = 0;
    for (int i = 0; i < titlesAr.size(); i++) {
      context.assertTrue(titlesAr.getJsonObject(i).containsKey("kbTitleName"));
      if (titlesAr.getJsonObject(i).getBoolean("kbManualMatch")) {
        noManual++;
      }
    }
    context.assertEquals(1, noManual);

    JsonObject n = new JsonObject();
    n.put("id", UUID.randomUUID());
    n.put("kbTitleName", "correct kb title name");
    n.put("kbTitleId", UUID.randomUUID().toString());
    postTitleObject = new JsonObject();
    postTitleObject.put("titles", new JsonArray().add(n));
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(postTitleObject.encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("title " + n.getString("id") + " matches nothing"));

    // missing kbTitleId
    n = new JsonObject();
    n.put("id", UUID.randomUUID());
    n.put("kbTitleName", "correct kb title name");
    postTitleObject = new JsonObject();
    postTitleObject.put("titles", new JsonArray().add(n));
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(postTitleObject.encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));

    // missing id
    n = new JsonObject();
    n.put("kbTitleName", "correct kb title name");
    n.put("kbTitleId", UUID.randomUUID().toString());
    postTitleObject = new JsonObject();
    postTitleObject.put("titles", new JsonArray().add(n));
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(postTitleObject.encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .body(is("Bad Request"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/title-data")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(15, resObject.getJsonArray("data").size());
    resObject = resObject.getJsonArray("data").getJsonObject(0);
    context.assertEquals(usageProviderId.toString(), resObject.getString("providerId"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", goodCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(8, resObject.getJsonArray("titles").size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?counterReportId=" + goodCounterReportId.toString())
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(3, titlesAr.size());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?counterReportId=x")
        .then().statusCode(400)
        .body(is("Invalid UUID string: x"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?providerId=" + usageProviderId.toString())
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(8, titlesAr.size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?providerId=" + UUID.randomUUID().toString())
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(0, titlesAr.size());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", badJsonCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("returned bad JSON"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", badStatusCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("returned status code 403"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", UUID.randomUUID()) // unknown ID
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(404);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(0, resObject.getJsonArray("data").size());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("agreementId", UUID.randomUUID()) // unknown ID
            .encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(404);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("agreementId", badJsonAgreementId)
            .encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(400)
        .body(containsString("Failed to decode"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("agreementId", badStatusAgreementId)
            .encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("returned status code 403"));

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("agreementId", badStatusAgreementId2)
            .encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("returned status code 500"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("agreementId", goodAgreementId)
            .encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(3, resObject.getInteger("reportLinesCreated"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(3, resObject.getJsonArray("data").size());
    log.info("AD: {}", resObject.encodePrettily());

    orderLinesCurrencies.clear();
    orderLinesCurrencies.add("DKK");
    orderLinesCurrencies.add("EUR");

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("agreementId", goodAgreementId)
            .encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Mixed currencies"));

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
