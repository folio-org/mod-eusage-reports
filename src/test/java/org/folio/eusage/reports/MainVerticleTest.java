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
  static final String pubDateSample = "1998-05-19";
  static final UUID goodKbTitleId = UUID.randomUUID();
  static final String goodKbTitleISSN = "1000-1002";
  static final String goodDoiValue = "publisherA:Code123";
  static final UUID otherKbTitleId = UUID.randomUUID();
  static final String otherKbTitleISSN = "1000-2000";
  static final String noMatchKbTitleISSN = "1001-1002";
  static final UUID goodCounterReportId = UUID.randomUUID();
  static final UUID otherCounterReportId = UUID.randomUUID();
  static final UUID badJsonCounterReportId = UUID.randomUUID();
  static final UUID badStatusCounterReportId = UUID.randomUUID();
  static final UUID goodAgreementId = UUID.randomUUID();
  static final UUID badJsonAgreementId = UUID.randomUUID();
  static final UUID badStatusAgreementId = UUID.randomUUID();
  static final UUID badStatusAgreementId2 = UUID.randomUUID();
  static final UUID usageProviderId = UUID.randomUUID();
  static final UUID goodFundId = UUID.randomUUID();
  static final UUID goodLedgerId = UUID.randomUUID();
  static final UUID goodFiscalYearId = UUID.randomUUID();
  static final UUID[] agreementLineIds = {
      UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID[] poLineIds = {
      UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID goodPackageId = UUID.randomUUID();
  static final UUID[] packageTitles = {
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
                            .put("value", goodDoiValue)
                        )
                        .add(new JsonObject()
                            .put("type", "PRINT_ISSN")
                            .put("value", goodKbTitleISSN)
                        )
                        .add(new JsonObject()
                            .put("type", "Publication_Date")
                            .put("value", pubDateSample)
                        )
                    )
                    .put("itemPerformance", new JsonArray()
                        .add(new JsonObject()
                            .put("Period", new JsonObject()
                                .put("End_Date", "2021-01-31")
                                .put("Begin_Date", "2021-01-01")
                            )
                            .put("category", "REQUESTS")
                            .put("instance", new JsonArray()
                                .add(new JsonObject()
                                    .put("count", 5)
                                    .put("Metric_Type", "Total_Item_Requests")
                                )
                                .add(new JsonObject()
                                    .put("count", 3)
                                    .put("Metric_Type", "Unique_Item_Requests")
                                )
                            )
                        )
                    )
                )
                .add(new JsonObject()
                    .put("Title", cnt == -1 ? "The other journal" : "The dogs journal")
                    .put("itemDataType", "JOURNAL")
                    .put("Item_ID", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "Print_ISSN")
                            .put("value", "1001-1001")
                        )
                        .add(null)
                        .add(new JsonObject()
                            .put("type", "Online_ISSN")
                            .put("value",  cnt == -1 ? otherKbTitleISSN : noMatchKbTitleISSN)
                        )
                    )
                    .put("Performance", new JsonArray()
                        .add(null)
                        .add(new JsonObject()
                            .put("Period", new JsonObject()
                                .put("End_Date", "2021-01-31")
                                .put("Begin_Date", "2021-01-01")
                            )
                            .put("category", "REQUESTS")
                            .put("instance", new JsonArray()
                                .add(new JsonObject()
                                    .put("Count", 35)
                                    .put("Metric_Type", "Total_Item_Requests")
                                )
                                .add(null)
                                .add(new JsonObject()
                                    .put("Count", 30)
                                    .put("Metric_Type", "Unique_Item_Requests")
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
                            .put("type", "ISBN")
                            .put("value", "978-3-16-148410-" + String.format("%d", cnt))
                        )
                    )
                    .put("itemPerformance", new JsonArray()
                        .add(new JsonObject()
                            .put("Period", new JsonObject()
                                .put("End_Date", "2021-01-31")
                                .put("Begin_Date", "2021-01-01")
                            )
                            .put("category", "REQUESTS")
                            .put("instance", new JsonArray()
                                .add(new JsonObject()
                                    .put("count", 135)
                                    .put("Metric_Type", "Total_Item_Requests")
                                )
                                .add(new JsonObject()
                                    .put("count", 120)
                                    .put("Metric_Type", "Unique_Item_Requests")
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
                            .put("type", "Proprietary_ID")
                            .put("value", "10.10ZZ")
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
    } else if (id.equals(otherCounterReportId)) {
        ctx.response().setChunked(true);
        ctx.response().putHeader("Content-Type", "application/json");
        ctx.response().end(getCounterReportMock(id, -1).encode());
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

  static JsonObject getKbTitle(UUID kbTitleId) {
    JsonObject res = new JsonObject();
    if (goodKbTitleId.equals(kbTitleId)) {
      res.put("name", "good kb title instance name");
      res.put("id", kbTitleId);
      res.put("identifiers", new JsonArray()
          .add(new JsonObject()
              .put("identifier", new JsonObject()
                  .put("value", goodKbTitleISSN)
              )
          ));
    } else if (otherKbTitleId.equals(kbTitleId)) {
      res.put("name", "other kb title instance name");
      res.put("id", kbTitleId);
      res.put("identifiers", new JsonArray()
          .add(new JsonObject()
              .put("identifier", new JsonObject()
                  .put("value", otherKbTitleISSN)
              )
          ));
    } else {
      res.put("name", "fake kb title instance name");
      res.put("id", kbTitleId);
      res.put("identifiers", new JsonArray()
          .add(new JsonObject()
              .put("identifier", new JsonObject()
                  .put("value", "1000-9999")
              )
          ));
    }
    return res;
  }
  static void getErmResource(RoutingContext ctx) {
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    String term = ctx.request().getParam("term");
    JsonArray ar = new JsonArray();
    UUID kbTitleId;
    switch (term) {
      case goodKbTitleISSN: kbTitleId = goodKbTitleId; break; // return a known kbTitleId for "The cats journal"
      case otherKbTitleISSN: kbTitleId = otherKbTitleId; break;
      default: kbTitleId = UUID.randomUUID();
    }
    if (!noMatchKbTitleISSN.equals(term)) { // for "The dogs journal" , no kb match
      ar.add(getKbTitle(kbTitleId));
    }
    ctx.response().end(ar.encode());
  }

  static void getErmResourceId(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));

    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().end(getKbTitle(id).encode());
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
        if (i == 0) {
          // fake package
          ar.add(new JsonObject()
              .put("id", agreementLineIds[i])
              .put("owner", new JsonObject()
                  .put("id", goodAgreementId)
                  .put("name", "Good agreement"))
              .put("resource", new JsonObject()
                  .put("class", "org.olf.kb.Pkg")
                  .put("name", "good package name")
                  .put("id", goodPackageId)
                  .put("_object", new JsonObject()
                  ))
              .put("poLines", poLinesAr)
          );
        } else {
          // fake package content item
          JsonArray coverage = new JsonArray();
          UUID kbtitleId;
          switch (i) {
            case 1:
              coverage.add(new JsonObject()
                  .put("startDate", "2020-03-09")
                  .put("endDate", "2020-04-05")
              );
              kbtitleId = goodKbTitleId;
              break;
            case 2:
              coverage.add(new JsonObject()
                  .put("startDate", "2021-03-09")
              );
              kbtitleId = otherKbTitleId;
              break;
            default:
              kbtitleId = UUID.randomUUID();
          }
          ar.add(new JsonObject()
              .put("id", agreementLineIds[i])
              .put("owner", new JsonObject()
                  .put("id", goodAgreementId)
                  .put("name", "Good agreement"))
              .put("resource", new JsonObject()
                  .put("class", "org.olf.kb.PackageContentItem")
                  .put("id", UUID.randomUUID())
                  .put("coverage", coverage)
                  .put("_object", new JsonObject()
                      .put("pti", new JsonObject()
                          .put("titleInstance", new JsonObject()
                              .put("id", kbtitleId)
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
        if (i == 0) {
          orderLine.put("fundDistribution", new JsonArray()
              .add(new JsonObject()
                  .put("fundId", goodFundId.toString())
              )
          );
        }
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
        {
          JsonObject invoice = new JsonObject()
            .put("poLineId", poLineId)
            .put("quantity", 1 + i)
            .put("subTotal", 10.0 + i * 5)
            .put("total", 12.0 + i * 6);
          if (i == 0) {
            invoice.put("subscriptionStart", "2020-01-01T00:00:00.000+00:00");
            invoice.put("subscriptionEnd", "2020-12-31T00:00:00.000+00:00");
          }
          ar.add(invoice);
        }
      }
    }
    ar.add(new JsonObject()
        .put("poLineId", poLineId)
    );
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().end(new JsonObject().put("invoiceLines", ar).encode());
  }

  static void getPackageContent(RoutingContext ctx) {
    String path = ctx.request().path();
    UUID id = UUID.fromString(path.substring(14, 50));
    if (!id.equals(goodPackageId)) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("Package not found");
      return;
    }
    JsonArray ar = new JsonArray();
    for (UUID packageTitle : packageTitles) {
      JsonObject item = new JsonObject()
          .put("id", UUID.randomUUID())
          .put("pti", new JsonObject()
              .put("titleInstance", new JsonObject()
                  .put("id", packageTitle)
              )
          );
      ar.add(item);
    }
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().setStatusCode(200);
    ctx.response().end(ar.encode());
  }

  static void getFund(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    if (id.equals(goodFundId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      JsonObject fund = new JsonObject();
      fund.put("ledgerId", goodLedgerId.toString());
      ctx.response().end(fund.encode());
    } else {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("not found");
    }
  }

  static void getLedger(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    if (id.equals(goodLedgerId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      JsonObject ledger = new JsonObject();
      ledger.put("fiscalYearOneId", goodFiscalYearId.toString());
      ctx.response().end(ledger.encode());
    } else {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("not found");
    }
  }

  static void getFiscalYear(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    if (id.equals(goodFiscalYearId)) {
      ctx.response().setChunked(true);
      ctx.response().putHeader("Content-Type", "application/json");
      JsonObject fiscalYear = new JsonObject();
      fiscalYear.put("periodStart", "2017-01-01T00:00:00Z");
      fiscalYear.put("periodEnd", "2017-12-31T23:59:59Z");
      ctx.response().end(fiscalYear.encode());
    } else {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("not found");
    }
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
    router.getWithRegex("/erm/resource/[-0-9a-z]*").handler(MainVerticleTest::getErmResourceId);
    router.getWithRegex("/erm/resource/[-0-9a-z]*/entitlementOptions").handler(MainVerticleTest::getErmResourceEntitlement);
    router.getWithRegex("/erm/sas/[-0-9a-z]*").handler(MainVerticleTest::getAgreement);
    router.getWithRegex("/erm/entitlements").handler(MainVerticleTest::getEntitlements);
    router.getWithRegex("/orders/order-lines/[-0-9a-z]*").handler(MainVerticleTest::getOrderLines);
    router.getWithRegex("/invoice-storage/invoice-lines").handler(MainVerticleTest::getInvoiceLines);
    router.getWithRegex("/erm/packages/[-0-9a-z]*/content").handler(MainVerticleTest::getPackageContent);
    router.getWithRegex("/finance-storage/funds/[-0-9a-z]*").handler(MainVerticleTest::getFund);
    router.getWithRegex("/finance-storage/ledgers/[-0-9a-z]*").handler(MainVerticleTest::getLedger);
    router.getWithRegex("/finance-storage/fiscal-years/[-0-9a-z]*").handler(MainVerticleTest::getFiscalYear);
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
          .body(containsString("testlib_mod_eusage_reports.title_entries"));
    }
  }

  @Test
  public void testGetTitlesNoTenant() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Tenant must not be null"));
  }

  @Test
  public void testPostTitlesFromCounterNoOkapiUrl() {
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
        .body(containsString("testlib_mod_eusage_reports.title_entries"));
  }

  @Test
  public void testPostTitlesBadUUID1() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject().put("titles", new JsonArray()
            .add(new JsonObject()
                .put("id", "1234")
                .put("kbTitleName", "kb title name")
                .put("kbTitleId", UUID.randomUUID().toString())
            )
        ).encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .body(is("Invalid UUID string: 1234"));
  }

  @Test
  public void testPostTitlesBadUUID2() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject().put("titles", new JsonArray()
            .add(new JsonObject()
                .put("id", UUID.randomUUID())
                .put("kbTitleName", "kb title name")
                .put("kbTitleId", "1234")
            )
        ).encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .body(is("Invalid UUID string: 1234"));
  }

  @Test
  public void testGetTitleDataNoTenant() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/title-data")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Tenant must not be null"));
  }

  @Test
  public void testGetReportDataNoTenant() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Tenant must not be null"));
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
  public void testFromCounterMissingOkapiUrl() {
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
  public void testFromAgreementNoId() {
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
    context.assertEquals(8, titlesAr.size());
    int noDefined = 0;
    int noUndefined = 0;
    int noGood = 0;
    JsonObject unmatchedTitle = null;
    for (int i = 0; i < titlesAr.size(); i++) {
      JsonObject title = titlesAr.getJsonObject(i);
      if (title.containsKey("kbTitleName")) {
        noDefined++;
        String kbTitleName = title.getString("kbTitleName");
        if ("good kb title instance name".equals(kbTitleName)) {
          noGood++;
        } else {
          context.assertEquals("fake kb title instance name", kbTitleName);
        }
      } else {
        String counterReportTitle = title.getString("counterReportTitle");
        if ("The dogs journal".equals(counterReportTitle)) {
          unmatchedTitle = title;
        } else {
          context.assertEquals("No match", counterReportTitle);
        }
        noUndefined++;
      }
    }
    context.assertEquals(1, noGood);
    context.assertEquals(6, noDefined);
    context.assertEquals(2, noUndefined);

    JsonObject postTitleObject = new JsonObject();
    postTitleObject.put("titles", new JsonArray().add(unmatchedTitle));

    // put without kbTitleId kbTitleName
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
    context.assertEquals(8, titlesAr.size());
    int noManual = 0;
    noDefined = 0;
    noUndefined = 0;
    for (int i = 0; i < titlesAr.size(); i++) {
      JsonObject title = titlesAr.getJsonObject(i);
      String counterReportTitle = title.getString("counterReportTitle");
      if ("The cats journal".equals(counterReportTitle)) {
        context.assertEquals(goodDoiValue, title.getString("DOI"));
      } else {
        context.assertFalse(title.containsKey("DOI"), title.encodePrettily());
      }
      if (title.getBoolean("kbManualMatch")) {
        context.assertFalse(title.containsKey("kbTitleId"));
        context.assertFalse(title.containsKey("kbTitleName"));
        noManual++;
      } else {
        if (title.containsKey("kbTitleName")) {
          noDefined++;
        } else {
          noUndefined++;
        }
      }
    }
    context.assertEquals(1, noManual);
    context.assertEquals(6, noDefined);
    context.assertEquals(1, noUndefined);

    // put with kbTitleId kbTitleName
    unmatchedTitle.put("kbTitleName", "correct kb title name");
    unmatchedTitle.put("kbTitleId", UUID.randomUUID().toString());
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
    context.assertEquals(8, titlesAr.size());
    noManual = 0;
    noDefined = 0;
    noUndefined = 0;
    for (int i = 0; i < titlesAr.size(); i++) {
      JsonObject title = titlesAr.getJsonObject(i);
      if (title.containsKey("kbTitleName")) {
        noDefined++;
      } else {
        noUndefined++;
      }
      if (title.getBoolean("kbManualMatch")) {
        noManual++;
      }
    }
    context.assertEquals(1, noManual);
    context.assertEquals(7, noDefined);
    context.assertEquals(1, noUndefined);

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
    JsonArray items = resObject.getJsonArray("data");
    context.assertEquals(20, items.size());
    int noWithPubDate = 0;
    for (int i = 0; i < items.size(); i++) {
      JsonObject item = items.getJsonObject(i);
      context.assertEquals(usageProviderId.toString(), item.getString("providerId"));
      String pubDate = item.getString("publicationDate");
      if (pubDate != null) {
        context.assertEquals(pubDateSample, pubDate);
        noWithPubDate++;
      }
    }
    context.assertEquals(5, noWithPubDate);

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
    context.assertEquals(9, resObject.getJsonArray("titles").size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?counterReportId=" + goodCounterReportId)
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(4, titlesAr.size());

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?counterReportId=x")
        .then().statusCode(400)
        .body(is("Invalid UUID string: x"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?providerId=" + usageProviderId)
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(9, titlesAr.size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?providerId=" + UUID.randomUUID())
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
    context.assertEquals(4, resObject.getInteger("reportLinesCreated"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", otherCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(14, resObject.getJsonArray("titles").size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    items = resObject.getJsonArray("data");
    context.assertEquals(4, items.size());
    int noPackages = 0;
    for (int i = 0; i < items.size(); i++) {
      String type =  items.getJsonObject(i).getString("type");
      if ("package".equals(type)) {
        context.assertEquals(goodPackageId.toString(), items.getJsonObject(i).getString("kbPackageId"));
        context.assertFalse(items.getJsonObject(i).containsKey("kbTitleId"));
        noPackages++;
      } else {
        context.assertEquals("serial", type);
        context.assertFalse(items.getJsonObject(i).containsKey("kbPackageId"));
        context.assertTrue(items.getJsonObject(i).containsKey("kbTitleId"));
      }
    }
    context.assertEquals(1, noPackages);

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
