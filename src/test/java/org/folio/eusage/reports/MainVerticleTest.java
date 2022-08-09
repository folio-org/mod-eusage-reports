package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;

@RunWith(VertxUnitRunner.class)
public class MainVerticleTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  static Vertx vertx;
  static final int MODULE_PORT = 9230;
  static final int MOCK_PORT = 9231;
  static final String FOLIO_INVOICE = "10004";
  static final String POLINE_NUMBER_SAMPLE = "121x-219";
  static final String pubDateSample = "1998-05-01";
  static final String pubYearSample = "1999";
  static final UUID goodKbTitleId = UUID.randomUUID();
  static boolean enableGoodKbTitle;
  static final String goodKbTitleISSN = "1000-1002";
  static final String goodKbTitleISSNstrip = "10001002";
  static final String goodDoiValue = "publisherA:Code123";
  static final UUID otherKbTitleId = UUID.randomUUID();
  static final String otherKbTitleISSN = "1000-2000";
  static final String noMatchKbTitleISSN = "1001-1002";
  static final String noMatchKbTitleISSNstrip = "10011002";
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
  static final UUID [] goodInvoiceIds = {
      UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID [] goodEncumbranceIds = {
      UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID [] goodFiscalYearIds = {
      UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID[] agreementLineIds = {
      UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()
  };
  static final UUID[] poLineIds = {
      UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()
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
                    .put("YOP", pubYearSample)
                    .put("Access_Type", "OA_Gold")
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
                            .put("period", new JsonObject()
                                .put("end", "2021-01-31")
                                .put("begin", "2021-01-01")
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
                .add(new JsonObject()
                    .put("Platform", "My Platform")
                    .put("itemPerformance", new JsonArray())
                )
            )
        )
    );
    return counterReport;
  }

  static void getCounterReportsChunk(RoutingContext ctx, int offset, int limit, int max, boolean first) {
    if (offset >= max || limit <= 0) {
      ctx.response().end("], \"totalRecords\": " + max + "}");
      return;
    }
    String lead = first ? "" : ",";
    JsonObject counterReport = getCounterReportMock(UUID.randomUUID(), offset + 1);
    ctx.response().write(lead + counterReport.encode())
        .onComplete(x -> getCounterReportsChunk(ctx, offset + 1, limit - 1, max, false));
  }

  static void getCounterReports(RoutingContext ctx) {
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().write("{ \"counterReports\": [ ")
        .onComplete(x -> {
          String limit = ctx.request().getParam("limit");
          String offset = ctx.request().getParam("offset");
          int total = 5;
          String query = ctx.request().getParam("query");
          if (query != null) {
            UUID matchProviderId = UUID.fromString(query.substring(query.lastIndexOf('=') + 1));
            if (!matchProviderId.equals(usageProviderId)) {
              total = 0;
            }
          }
          getCounterReportsChunk(ctx, offset == null ? 0 : Integer.parseInt(offset),
              limit == null ? 10 : Integer.parseInt(limit), total, true);
        });
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
      res.put("publicationType", new JsonObject().put("value", "monograph"));
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
      res.put("publicationType", new JsonObject().put("value", "serial"));
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
      case goodKbTitleISSN:
      case goodKbTitleISSNstrip:
        kbTitleId = enableGoodKbTitle ? goodKbTitleId : null;
        break; // return a known kbTitleId for "The cats journal"
      case otherKbTitleISSN:
        kbTitleId = otherKbTitleId;
        break;
      case noMatchKbTitleISSN:
      case noMatchKbTitleISSNstrip:
        kbTitleId = null; // for "The dogs journal" , no kb match
        break;
      default:
        kbTitleId = UUID.randomUUID();
    }
    if (kbTitleId != null) {
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
    String page = ctx.request().getParam("page");
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
    if (agreementId.equals(goodAgreementId) && "1".equals(page)) {
      for (int i = 0; i < agreementLineIds.length; i++) {
        JsonArray poLinesAr = new JsonArray();
        for (int j = 0; j < i && j < poLineIds.length; j++) {
          poLinesAr.add(new JsonObject()
              .put("poLineId", poLineIds[j])
          );
        }
        if (i == 1) {
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
        orderLine.put("poLineNumber", POLINE_NUMBER_SAMPLE);
        String currency = i < orderLinesCurrencies.size() ? orderLinesCurrencies.get(i) : "USD";
        orderLine.put("cost", new JsonObject()
            .put("currency", currency)
        );
        if (i != 2) {
          orderLine.put("fundDistribution", new JsonArray()
              .add(new JsonObject()
                  .put("fundId", goodFundId.toString())
                  .put("encumbrance", goodEncumbranceIds[i].toString())
              )
          );
        }
        if (i == 0) {
          orderLine.put("purchaseOrderId", UUID.randomUUID().toString());
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
    String limit = ctx.request().getParam("limit");
    if (!"2147483647".equals(limit)) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(400);
      ctx.response().end("limit missing");
      return;
    }
    UUID poLineId = UUID.fromString(query.substring(10));

    JsonArray ar = new JsonArray();
    for (int i = 0; i < poLineIds.length; i++) {
      if (poLineId.equals(poLineIds[i])) {
        {
          JsonObject invoiceLine = new JsonObject()
              .put("poLineId", poLineId)
              .put("invoiceId", goodInvoiceIds[i])
              .put("quantity", 1 + i)
              .put("subTotal", 10.0 + i * 5)
              .put("total", 12.0 + i * 6)
              .put("invoiceLineNumber", String.format("%d", i));
          if (i == 0) {
            invoiceLine.put("subscriptionStart", "2020-01-01T00:00:00.000+00:00");
            invoiceLine.put("subscriptionEnd", "2020-12-31T00:00:00.000+00:00");
          }
          ar.add(invoiceLine);
        }
      }
    }
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().end(new JsonObject().put("invoiceLines", ar).encode());
  }

  static void getPackageContent(RoutingContext ctx) {
    String page = ctx.request().getParam("page");
    String path = ctx.request().path();
    UUID id = UUID.fromString(path.substring(14, 50));
    if (!id.equals(goodPackageId)) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(404);
      ctx.response().end("Package not found");
      return;
    }
    JsonArray ar = new JsonArray();
    if ("1".equals(page)) {
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
    }
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().setStatusCode(200);
    ctx.response().end(ar.encode());
  }

  static void getInvoice(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    for (int i = 0; i < goodInvoiceIds.length; i++) {
      if (id.equals(goodInvoiceIds[i])) {
        ctx.response().setChunked(true);
        ctx.response().putHeader("Content-Type", "application/json");
        JsonObject invoice = new JsonObject();
        invoice.put("id", id.toString());
        invoice.put("invoiceDate", "2017-06-18T00:00:00.000+00:00");
        invoice.put("paymentDate", "2017-06-18T13:55:04.957+00:00");
        if (i > 0) {
          invoice.put("folioInvoiceNo", FOLIO_INVOICE);
        }
        ctx.response().end(invoice.encode());
        return;
      }
    }
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().setStatusCode(404);
    ctx.response().end("not found");
  }

  static void getBudgets(RoutingContext ctx) {
    String query = ctx.request().getParam("query");
    if (query == null || !query.startsWith("fundId==")) {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().setStatusCode(400);
      ctx.response().end("query missing");
      return;
    }
    UUID fundId = UUID.fromString(query.substring(8));
    JsonArray budgets = new JsonArray();
    if (fundId.equals(goodFundId)) {
      for (UUID fiscalYearId : goodFiscalYearIds) {
        JsonObject budget = new JsonObject();
        budget.put("fiscalYearId", fiscalYearId.toString());
        budgets.add(budget);
      }
    }
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.response().end(new JsonObject().put("budgets", budgets).encode());
  }

  static void getFiscalYear(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    for (int i = 0; i < goodFiscalYearIds.length; i++) {
      if (id.equals(goodFiscalYearIds[i])) {
        ctx.response().setChunked(true);
        ctx.response().putHeader("Content-Type", "application/json");
        JsonObject fiscalYear = new JsonObject();
        int year = 2016 + i;
        fiscalYear.put("periodStart", year + "-01-01T00:00:00Z");
        fiscalYear.put("periodEnd", year + "-12-31T23:59:59Z");
        ctx.response().end(fiscalYear.encode());
        return;
      }
    }
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().setStatusCode(404);
    ctx.response().end("not found");
  }

  static void getTransaction(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));
    for (UUID id1 : goodEncumbranceIds) {
      if (id.equals(id1)) {
        ctx.response().setChunked(true);
        ctx.response().putHeader("Content-Type", "application/json");
        JsonObject transaction = new JsonObject();
        transaction.put("id", id);
        transaction.put("amount", 100.0);
        ctx.response().end(transaction.encode());
        return;
      }
    }
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().setStatusCode(404);
    ctx.response().end("not found");
  }


  static void getCompositeOrders(RoutingContext ctx) {
    String path = ctx.request().path();
    int offset = path.lastIndexOf('/');
    UUID id = UUID.fromString(path.substring(offset + 1));

    JsonObject ret = new JsonObject();
    ret.put("orderType", "One-Time");
    ctx.response().setChunked(true);
    ctx.response().putHeader("Content-Type", "application/json");
    ctx.end(ret.encode());
  }

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.baseURI = "http://localhost:" + MODULE_PORT;
    RestAssured.requestSpecification = new RequestSpecBuilder().build();

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
    router.getWithRegex("/invoice-storage/invoices/[-0-9a-z]*").handler(MainVerticleTest::getInvoice);
    router.getWithRegex("/erm/packages/[-0-9a-z]*/content").handler(MainVerticleTest::getPackageContent);
    router.getWithRegex("/finance-storage/budgets[-0-9a-z]*").handler(MainVerticleTest::getBudgets);
    router.getWithRegex("/finance-storage/fiscal-years/[-0-9a-z]*").handler(MainVerticleTest::getFiscalYear);
    router.getWithRegex("/finance-storage/transactions/[-0-9a-z]*").handler(MainVerticleTest::getTransaction);
    router.getWithRegex("/orders/composite-orders/[-0-9a-z]*").handler(MainVerticleTest::getCompositeOrders);
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
  public void testGetTitlesBadCql() {
    String tenant = "testlib";
    for (int i = 0; i < 5; i++) { // would hang wo connection close
      RestAssured.given()
          .header(XOkapiHeaders.TENANT, tenant)
          .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
          .get("/eusage-reports/report-titles?query=foo=bar")
          .then().statusCode(400)
          .header("Content-Type", is("text/plain"))
          .body(containsString("Unsupported CQL index: foo"));
    }
  }

  @Test
  public void testGetTitlesNoTenant() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("X-Okapi-Tenant header is missing"));
  }

  @Test
  public void testGetTitlesBadLimit() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?limit=-2")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("limit in location QUERY: value should be >= 0"));
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
  public void testPostTitlesBadJson() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header("Content-Type", "application/json")
        .body("{")
        .post("/eusage-reports/report-titles")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Failed to decode"));
  }

  @Test
  public void testPostTitlesBadId() {
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
        .header("Content-Type", is("text/plain"))
        .body(containsString("Validation error for body application/json"));
  }

  @Test
  public void testPostTitlesBadKbTitleId() {
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
        .header("Content-Type", is("text/plain"))
        .body(containsString("Validation error for body application/json"));
  }

  @Test
  public void testGetTitleDataNoTenant() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/title-data")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("X-Okapi-Tenant header is missing"));
  }

  @Test
  public void testGetPackagesNoInit() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-packages")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("testlib_mod_eusage_reports.package_entries"));
  }

  @Test
  public void testGetPackagesBadCql() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-packages?query=foo=bar")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Unsupported CQL index: foo"));
  }

  @Test
  public void testGetReportDataNoTenant() {
    RestAssured.given()
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("X-Okapi-Tenant header is missing"));
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

  void analyzeTitles(TestContext context, String tenant,
      int expectTotal, int expectNumber, int expectUndef, int expectManual, int expectIgnored) {
    ExtractableResponse<Response> response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?facets=status&query=cql.allRecords=1")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    JsonObject resObject = new JsonObject(response.body().asString());
    analyzeTitles(context, resObject, expectTotal, expectNumber, expectUndef, expectManual, expectIgnored);
  }

  void analyzeTitles(TestContext context, JsonObject resObject,
                     int expectTotal, int expectNumber, int expectUndef, int expectManual, int expectIgnored) {
    JsonObject resultInfo = resObject.getJsonObject("resultInfo");
    context.assertEquals(expectTotal, resultInfo.getInteger("totalRecords"));
    context.assertEquals(0, resultInfo.getJsonArray("diagnostics").size());
    JsonArray titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(expectNumber, titlesAr.size());
    int noManual = 0;
    int noIgnored = 0;
    int noUndef = 0;
    int noMatched = 0;
    for (int i = 0; i < titlesAr.size(); i++) {
      JsonObject title = titlesAr.getJsonObject(i);
      if (title.containsKey("kbTitleId")) {
        if (title.getBoolean("kbManualMatch")) {
          noManual++;
        } else {
          noMatched++;
        }
      } else {
        if (title.getBoolean("kbManualMatch")) {
          noIgnored++;
        } else {
          noUndef++;
        }
      }
      String publicationType = title.getString("publicationType");
      String kbTitleName = title.getString("kbTitleName");
      if (kbTitleName == null || kbTitleName.startsWith("correct")) {
        context.assertNull(publicationType);
      } else if (kbTitleName.startsWith("fake")) {
        context.assertEquals("serial", publicationType);
      } else {
        context.assertEquals("monograph", publicationType);
      }
    }
    context.assertEquals(expectUndef, noUndef);
    context.assertEquals(expectManual, noManual);
    context.assertEquals(expectIgnored, noIgnored);
    JsonArray facets = resultInfo.getJsonArray("facets");
    context.assertEquals(1, facets.size());
    context.assertEquals("status", facets.getJsonObject(0).getString("type"));
    JsonArray facetValues = facets.getJsonObject(0).getJsonArray("facetValues");
    context.assertEquals(3, facetValues.size());
    context.assertEquals("matched", facetValues.getJsonObject(0).getString("value"));
    context.assertEquals(noManual + noMatched, facetValues.getJsonObject(0).getInteger("count"));
    context.assertEquals("unmatched", facetValues.getJsonObject(1).getString("value"));
    context.assertEquals(noUndef, facetValues.getJsonObject(1).getInteger("count"));
    context.assertEquals("ignored", facetValues.getJsonObject(2).getString("value"));
    context.assertEquals(noIgnored, facetValues.getJsonObject(2).getInteger("count"));
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
  public void testFromCounterBadId() {
    String tenant = "testlib";

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", "1234")
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Validation error for body application/json"));
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
  public void testFromAgreementBadId() {
    String tenant = "testlib";
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject().put("agreementId", "1234").encode())
        .post("/eusage-reports/report-data/from-agreement")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Bad Request"));
  }

  @Test
  public void testPostTenantOK(TestContext context) {
    String tenant = "testlib";
    tenantOp(context, tenant, new JsonObject()
            .put("module_to", "mod-eusage-reports-1.0.0"), null);

    ExtractableResponse<Response> response;

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?facets=foo")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is("Unsupported facet: foo"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?query=cql.allRecords=1")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    JsonObject resObject = new JsonObject(response.body().asString());
    JsonObject resultInfo = resObject.getJsonObject("resultInfo");
    JsonArray facets = resultInfo.getJsonArray("facets");
    context.assertEquals(0, facets.size());

    analyzeTitles(context, tenant, 0, 0, 0, 0, 0);

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

    enableGoodKbTitle = false;
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body("{}")
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));
    analyzeTitles(context, tenant, 8, 8, 3, 0, 0);

    enableGoodKbTitle = true;
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body("{}")
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));

    analyzeTitles(context, tenant, 8, 8, 2, 0, 0);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body("{}")
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?facets=status&query=cql.allRecords=1")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    analyzeTitles(context, resObject, 8, 8, 2, 0, 0);
    JsonArray titlesAr = resObject.getJsonArray("titles");
    int noGood = 0;
    JsonObject unmatchedTitle = null;
    for (int i = 0; i < titlesAr.size(); i++) {
      JsonObject title = titlesAr.getJsonObject(i);
      if (title.containsKey("kbTitleName")) {
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
      }
    }
    context.assertEquals(1, noGood);
    // put without kbTitleId kbTitleName (so title is ignored)
    JsonObject postTitleObject = new JsonObject();
    postTitleObject.put("titles", new JsonArray().add(unmatchedTitle));
    unmatchedTitle.remove("kbManualMatch"); // default is true
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
        .get("/eusage-reports/report-titles?facets=status")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    analyzeTitles(context, resObject, 8, 8, 1, 0, 1);
    titlesAr = resObject.getJsonArray("titles");
    for (int i = 0; i < titlesAr.size(); i++) {
      JsonObject title = titlesAr.getJsonObject(i);
      String counterReportTitle = title.getString("counterReportTitle");
      if ("The cats journal".equals(counterReportTitle)) {
        context.assertEquals(goodDoiValue, title.getString("DOI"));
      } else {
        context.assertFalse(title.containsKey("DOI"), title.encodePrettily());
      }
    }

    // un-ignore the title, no longer manual match
    unmatchedTitle.put("kbManualMatch", false);
    postTitleObject.put("titles", new JsonArray().add(unmatchedTitle));
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(postTitleObject.encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(204);

    analyzeTitles(context, tenant, 8, 8, 2, 0, 0);

    // put with kbTitleId kbTitleName with manual
    unmatchedTitle.put("kbManualMatch", true);
    unmatchedTitle.put("kbTitleName", "correct kb title name");
    unmatchedTitle.put("kbTitleId", UUID.randomUUID().toString());
    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(postTitleObject.encode())
        .post("/eusage-reports/report-titles")
        .then().statusCode(204);

    analyzeTitles(context, tenant, 8, 8, 1, 1, 0);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?facets=status&query=counterReportTitle=\"cats journal\"")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    analyzeTitles(context, resObject, 1, 1, 0, 0, 0);
    context.assertEquals("The cats journal",
        resObject.getJsonArray("titles").getJsonObject(0).getString("counterReportTitle"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?facets=status&query=kbManualMatch=true")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    analyzeTitles(context, resObject, 1, 1, 0, 1, 0);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?facets=status&query=kbManualMatch=false")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    analyzeTitles(context, resObject, 7, 7, 1, 0, 0);

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
        .header("Content-Type", is("text/plain"))
        .body(containsString("Validation error for body application/json"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/title-data?limit=100")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    JsonArray items = resObject.getJsonArray("data");
    context.assertEquals(60, items.size());
    int noWithPubDate = 0;
    for (int i = 0; i < items.size(); i++) {
      JsonObject item = items.getJsonObject(i);
      context.assertEquals(usageProviderId.toString(), item.getString("providerId"));
      String pubDate = item.getString("publicationDate");
      if (pubDate != null) {
        noWithPubDate++;
        String title = item.getString("counterReportTitle");
        context.assertEquals("The dogs journal".equals(title), item.getBoolean("openAccess"));
        if ("The dogs journal".equals(title)) {
          context.assertEquals(pubYearSample + "-01-01", pubDate);
        } else {
          context.assertEquals(pubDateSample, pubDate);
        }
      }
    }
    context.assertEquals(30, noWithPubDate);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", goodCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));
    analyzeTitles(context, tenant, 9, 9, 1, 1, 0);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("providerId", usageProviderId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));
    analyzeTitles(context, tenant, 9, 9, 1, 1, 0);

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("providerId", UUID.randomUUID().toString())
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));
    analyzeTitles(context, tenant, 9, 9, 1, 1, 0);

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/title-data?limit=100&query=counterReportId==" + goodCounterReportId)
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    items = resObject.getJsonArray("data");
    context.assertEquals(4, items.size());

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
        .get("/eusage-reports/report-titles?limit=10&providerId=" + usageProviderId)
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(9, resObject.getJsonObject("resultInfo").getInteger("totalRecords"));
    context.assertEquals(9, titlesAr.size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?providerId=" + usageProviderId + "&query=kbTitleId=" + goodKbTitleId)
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(1, titlesAr.size());
    context.assertEquals(1, resObject.getJsonObject("resultInfo").getInteger("totalRecords"));
    context.assertEquals(goodKbTitleId.toString(), titlesAr.getJsonObject(0).getString("kbTitleId"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?providerId=" + usageProviderId + "&query=kbTitleId=" + goodKbTitleId
        + " sortby counterreporttitle")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(1, titlesAr.size());
    context.assertEquals(1, resObject.getJsonObject("resultInfo").getInteger("totalRecords"));
    context.assertEquals(goodKbTitleId.toString(), titlesAr.getJsonObject(0).getString("kbTitleId"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?query=kbTitleId=\"\" sortby counterreporttitle")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(8, titlesAr.size());
    for (int i = 1; i < titlesAr.size(); i++) {
      context.assertTrue(titlesAr.getJsonObject(i).getString("counterReportTitle")
          .compareTo(titlesAr.getJsonObject(i - 1).getString("counterReportTitle")) >= 0);
    }

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?query=kbTitleId=\"\" sortby id/sort.descending")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(8, titlesAr.size());
    for (int i = 1; i < titlesAr.size(); i++) {
      context.assertTrue(titlesAr.getJsonObject(i).getString("id")
          .compareTo(titlesAr.getJsonObject(i - 1).getString("id")) <= 0);
    }

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-titles?query=kbTitleId<>\"\"")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    titlesAr = resObject.getJsonArray("titles");
    context.assertEquals(1, titlesAr.size());

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
        .get("/eusage-reports/report-data/status/" + UUID.randomUUID())
        .then().statusCode(404);

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

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data/status/" + goodAgreementId)
        .then().statusCode(200)
        .body("id", is(goodAgreementId.toString()))
        .body("lastUpdated", Matchers.not(isEmptyOrNullString()))
        .body("active", is(false));

    // running the from-agreement twice (wiping out the ond one above)
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

    RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("counterReportId", otherCounterReportId)
            .encode())
        .post("/eusage-reports/report-titles/from-counter")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"));

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .header("Content-Type", "application/json")
        .get("/eusage-reports/report-titles?offset=11")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();

    resObject = new JsonObject(response.body().asString());
    context.assertEquals(6, resObject.getJsonArray("titles").size());
    context.assertEquals(17, resObject.getJsonObject("resultInfo").getInteger("totalRecords"));
    context.assertEquals(0, resObject.getJsonObject("resultInfo").getJsonArray("diagnostics").size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    items = resObject.getJsonArray("data");
    context.assertEquals(7, items.size());
    for (int i = 0; i < items.size(); i++) {
      JsonObject item = items.getJsonObject(i);
      String type =  item.getString("type");
      if (i == 0) {
        context.assertEquals("serial", type);
        context.assertFalse(item.containsKey("kbPackageId"));
        context.assertTrue(item.containsKey("kbTitleId"));
        context.assertFalse(item.containsKey("encumberedCost"));
      } else if (i == 1) {
        context.assertEquals ("package", type);
        context.assertEquals(goodPackageId.toString(), item.getString("kbPackageId"));
        context.assertFalse(item.containsKey("kbTitleId"));
        context.assertEquals(100.0, item.getDouble("encumberedCost"));
      } else if (i == 6) {
        context.assertEquals("serial", type);
        context.assertFalse(item.containsKey("kbPackageId"));
        context.assertTrue(item.containsKey("kbTitleId"));
        context.assertEquals(0.0, item.getDouble("encumberedCost"));
        context.assertFalse(item.containsKey("invoiceNumber"));
        context.assertEquals(POLINE_NUMBER_SAMPLE, item.getString("poLineNumber"));
      } else {
        context.assertEquals("serial", type);
        context.assertFalse(item.containsKey("kbPackageId"));
        context.assertTrue(item.containsKey("kbTitleId"));
        context.assertEquals(100.0, item.getDouble("encumberedCost"));
        String invoiceNumber = item.getString("invoiceNumber");
        context.assertEquals("Ongoing".equals(item.getString("orderType")), (FOLIO_INVOICE + "-1").equals(invoiceNumber));
        context.assertEquals("One-Time".equals(item.getString("orderType")), "0".equals(invoiceNumber));
        context.assertEquals(POLINE_NUMBER_SAMPLE, item.getString("poLineNumber"));
      }
    }

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-data?query=agreementLineId==" + agreementLineIds[0])
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(1, resObject.getJsonArray("data").size());

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-packages")
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(packageTitles.length, resObject.getJsonArray("packages").size());
    for (int i = 0; i < packageTitles.length; i++) {
      JsonObject kbPackage = resObject.getJsonArray("packages").getJsonObject(i);
      context.assertEquals("good package name", kbPackage.getString("kbPackageName"));
      context.assertEquals(goodPackageId.toString(), kbPackage.getString("kbPackageId"));
      context.assertEquals(packageTitles[i].toString(), kbPackage.getString("kbTitleId"));
    }

    response = RestAssured.given()
        .header(XOkapiHeaders.TENANT, tenant)
        .header(XOkapiHeaders.URL, "http://localhost:" + MOCK_PORT)
        .get("/eusage-reports/report-packages?query=kbPackageId==" + goodPackageId)
        .then().statusCode(200)
        .header("Content-Type", is("application/json"))
        .extract();
    resObject = new JsonObject(response.body().asString());
    context.assertEquals(packageTitles.length, resObject.getJsonArray("packages").size());

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
