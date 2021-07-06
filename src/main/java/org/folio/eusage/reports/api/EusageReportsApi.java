package org.folio.eusage.reports.api;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonEventType;
import io.vertx.core.parsetools.JsonParser;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.TenantPgPool;

public class EusageReportsApi implements RouterCreator, TenantInitHooks {
  private static final Logger log = LogManager.getLogger(EusageReportsApi.class);

  private WebClient webClient;

  static String titleEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".title_entries";
  }

  static String packageEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".package_entries";
  }

  static String titleDataTable(TenantPgPool pool) {
    return pool.getSchema() + ".title_data";
  }

  static String agreementEntriesTable(TenantPgPool pool) {
    return pool.getSchema() + ".agreement_entries";
  }

  static void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
    failHandler(statusCode, ctx, e != null ? e.getMessage() : null);
  }

  static void failHandler(int statusCode, RoutingContext ctx, String msg) {
    ctx.response().setStatusCode(statusCode);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end(msg != null ? msg : "Failure");
  }

  static String stringOrNull(RequestParameter requestParameter) {
    return requestParameter == null ? null : requestParameter.getString();
  }

  private static JsonObject copyWithoutNulls(JsonObject obj) {
    JsonObject n = new JsonObject();
    obj.getMap().forEach((key, value) -> {
      if (value != null) {
        n.put(key, value);
      }
    });
    return n;
  }


  private static void endHandler(RoutingContext ctx, SqlConnection sqlConnection, Transaction tx) {
    ctx.response().write("] }");
    ctx.response().end();
    tx.commit().compose(x -> sqlConnection.close());
  }

  private static void endStream(RowStream<Row> stream, RoutingContext ctx,
                         SqlConnection sqlConnection, Transaction tx) {
    stream.endHandler(end -> endHandler(ctx, sqlConnection, tx));
    stream.exceptionHandler(e -> {
      log.error("stream error {}", e.getMessage(), e);
      endHandler(ctx, sqlConnection, tx);
    });
  }

  static Future<Void> streamResult(RoutingContext ctx, TenantPgPool pool, String qry,
                                   String property, Function<Row, JsonObject> handler) {
    return pool.getConnection().compose(sqlConnection ->
        sqlConnection.prepare(qry)
            .<Void>compose(pq ->
                sqlConnection.begin().compose(tx -> {
                  ctx.response().setChunked(true);
                  ctx.response().putHeader("Content-Type", "application/json");
                  ctx.response().write("{ \"" + property + "\" : [");
                  AtomicBoolean first = new AtomicBoolean(true);
                  RowStream<Row> stream = pq.createStream(50);
                  stream.handler(row -> {
                    if (!first.getAndSet(false)) {
                      ctx.response().write(",");
                    }
                    JsonObject response = handler.apply(row);
                    ctx.response().write(copyWithoutNulls(response).encode());
                  });
                  endStream(stream, ctx, sqlConnection, tx);
                  return Future.succeededFuture();
                })
            ).onFailure(x -> sqlConnection.close())
    );
  }

  Future<Void> getReportTitles(Vertx vertx, RoutingContext ctx) {
    try {
      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
      String counterReportId = stringOrNull(params.queryParameter("counterReportId"));
      String providerId = stringOrNull(params.queryParameter("providerId"));

      TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
      String qry = "SELECT DISTINCT ON (" + titleEntriesTable(pool) + ".id) * FROM "
          + titleEntriesTable(pool);
      if (counterReportId != null) {
        qry = qry + " INNER JOIN " + titleDataTable(pool)
            + " ON titleEntryId = " + titleEntriesTable(pool) + ".id"
            + " WHERE counterReportId = '" + UUID.fromString(counterReportId) + "'";
      } else if (providerId != null) {
        qry = qry + " INNER JOIN " + titleDataTable(pool)
            + " ON titleEntryId = " + titleEntriesTable(pool) + ".id"
            + " WHERE providerId = '" + UUID.fromString(providerId) + "'";
      }
      return streamResult(ctx, pool, qry, "titles", row ->
          new JsonObject()
              .put("id", row.getUUID(0))
              .put("counterReportTitle", row.getString(1))
              .put("kbTitleName", row.getString(2))
              .put("kbTitleId", row.getUUID(3))
              .put("kbManualMatch", row.getBoolean(4))
      );
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  Future<Void> postReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection()
        .compose(sqlConnection -> {
          Future<Void> future = Future.succeededFuture();
          final JsonArray titles = ctx.getBodyAsJson().getJsonArray("titles");
          for (int i = 0; i < titles.size(); i++) {
            final JsonObject titleEntry = titles.getJsonObject(i);
            UUID id = UUID.fromString(titleEntry.getString("id"));
            String kbTitleName = titleEntry.getString("kbTitleName");
            String kbTitleIdStr = titleEntry.getString("kbTitleId");
            future = future.compose(x ->
                sqlConnection.preparedQuery("UPDATE " + titleEntriesTable(pool)
                    + " SET"
                    + " kbTitleName = $2,"
                    + " kbTitleId = $3,"
                    + " kbManualMatch = TRUE"
                    + " WHERE id = $1")
                    .execute(Tuple.of(id, kbTitleName, kbTitleIdStr == null
                        ? null : UUID.fromString(kbTitleIdStr)))
                    .compose(rowSet -> {
                      if (rowSet.rowCount() == 0) {
                        return Future.failedFuture("title " + id + " matches nothing");
                      }
                      return Future.succeededFuture();
                    }));
          }
          return future
              .eventually(x -> sqlConnection.close())
              .onFailure(x -> log.error(x.getMessage(), x));
        })
        .compose(x -> {
          ctx.response().setStatusCode(204);
          ctx.response().end();
          return Future.succeededFuture();
        });
  }

  Future<Void> getTitleData(Vertx vertx, RoutingContext ctx) {
    try {
      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
      TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
      String qry = "SELECT * FROM " + titleDataTable(pool);
      return streamResult(ctx, pool, qry, "data", row -> {
            JsonObject obj = new JsonObject()
                .put("id", row.getUUID(0))
                .put("titleEntryId", row.getUUID(1))
                .put("counterReportId", row.getUUID(2))
                .put("counterReportTitle", row.getString(3))
                .put("providerId", row.getUUID(4))
                .put("usageDateRange", row.getString(6))
                .put("uniqueAccessCount", row.getInteger(7))
                .put("totalAccessCount", row.getInteger(8))
                .put("openAccess", row.getBoolean(9));
            LocalDate publicationDate = row.getLocalDate(5);
            if (publicationDate != null) {
              obj.put("publicationDate",
                  publicationDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
            }
            return obj;
          }
      );
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  Future<Void> postFromCounter(Vertx vertx, RoutingContext ctx) {
    return populateCounterReportTitles(vertx, ctx)
        .onFailure(x -> log.error(x.getMessage(), x))
        .compose(x -> {
          if (Boolean.TRUE.equals(x)) {
            return getReportTitles(vertx, ctx);
          }
          failHandler(404, ctx, "Not Found");
          return Future.succeededFuture();
        });
  }

  static Tuple parseErmTitle(JsonObject resource) {
    UUID titleId = UUID.fromString(resource.getString("id"));
    return Tuple.of(titleId, resource.getString("name"));
  }

  Future<Tuple> ermTitleLookup(RoutingContext ctx, String identifier, String type) {
    // assuming identifier only has unreserved characters
    // TODO .. this will match any type of identifier.
    // what if there's more than one hit?
    if (identifier == null) {
      return Future.succeededFuture();
    }
    String uri = "/erm/resource?"
        + "match=identifiers.identifier.value&term=" + identifier
        + "&filters=identifiers.identifier.ns.value%3D" + type;
    return getRequestSend(ctx, uri)
        .map(res -> {
          JsonArray ar = res.bodyAsJsonArray();
          return ar.isEmpty() ? null : parseErmTitle(ar.getJsonObject(0));
        });
  }

  Future<Tuple> ermTitleLookup(RoutingContext ctx, UUID id) {
    String uri = "/erm/resource/" + id;
    return getRequestSend(ctx, uri)
        .map(res -> parseErmTitle(res.bodyAsJsonObject()));
  }

  Future<List<UUID>> ermPackageContentLookup(RoutingContext ctx, UUID id) {
    // example: /erm/packages/dfb61870-1252-4ece-8f75-db02faf4ab82/content
    String uri = "/erm/packages/" + id + "/content";
    return getRequestSend(ctx, uri)
        .map(res -> {
          JsonArray ar = res.bodyAsJsonArray();
          List<UUID> list = new ArrayList<>();
          for (int i = 0; i < ar.size(); i++) {
            UUID kbTitleId = UUID.fromString(ar.getJsonObject(i).getJsonObject("pti")
                .getJsonObject("titleInstance").getString("id"));
            list.add(kbTitleId);
          }
          return list;
        });
  }

  Future<UUID> updateTitleEntryByKbTitle(TenantPgPool pool, SqlConnection con, UUID kbTitleId,
                                         String counterReportTitle, String printIssn,
                                         String onlineIssn) {
    if (kbTitleId == null) {
      return Future.succeededFuture(null);
    }
    return con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
        + " WHERE kbTitleId = $1")
        .execute(Tuple.of(kbTitleId)).compose(res -> {
          if (res.iterator().hasNext()) {
            Row row = res.iterator().next();
            UUID id = row.getUUID(0);
            return con.preparedQuery("UPDATE " + titleEntriesTable(pool)
                + " SET"
                + " counterReportTitle = $2,"
                + " printISSN = $3,"
                + " onlineISSN = $4"
                + " WHERE id = $1")
                .execute(Tuple.of(id, counterReportTitle, printIssn, onlineIssn))
                .map(id);
          }
          return Future.succeededFuture(null);
        });
  }

  Future<UUID> upsertTitleEntryCounterReport(TenantPgPool pool, SqlConnection con,
                                             RoutingContext ctx, String counterReportTitle,
                                             String printIssn, String onlineIssn, String isbn) {
    return con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
        + " WHERE counterReportTitle = $1")
        .execute(Tuple.of(counterReportTitle))
        .compose(res1 -> {
          if (res1.iterator().hasNext()) {
            Row row = res1.iterator().next();
            return Future.succeededFuture(row.getUUID(0));
          }
          String identifier = null;
          String type = null;
          if (onlineIssn != null) {
            identifier = onlineIssn;
            type = "issn";
          } else if (printIssn != null) {
            identifier = printIssn;
            type = "issn";
          } else if (isbn != null) {
            // ERM seem to have ISBNs without hyphen
            identifier = isbn.replace("-", "");
            type = "isbn";
          }
          return ermTitleLookup(ctx, identifier, type).compose(erm -> {
            UUID kbTitleId = erm != null ? erm.getUUID(0) : null;
            String kbTitleName = erm != null ? erm.getString(1) : null;
            return updateTitleEntryByKbTitle(pool, con, kbTitleId,
                counterReportTitle, printIssn, onlineIssn)
                .compose(id -> {
                  if (id != null) {
                    return Future.succeededFuture(id);
                  }
                  return con.preparedQuery(" INSERT INTO " + titleEntriesTable(pool)
                      + "(id, counterReportTitle,"
                      + " kbTitleName, kbTitleId,"
                      + " kbManualMatch, printISSN, onlineISSN)"
                      + " VALUES ($1, $2, $3, $4, $5, $6, $7)"
                      + " ON CONFLICT (counterReportTitle) DO NOTHING")
                      .execute(Tuple.of(UUID.randomUUID(), counterReportTitle,
                          kbTitleName, kbTitleId,
                          false, printIssn, onlineIssn))
                      .compose(x ->
                          con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
                              + " WHERE counterReportTitle = $1")
                              .execute(Tuple.of(counterReportTitle))
                              .map(res2 -> res2.iterator().next().getUUID(0)));
                });
          });
        });
  }

  Future<Void> createTitleFromAgreement(TenantPgPool pool, SqlConnection con,
                                        UUID kbTitleId, RoutingContext ctx) {
    if (kbTitleId == null) {
      return Future.succeededFuture();
    }
    return con.preparedQuery("SELECT * FROM " + titleEntriesTable(pool)
        + " WHERE kbTitleId = $1")
        .execute(Tuple.of(kbTitleId))
        .compose(res -> {
          if (res.iterator().hasNext()) {
            return Future.succeededFuture();
          }
          return ermTitleLookup(ctx, kbTitleId).compose(erm -> {
            String kbTitleName = erm.getString(1);
            return con.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
                + "(id, kbTitleName, kbTitleId, kbManualMatch)"
                + " VALUES ($1, $2, $3, $4)")
                .execute(Tuple.of(UUID.randomUUID(), kbTitleName, kbTitleId, false))
                .mapEmpty();
          });
        });
  }

  Future<Void> createPackageFromAgreement(TenantPgPool pool, SqlConnection con, UUID kbPackageId,
                                          String kbPackageName, RoutingContext ctx) {
    if (kbPackageId == null) {
      return Future.succeededFuture();
    }
    return con.preparedQuery("SELECT * FROM " + packageEntriesTable(pool)
        + " WHERE kbPackageId = $1")
        .execute(Tuple.of(kbPackageId))
        .compose(res -> {
          if (res.iterator().hasNext()) {
            return Future.succeededFuture();
          }
          return ermPackageContentLookup(ctx, kbPackageId)
              .compose(list -> {
                Future<Void> future = Future.succeededFuture();
                for (UUID kbTitleId : list) {
                  future = future.compose(x -> createTitleFromAgreement(pool, con, kbTitleId, ctx));
                  future = future.compose(x ->
                    con.preparedQuery("INSERT INTO " + packageEntriesTable(pool)
                        + "(kbPackageId, kbPackageName, kbTitleId)"
                        + "VALUES ($1, $2, $3)")
                        .execute(Tuple.of(kbPackageId, kbPackageName, kbTitleId))
                        .mapEmpty()
                  );
                }
                return future;
              });
        });
  }

  static Future<Void> clearTdEntry(TenantPgPool pool, SqlConnection con, UUID counterReportId) {
    return con.preparedQuery("DELETE FROM " + titleDataTable(pool)
        + " WHERE counterReportId = $1")
        .execute(Tuple.of(counterReportId))
        .mapEmpty();
  }

  static Future<Void> clearTdEntry(TenantPgPool pool, UUID counterReportId) {
    return pool.getConnection()
        .compose(con -> clearTdEntry(pool, con, counterReportId)
            .eventually(x -> con.close()));
  }

  static Future<Void> insertTdEntry(TenantPgPool pool, SqlConnection con, UUID titleEntryId,
                                    UUID counterReportId, String counterReportTitle,
                                    UUID providerId, String publicationDate,
                                    String usageDateRange,
                                    int uniqueAccessCount, int totalAccessCount) {
    LocalDate localDate = publicationDate != null ? LocalDate.parse(publicationDate) : null;
    return con.preparedQuery("INSERT INTO " + titleDataTable(pool)
        + "(id, titleEntryId,"
        + " counterReportId, counterReportTitle, providerId,"
        + " publicationDate, usageDateRange,"
        + " uniqueAccessCount, totalAccessCount, openAccess)"
        + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")
        .execute(Tuple.of(UUID.randomUUID(), titleEntryId,
            counterReportId, counterReportTitle, providerId,
            localDate, usageDateRange,
            uniqueAccessCount, totalAccessCount, false))
        .mapEmpty();
  }

  static int getTotalCount(JsonObject reportItem, String type) {
    int count = 0;
    JsonArray itemPerformances = reportItem.getJsonArray(altKey(reportItem,
        "itemPerformance", "Performance"));
    for (int i = 0; i < itemPerformances.size(); i++) {
      JsonObject itemPerformance = itemPerformances.getJsonObject(i);
      if (itemPerformance != null) {
        JsonArray instances = itemPerformance.getJsonArray(altKey(itemPerformance,
            "instance", "Instance"));
        for (int j = 0; j < instances.size(); j++) {
          JsonObject instance = instances.getJsonObject(j);
          if (instance != null) {
            String foundType = instance.getString("Metric_Type");
            if (type.equals(foundType)) {
              count += instance.getInteger(altKey(instance, "count", "Count"));
            }
          }
        }
      }
    }
    return count;
  }

  static String getUsageDate(JsonObject reportItem) {
    JsonArray itemPerformances = reportItem.getJsonArray(altKey(reportItem,
        "itemPerformance", "Performance"));
    for (int i = 0; i < itemPerformances.size(); i++) {
      JsonObject itemPerformance = itemPerformances.getJsonObject(i);
      if (itemPerformance != null) {
        JsonObject period = itemPerformance.getJsonObject("Period");
        return "[" + period.getString("Begin_Date") + "," + period.getString("End_Date") + "]";
      }
    }
    return null;
  }


  static JsonObject getIssnIdentifiers(JsonObject reportItem) {
    JsonArray itemIdentifiers = reportItem.getJsonArray(altKey(reportItem,
        "itemIdentifier", "Item_ID"));
    JsonObject ret = new JsonObject();
    for (int k = 0; k < itemIdentifiers.size(); k++) {
      JsonObject itemIdentifier = itemIdentifiers.getJsonObject(k);
      if (itemIdentifier != null) {
        String type = itemIdentifier.getString(altKey(itemIdentifier, "type", "Type"));
        String value = itemIdentifier.getString(altKey(itemIdentifier, "value", "Value"));
        if ("ONLINE_ISSN".equals(type) || "Online_ISSN".equals(type)) {
          ret.put("onlineISSN", value);
        }
        if ("PRINT_ISSN".equals(type) || "Print_ISSN".equals(type)) {
          ret.put("printISSN", value);
        }
        if ("ISBN".equals(type)) {
          ret.put("ISBN", value);
        }
        if ("Publication_Date".equals(type)) {
          ret.put(type, value);
        }
      }
    }
    return ret;
  }

  static String altKey(JsonObject jsonObject, String... keys) {
    for (String key : keys) {
      if (jsonObject.containsKey(key)) {
        return key;
      }
    }
    return keys[0];
  }

  Future<Void> handleReport(TenantPgPool pool, RoutingContext ctx, JsonObject jsonObject) {
    final UUID counterReportId = UUID.fromString(jsonObject.getString("id"));
    final UUID providerId = UUID.fromString(jsonObject.getString("providerId"));
    final JsonObject reportItem = jsonObject.getJsonObject("reportItem");
    final String usageDateRange = getUsageDate(reportItem);
    final JsonObject identifiers = getIssnIdentifiers(reportItem);
    final String onlineIssn = identifiers.getString("onlineISSN");
    final String printIssn = identifiers.getString("printISSN");
    final String isbn = identifiers.getString("ISBN");
    final String publicationDate = identifiers.getString("Publication_Date");
    final String counterReportTitle = reportItem.getString(altKey(reportItem,
        "itemName", "Title"));
    log.debug("handleReport title={} match={}", counterReportTitle, onlineIssn);
    return pool.getConnection().compose(con -> con.begin().compose(tx -> {
      Future<Void> future = Future.succeededFuture();
      final int totalAccessCount = getTotalCount(reportItem, "Total_Item_Requests");
      final int uniqueAccessCount = getTotalCount(reportItem, "Unique_Item_Requests");
      future = future.compose(x ->
          upsertTitleEntryCounterReport(pool, con, ctx, counterReportTitle,
              printIssn, onlineIssn, isbn)
              .compose(titleEntryId -> insertTdEntry(pool, con, titleEntryId, counterReportId,
                  counterReportTitle,  providerId, publicationDate, usageDateRange,
                  uniqueAccessCount, totalAccessCount))
      );
      return future.compose(x -> tx.commit());
    }).eventually(x -> con.close()));
  }

  HttpRequest<Buffer> getRequest(RoutingContext ctx, String uri) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String token = stringOrNull(params.headerParameter(XOkapiHeaders.TOKEN));
    log.info("GET {} request", uri);
    return webClient.request(HttpMethod.GET, new RequestOptions().setAbsoluteURI(okapiUrl + uri))
        .putHeader(XOkapiHeaders.TOKEN, token)
        .putHeader(XOkapiHeaders.TENANT, tenant);
  }

  Future<HttpResponse<Buffer>> getRequestSend(RoutingContext ctx, String uri, int other) {
    return getRequest(ctx, uri).send().map(res -> {
      if (res.statusCode() != 200 && res.statusCode() != other) {
        throw new RuntimeException("GET " + uri + " returned status code " + res.statusCode());
      }
      return res;
    });
  }

  Future<HttpResponse<Buffer>> getRequestSend(RoutingContext ctx, String uri) {
    return getRequestSend(ctx, uri, -1);
  }

  Future<Boolean> populateCounterReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String id = ctx.getBodyAsJson().getString("counterReportId");

    if (okapiUrl == null) {
      return Future.failedFuture("Missing " + XOkapiHeaders.URL);
    }
    Promise<Void> promise = Promise.promise();
    JsonParser parser = JsonParser.newParser();
    AtomicBoolean objectMode = new AtomicBoolean(false);
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    List<Future<Void>> futures = new ArrayList<>();

    final String uri = "/counter-reports" + (id != null ? "/" + id : "");
    AtomicInteger pathSize = new AtomicInteger(id != null ? 1 : 0);
    JsonObject reportObj = new JsonObject();
    parser.handler(event -> {
      log.debug("event type={}", event.type().name());
      JsonEventType type = event.type();
      if (JsonEventType.END_OBJECT.equals(type)) {
        pathSize.decrementAndGet();
        objectMode.set(false);
        parser.objectEventMode();
      }
      if (JsonEventType.START_OBJECT.equals(type)) {
        pathSize.incrementAndGet();
      }
      if (objectMode.get() && event.isObject()) {
        reportObj.put("reportItem", event.objectValue());
        log.debug("Object value {}", reportObj.encodePrettily());
        futures.add(handleReport(pool, ctx, reportObj));
      } else {
        String f = event.fieldName();
        log.debug("Field = {}", f);
        if (pathSize.get() == 2) { // if inside each top-level of each report
          if ("id".equals(f)) {
            reportObj.put("id", event.stringValue());
            futures.add(clearTdEntry(pool, UUID.fromString(reportObj.getString("id"))));
          }
          if ("providerId".equals(f)) {
            reportObj.put(f, event.stringValue());
          }
        }
        if ("reportItems".equals(f) || "Report_Items".equals(f)) {
          objectMode.set(true);
          parser.objectValueMode();
        }
      }
    });
    parser.exceptionHandler(x -> {
      log.error("GET {} returned bad JSON: {}", uri, x.getMessage(), x);
      promise.tryFail("GET " + uri + " returned bad JSON: " + x.getMessage());
    });
    parser.endHandler(e -> {
      GenericCompositeFuture.all(futures)
          .onComplete(x -> promise.handle(x.mapEmpty()));
    });
    return getRequest(ctx, uri)
        .as(BodyCodec.jsonStream(parser))
        .send()
        .compose(res -> {
          if (res.statusCode() == 404) {
            return Future.succeededFuture(false);
          }
          if (res.statusCode() != 200) {
            return Future.failedFuture("GET " + uri + " returned status code " + res.statusCode());
          }
          return promise.future().map(true);
        });
  }

  Future<Void> getReportData(Vertx vertx, RoutingContext ctx) {
    try {
      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));

      TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
      String qry = "SELECT * FROM " + agreementEntriesTable(pool);
      return streamResult(ctx, pool, qry, "data", row ->
          new JsonObject()
              .put("id", row.getUUID(0))
              .put("kbTitleId", row.getUUID(1))
              .put("kbPackageId", row.getUUID(2))
              .put("type", row.getString(3))
              .put("agreementId", row.getUUID(4))
              .put("agreementLineId", row.getUUID(5))
              .put("poLineId", row.getUUID(6))
              .put("encumberedCost", row.getNumeric(7))
              .put("invoicedCost", row.getNumeric(8))
              .put("fiscalYearRange", row.getString(9))
              .put("subscriptionDateRange", row.getString(10))
              .put("coverageDateRanges", row.getString(11))
      );
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return Future.failedFuture(e);
    }
  }

  Future<Boolean> agreementExists(RoutingContext ctx, UUID agreementId) {
    final String uri = "/erm/sas/" + agreementId;
    return getRequestSend(ctx, uri, 404)
        .map(res -> res.statusCode() != 404);
  }

  Future<JsonObject> lookupOrderLine(UUID poLineId, RoutingContext ctx) {
    String uri = "/orders/order-lines/" + poLineId;
    return getRequestSend(ctx, uri)
        .map(res -> res.bodyAsJsonObject());
  }

  Future<JsonObject> lookupInvoiceLine(UUID poLineId, RoutingContext ctx) {
    String uri = "/invoice-storage/invoice-lines?query=poLineId%3D%3D" + poLineId;
    return getRequestSend(ctx, uri)
        .map(res -> res.bodyAsJsonObject());
  }

  Future<JsonObject> lookupPoLine(JsonArray poLinesAr, RoutingContext ctx) {
    List<Future<Void>> futures = new ArrayList<>();

    JsonObject totalCost = new JsonObject();
    totalCost.put("encumberedCost", 0.0);
    totalCost.put("invoicedCost", 0.0);
    for (int i = 0; i < poLinesAr.size(); i++) {
      UUID poLineId = UUID.fromString(poLinesAr.getJsonObject(i).getString("poLineId"));
      futures.add(lookupOrderLine(poLineId, ctx).compose(poLine -> {
        JsonObject cost = poLine.getJsonObject("cost");
        String currency = totalCost.getString("currency");
        String newCurrency = cost.getString("currency");
        if (currency != null && !currency.equals(newCurrency)) {
          return Future.failedFuture("Mixed currencies (" + currency + ", " + newCurrency
              + ") in order lines " + poLinesAr.encode());
        }
        totalCost.put("currency", newCurrency);
        totalCost.put("encumberedCost",
            totalCost.getDouble("encumberedCost") + cost.getDouble("listUnitPriceElectronic"));
        return Future.succeededFuture();
      }));
      futures.add(lookupInvoiceLine(poLineId, ctx).compose(invoiceResponse -> {
        JsonArray invoices = invoiceResponse.getJsonArray("invoiceLines");
        for (int j = 0; j < invoices.size(); j++) {
          JsonObject invoiceLine = invoices.getJsonObject(j);
          Double thisTotal = invoiceLine.getDouble("total");
          if (thisTotal != null) {
            totalCost.put("invoicedCost", thisTotal + totalCost.getDouble("invoicedCost"));
          }
        }
        totalCost.put("subscriptionDateRange", getRanges(invoices,
            "subscriptionStart", "subscriptionEnd"));
        return Future.succeededFuture();
      }));
    }
    return GenericCompositeFuture.all(futures).map(totalCost);
  }

  static String getRanges(JsonArray ar, String startProp, String endProp) {
    if (ar == null) {
      return null;
    }
    for (int i = 0; i < ar.size(); i++) {
      JsonObject o = ar.getJsonObject(i);
      String start = o.getString(startProp);
      String end = o.getString(endProp);
      // only one range for now..
      if (start != null) {
        return "[" + start + "," + (end != null ? end : "today") + "]";
      }
    }
    return null;
  }

  Future<Void> populateAgreementLine(TenantPgPool pool, SqlConnection con,
                                     JsonObject agreementLine, UUID agreementId,
                                     RoutingContext ctx) {
    try {
      JsonArray poLines = agreementLine.getJsonArray("poLines");
      UUID poLineId = poLines.isEmpty()
          ? null : UUID.fromString(poLines.getJsonObject(0).getString("poLineId"));
      final Future<JsonObject> future = lookupPoLine(poLines, ctx);
      final UUID agreementLineId = UUID.fromString(agreementLine.getString("id"));
      JsonObject resourceObject = agreementLine.getJsonObject("resource");
      JsonObject underScoreObject = resourceObject.getJsonObject("_object");
      JsonArray coverage = resourceObject.getJsonArray("coverage");
      String coverageDateRanges = getRanges(coverage, "startDate", "endDate");
      final String resourceClass = resourceObject.getString("class");
      JsonObject titleInstance = null;
      if (!resourceClass.equals("org.olf.kb.Pkg")) {
        JsonObject pti = underScoreObject.getJsonObject("pti");
        titleInstance = pti.getJsonObject("titleInstance");
      }
      String type = titleInstance != null
          ? titleInstance.getJsonObject("publicationType").getString("value") : "package";
      UUID kbTitleId = titleInstance != null
          ? UUID.fromString(titleInstance.getString("id")) : null;
      UUID kbPackageId = titleInstance == null
          ? UUID.fromString(resourceObject.getString("id")) : null;
      String kbPackageName = titleInstance == null
          ? resourceObject.getString("name") : null;
      return future.compose(cost -> createTitleFromAgreement(pool, con, kbTitleId, ctx)
              .compose(x -> createPackageFromAgreement(pool, con, kbPackageId, kbPackageName, ctx))
              .compose(x -> {
                UUID id = UUID.randomUUID();
                Number encumberedCost = cost.getDouble("encumberedCost");
                Number invoicedCost = cost.getDouble("invoicedCost");
                String subScriptionDateRange = cost.getString("subscriptionDateRange");
                return con.preparedQuery("INSERT INTO " + agreementEntriesTable(pool)
                    + "(id, kbTitleId, kbPackageId, type,"
                    + " agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,"
                    + " subscriptionDateRange, coverageDateRanges)"
                    + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)")
                    .execute(Tuple.of(id, kbTitleId, kbPackageId, type,
                        agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,
                        subScriptionDateRange, coverageDateRanges))
                    .mapEmpty();
              })
      );
    } catch (Exception e) {
      log.error("Failed to decode agreementLine: {}", e.getMessage(), e);
      return Future.failedFuture("Failed to decode agreement line: " + e.getMessage());
    }
  }

  Future<Integer> populateAgreement(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String agreementIdStr = ctx.getBodyAsJson().getString("agreementId");
    if (agreementIdStr == null) {
      return Future.failedFuture("Missing agreementId property");
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.getConnection().compose(con -> con.begin().compose(tx -> {
      final UUID agreementId = UUID.fromString(agreementIdStr);
      return agreementExists(ctx, agreementId)
          .compose(exists -> {
            if (!exists) {
              return Future.succeededFuture(null);
            }
            // expand agreement to get agreement lines, now that we know the agreement ID is good.
            // the call below returns 500 with a stacktrace if agreement ID is no good.
            // example: /erm/entitlements?filters=owner%3D3b6623de-de39-4b43-abbc-998bed892025
            String uri = "/erm/entitlements?filters=owner%3D" + agreementId;
            return getRequestSend(ctx, uri)
                .compose(res -> {
                  Future<Void> future = Future.succeededFuture();
                  JsonArray items = res.bodyAsJsonArray();
                  for (int i = 0; i < items.size(); i++) {
                    JsonObject agreementLine = items.getJsonObject(i);
                    future = future.compose(v ->
                        populateAgreementLine(pool, con, agreementLine, agreementId, ctx));
                  }
                  return future.compose(x -> tx.commit()).map(items.size());
                });
          });
    }).eventually(x -> con.close()));
  }

  Future<Integer> postFromAgreement(Vertx vertx, RoutingContext ctx) {
    return populateAgreement(vertx, ctx)
        .compose(linesCreated -> {
          if (linesCreated == null) {
            failHandler(404, ctx, "Not found");
            return Future.succeededFuture();
          }
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", "application/json");
          ctx.response().end(new JsonObject().put("reportLinesCreated", linesCreated).encode());
          return Future.succeededFuture();
        });
  }

  @Override
  public Future<Router> createRouter(Vertx vertx) {
    webClient = WebClient.create(vertx);
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("getReportTitles")
              .handler(ctx -> getReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("postReportTitles")
              .handler(ctx -> postReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("postFromCounter")
              .handler(ctx -> postFromCounter(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("getTitleData")
              .handler(ctx -> getTitleData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("getReportData")
              .handler(ctx -> getReportData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          routerBuilder
              .operation("postFromAgreement")
              .handler(ctx -> postFromAgreement(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)));
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    if (!tenantAttributes.containsKey("module_to")) {
      return Future.succeededFuture(); // doing nothing for disable
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    Future<Void> future = pool
        .query("CREATE TABLE IF NOT EXISTS " + titleEntriesTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "counterReportTitle text UNIQUE, "
            + "kbTitleName text, "
            + "kbTitleId UUID, "
            + "kbManualMatch boolean,"
            + "printISSN text,"
            + "onlineISSN text"
            + ")")
        .execute().mapEmpty();
    future = future.compose(x -> pool
        .query("CREATE TABLE IF NOT EXISTS " + packageEntriesTable(pool) + " ( "
            + "kbPackageId UUID, "
            + "kbPackageName text, "
            + "kbTitleId UUID "
            + ")")
        .execute().mapEmpty());
    future = future.compose(x -> pool
        .query("CREATE TABLE IF NOT EXISTS " + titleDataTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "titleEntryId UUID, "
            + "counterReportId UUID, "
            + "counterReportTitle text, "
            + "providerId UUID, "
            + "publicationDate date, "
            + "usageDateRange daterange,"
            + "uniqueAccessCount integer, "
            + "totalAccessCount integer, "
            + "openAccess boolean"
            + ")")
        .execute().mapEmpty());
    future = future.compose(x -> pool
        .query("CREATE TABLE IF NOT EXISTS " + agreementEntriesTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "kbTitleId UUID, "
            + "kbPackageId UUID, "
            + "type text, "
            + "agreementId UUID, "
            + "agreementLineId UUID, "
            + "poLineId UUID, "
            + "encumberedCost numeric(20, 8), "
            + "invoicedCost numeric(20, 8), "
            + "fiscalYearRange daterange, "
            + "subscriptionDateRange daterange, "
            + "coverageDateRanges daterange"
            + ")")
        .execute().mapEmpty());
    return future;
  }

  @Override
  public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    return Future.succeededFuture();
  }
}
