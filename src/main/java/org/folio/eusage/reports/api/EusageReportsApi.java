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

  static void failHandler(RoutingContext ctx) {
    Throwable t = ctx.failure();
    // both semantic errors and syntax errors are from same pile.. Choosing 400 over 422.
    int statusCode = t.getClass().getName().startsWith("io.vertx.ext.web.validation") ? 400 : 500;
    failHandler(statusCode, ctx, t.getMessage());
  }

  static void failHandler(int statusCode, RoutingContext ctx, Throwable e) {
    log.error(e.getMessage(), e);
    failHandler(statusCode, ctx, e.getMessage());
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

  static void resultFooter(RoutingContext ctx, Integer count, String diagnostic) {
    JsonObject resultInfo = new JsonObject();
    resultInfo.put("totalRecords", count);
    JsonArray diagnostics = new JsonArray();
    if (diagnostic != null) {
      diagnostics.add(new JsonObject().put("message", diagnostic));
    }
    resultInfo.put("diagnostics", diagnostics);
    resultInfo.put("facets", new JsonArray());
    ctx.response().write("], \"resultInfo\": " + resultInfo.encode() + "}");
    ctx.response().end();
  }

  static Future<Void> streamResult(RoutingContext ctx, SqlConnection sqlConnection,
                                   String query, String cnt,
                                   String property, Function<Row, JsonObject> handler) {
    return sqlConnection.prepare(query)
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
              stream.endHandler(end -> {
                sqlConnection.query(cnt).execute()
                    .onSuccess(cntRes -> {
                      Integer count = cntRes.iterator().next().getInteger(0);
                      resultFooter(ctx, count, null);
                    })
                    .onFailure(f -> {
                      log.error(f.getMessage(), f);
                      resultFooter(ctx, 0, f.getMessage());
                    })
                    .eventually(x -> tx.commit().compose(y -> sqlConnection.close()));
              });
              stream.exceptionHandler(e -> {
                log.error("stream error {}", e.getMessage(), e);
                resultFooter(ctx, 0, e.getMessage());
                tx.commit().compose(y -> sqlConnection.close());
              });
              return Future.succeededFuture();
            })
        );
  }

  static Future<Void> streamResult(RoutingContext ctx, TenantPgPool pool,
                                   String distinct, String from,
                                   String property,
                                   Function<Row, JsonObject> handler) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    Integer offset = params.queryParameter("offset").getInteger();
    Integer limit = params.queryParameter("limit").getInteger();
    String query = "SELECT " + (distinct != null ? "DISTINCT ON (" + distinct + ")" : "")
        + " * FROM " + from + " LIMIT " + limit + " OFFSET " + offset;
    log.info("query={}", query);
    String cnt = "SELECT COUNT(" + (distinct != null ? "DISTINCT " + distinct : "*")
        + ") FROM " + from;
    log.info("cnt={}", cnt);
    return pool.getConnection()
        .compose(sqlConnection -> streamResult(ctx, sqlConnection, query, cnt, property, handler)
            .onFailure(x -> sqlConnection.close()));
  }

  Future<Void> getReportTitles(Vertx vertx, RoutingContext ctx) {
    try {
      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
      String counterReportId = stringOrNull(params.queryParameter("counterReportId"));
      String providerId = stringOrNull(params.queryParameter("providerId"));
      TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
      String distinct = titleEntriesTable(pool) + ".id";
      String from = titleEntriesTable(pool);
      if (counterReportId != null) {
        from = from + " INNER JOIN " + titleDataTable(pool)
            + " ON titleEntryId = " + titleEntriesTable(pool) + ".id"
            + " WHERE counterReportId = '" + UUID.fromString(counterReportId) + "'";
      } else if (providerId != null) {
        from = from + " INNER JOIN " + titleDataTable(pool)
            + " ON titleEntryId = " + titleEntriesTable(pool) + ".id"
            + " WHERE providerId = '" + UUID.fromString(providerId) + "'";
      }
      return streamResult(ctx, pool, distinct, from, "titles",
          row -> new JsonObject()
              .put("id", row.getUUID(0))
              .put("counterReportTitle", row.getString(1))
              .put("kbTitleName", row.getString(2))
              .put("kbTitleId", row.getUUID(3))
              .put("kbManualMatch", row.getBoolean(4))
              .put("printISSN", row.getString(5))
              .put("onlineISSN", row.getString(6))
              .put("ISBN", row.getString(7))
              .put("DOI", row.getString(8))
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
            Boolean kbManualMatch = titleEntry.getBoolean("kbManualMatch", true);
            future = future.compose(x ->
                sqlConnection.preparedQuery("UPDATE " + titleEntriesTable(pool)
                    + " SET"
                    + " kbTitleName = $2,"
                    + " kbTitleId = $3,"
                    + " kbManualMatch = $4"
                    + " WHERE id = $1")
                    .execute(Tuple.of(id, kbTitleName, kbTitleIdStr == null
                        ? null : UUID.fromString(kbTitleIdStr), kbManualMatch))
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
      String from = titleDataTable(pool);
      return streamResult(ctx, pool, null, from, "data",
          row -> {
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

  Future<Tuple> ermTitleLookup2(RoutingContext ctx, String identifier, String type) {
    if (identifier == null) {
      return Future.succeededFuture();
    }
    // some titles do not have hyphen in identifier, so try that as well
    return ermTitleLookup(ctx, identifier, type).compose(x -> x != null
        ? Future.succeededFuture(x)
        : ermTitleLookup(ctx, identifier.replace("-", ""), type)
    );
  }

  Future<Tuple> ermTitleLookup(RoutingContext ctx, String identifier, String type) {
    // assuming identifier only has unreserved characters
    // TODO .. this will match any type of identifier.
    // what if there's more than one hit?
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
                                         String onlineIssn, String isbn, String doi) {
    if (kbTitleId == null) {
      return Future.succeededFuture(null);
    }
    return con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
        + " WHERE kbTitleId = $1")
        .execute(Tuple.of(kbTitleId)).compose(res -> {
          if (!res.iterator().hasNext()) {
            return Future.succeededFuture(null);
          }
          Row row = res.iterator().next();
          UUID id = row.getUUID(0);
          return con.preparedQuery("UPDATE " + titleEntriesTable(pool)
              + " SET"
              + " counterReportTitle = $2,"
              + " printISSN = $3,"
              + " onlineISSN = $4,"
              + " ISBN = $5,"
              + " DOI = $6"
              + " WHERE id = $1")
              .execute(Tuple.of(id, counterReportTitle, printIssn, onlineIssn, isbn, doi))
              .map(id);
        });
  }

  Future<UUID> upsertTitleEntryCounterReport(TenantPgPool pool, SqlConnection con,
                                             RoutingContext ctx, String counterReportTitle,
                                             String printIssn, String onlineIssn, String isbn,
                                             String doi) {
    return con.preparedQuery("SELECT * FROM " + titleEntriesTable(pool)
        + " WHERE counterReportTitle = $1")
        .execute(Tuple.of(counterReportTitle))
        .compose(res1 -> {
          String identifier = null;
          String type = null;
          if (onlineIssn != null) {
            identifier = onlineIssn;
            type = "issn";
          } else if (printIssn != null) {
            identifier = printIssn;
            type = "issn";
          } else if (isbn != null) {
            identifier = isbn;
            type = "isbn";
          }
          if (res1.iterator().hasNext()) {
            Row row = res1.iterator().next();
            UUID id = row.getUUID(0);
            Boolean kbManualMatch = row.getBoolean(4);
            if (row.getUUID(3) != null || Boolean.TRUE.equals(kbManualMatch)) {
              return Future.succeededFuture(id);
            }
            return ermTitleLookup2(ctx, identifier, type).compose(erm -> {
              if (erm == null) {
                return Future.succeededFuture(id);
              }
              UUID kbTitleId = erm.getUUID(0);
              String kbTitleName = erm.getString(1);
              return con.preparedQuery("UPDATE " + titleEntriesTable(pool)
                  + " SET"
                  + " kbTitleName = $2,"
                  + " kbTitleId = $3"
                  + " WHERE id = $1")
                  .execute(Tuple.of(id, kbTitleName, kbTitleId)).map(id);
            });
          }
          return ermTitleLookup2(ctx, identifier, type).compose(erm -> {
            UUID kbTitleId = erm != null ? erm.getUUID(0) : null;
            String kbTitleName = erm != null ? erm.getString(1) : null;

            return updateTitleEntryByKbTitle(pool, con, kbTitleId,
                counterReportTitle, printIssn, onlineIssn, isbn, doi)
                .compose(id -> {
                  if (id != null) {
                    return Future.succeededFuture(id);
                  }
                  return con.preparedQuery(" INSERT INTO " + titleEntriesTable(pool)
                      + "(id, counterReportTitle,"
                      + " kbTitleName, kbTitleId,"
                      + " kbManualMatch, printISSN, onlineISSN, ISBN, DOI)"
                      + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
                      + " ON CONFLICT (counterReportTitle) DO NOTHING")
                      .execute(Tuple.of(UUID.randomUUID(), counterReportTitle,
                          kbTitleName, kbTitleId,
                          false, printIssn, onlineIssn, isbn, doi))
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
        if ("DOI".equals(type)) {
          ret.put("DOI", value);
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

  Future<Void> handleReport(TenantPgPool pool, SqlConnection con, RoutingContext ctx,
                            JsonObject jsonObject) {
    final UUID counterReportId = UUID.fromString(jsonObject.getString("id"));
    final UUID providerId = UUID.fromString(jsonObject.getString("providerId"));
    final JsonObject reportItem = jsonObject.getJsonObject("reportItem");
    final String counterReportTitle = reportItem.getString(altKey(reportItem,
        "itemName", "Title"));
    if (counterReportTitle == null) {
      return Future.succeededFuture();
    }
    final String usageDateRange = getUsageDate(reportItem);
    final JsonObject identifiers = getIssnIdentifiers(reportItem);
    final String onlineIssn = identifiers.getString("onlineISSN");
    final String printIssn = identifiers.getString("printISSN");
    final String isbn = identifiers.getString("ISBN");
    final String doi = identifiers.getString("DOI");
    final String publicationDate = identifiers.getString("Publication_Date");
    log.debug("handleReport title={} match={}", counterReportTitle, onlineIssn);
    final int totalAccessCount = getTotalCount(reportItem, "Total_Item_Requests");
    final int uniqueAccessCount = getTotalCount(reportItem, "Unique_Item_Requests");
    return upsertTitleEntryCounterReport(pool, con, ctx, counterReportTitle,
        printIssn, onlineIssn, isbn, doi)
        .compose(titleEntryId -> insertTdEntry(pool, con, titleEntryId, counterReportId,
            counterReportTitle, providerId, publicationDate, usageDateRange,
            uniqueAccessCount, totalAccessCount));
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

  /**
   * Populate counter reports.
   * @see <a
   * href="https://www.projectcounter.org/code-of-practice-five-sections/3-0-technical-specifications/">
   * Counter reports specification</a>
   * @param vertx Vertx. context.
   * @param ctx Routing Context
   * @return Result with True if found; False if counter report not found.
   */
  Future<Boolean> populateCounterReportTitles(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String id = ctx.getBodyAsJson().getString("counterReportId");
    final String providerId = ctx.getBodyAsJson().getString("providerId");

    if (okapiUrl == null) {
      return Future.failedFuture("Missing " + XOkapiHeaders.URL);
    }
    Promise<Void> promise = Promise.promise();
    JsonParser parser = JsonParser.newParser();
    AtomicBoolean objectMode = new AtomicBoolean(false);
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    List<Future<Void>> futures = new ArrayList<>();

    String parms = "";
    if (providerId != null) {
      parms = "?query=providerId%3D%3D" + providerId;
    }
    final String uri = "/counter-reports" + (id != null ? "/" + id : "") + parms;
    AtomicInteger pathSize = new AtomicInteger(id != null ? 1 : 0);
    JsonObject reportObj = new JsonObject();
    return pool.getConnection().compose(con -> {
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
          futures.add(handleReport(pool, con, ctx, reportObj));
        } else {
          String f = event.fieldName();
          log.debug("Field = {}", f);
          if (pathSize.get() == 2) { // if inside each top-level of each report
            if ("id".equals(f)) {
              reportObj.put("id", event.stringValue());
              futures.add(clearTdEntry(pool, con, UUID.fromString(reportObj.getString("id"))));
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
      parser.endHandler(e ->
          GenericCompositeFuture.all(futures)
              .onComplete(x -> promise.handle(x.mapEmpty()))
      );
      return getRequest(ctx, uri)
          .as(BodyCodec.jsonStream(parser))
          .send()
          .compose(res -> {
            if (res.statusCode() == 404) {
              return Future.succeededFuture(false);
            }
            if (res.statusCode() != 200) {
              return Future.failedFuture("GET " + uri + " returned status code "
                  + res.statusCode());
            }
            return promise.future().map(true);
          }).eventually(x -> con.close());
    });
  }

  Future<Void> getReportData(Vertx vertx, RoutingContext ctx) {
    try {
      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
      TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
      String from = agreementEntriesTable(pool);
      return streamResult(ctx, pool, null, from, "data", row ->
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

  /**
   * Fetch PO line by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-orders-storage/schemas/po_line.json">
   * po line schema</a>
   * @param poLineId PO line ID.
   * @param ctx Routing context.
   * @return PO line JSON object.
   */
  Future<JsonObject> lookupOrderLine(UUID poLineId, RoutingContext ctx) {
    String uri = "/orders/order-lines/" + poLineId;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Fetch invoice lines by PO line ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-invoice-storage/schemas/invoice_line.json">
   * invoice line schema</a>
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-invoice-storage/schemas/invoice_line_collection.json">
   * invoice lines response schema</a>
   * @param poLineId PO line ID.
   * @param ctx Routing context.
   * @return Invoice lines response.
   */
  Future<JsonObject> lookupInvoiceLines(UUID poLineId, RoutingContext ctx) {
    String uri = "/invoice-storage/invoice-lines?query=poLineId%3D%3D" + poLineId;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Fetch fund by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/fund.json">
   * fund schema</a>
   * @param fundId fund UUID.
   * @param ctx Routing Context.
   * @return Fund object.
   */
  Future<JsonObject> lookupFund(UUID fundId, RoutingContext ctx) {
    String uri = "/finance-storage/funds/" + fundId;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Fetch ledger by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/ledger.json">
   * ledger schema</a>
   * @param ledgerId ledger UUID.
   * @param ctx Routing Context.
   * @return Ledger object.
   */
  Future<JsonObject> lookupLedger(UUID ledgerId, RoutingContext ctx) {
    String uri = "/finance-storage/ledgers/" + ledgerId;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Fetch fiscal year by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/fiscal_year.json">
   * fiscal year schema</a>
   * @param id fiscal year UUID.
   * @param ctx Routing Context.
   * @return Fiscal year object.
   */
  Future<JsonObject> lookupFiscalYear(UUID id, RoutingContext ctx) {
    String uri = "/finance-storage/fiscal-years/" + id;
    return getRequestSend(ctx, uri)
        .map(res -> res.bodyAsJsonObject());
  }

  Future<Void> getFiscalYear(JsonObject poLine, RoutingContext ctx, JsonObject result) {
    Future<Void> future = Future.succeededFuture();
    JsonArray fundDistribution = poLine.getJsonArray("fundDistribution");
    if (fundDistribution == null) {
      return future;
    }
    for (int i = 0; i < fundDistribution.size(); i++) {
      // fundId is a required property
      UUID fundId = UUID.fromString(fundDistribution.getJsonObject(i).getString("fundId"));
      future = future.compose(x -> lookupFund(fundId, ctx).compose(fund -> {
        // fundStatus not checked. Should we ignore if Inactive?
        // ledgerId is a required property
        UUID ledgerId = UUID.fromString(fund.getString("ledgerId"));
        return lookupLedger(ledgerId, ctx).compose(ledger -> {
          // fiscalYearOneId is a required property
          UUID fiscalYearId = UUID.fromString(ledger.getString("fiscalYearOneId"));
          return lookupFiscalYear(fiscalYearId, ctx).compose(fiscalYear -> {
            // TODO: determine what happens for multiple fiscalYear
            // periodStart, periodEnd are required properties
            result.put("fiscalYear", getRange(fiscalYear, "periodStart", "periodEnd"));
            return Future.succeededFuture();
          });
        });
      }));
    }
    return future;
  }

  Future<JsonObject> parsePoLines(JsonArray poLinesAr, RoutingContext ctx) {
    List<Future<Void>> futures = new ArrayList<>();

    JsonObject result = new JsonObject();
    result.put("encumberedCost", 0.0);
    result.put("invoicedCost", 0.0);
    for (int i = 0; i < poLinesAr.size(); i++) {
      UUID poLineId = UUID.fromString(poLinesAr.getJsonObject(i).getString("poLineId"));
      futures.add(lookupOrderLine(poLineId, ctx).compose(poLine -> {
        JsonObject cost = poLine.getJsonObject("cost");
        String currency = result.getString("currency");
        String newCurrency = cost.getString("currency");
        if (currency != null && !currency.equals(newCurrency)) {
          return Future.failedFuture("Mixed currencies (" + currency + ", " + newCurrency
              + ") in order lines " + poLinesAr.encode());
        }
        result.put("currency", newCurrency);
        result.put("encumberedCost",
            result.getDouble("encumberedCost") + cost.getDouble("listUnitPriceElectronic"));
        return getFiscalYear(poLine, ctx, result);
      }));
      futures.add(lookupInvoiceLines(poLineId, ctx).compose(invoiceResponse -> {
        JsonArray invoices = invoiceResponse.getJsonArray("invoiceLines");
        for (int j = 0; j < invoices.size(); j++) {
          JsonObject invoiceLine = invoices.getJsonObject(j);
          Double thisTotal = invoiceLine.getDouble("total");
          if (thisTotal != null) {
            result.put("invoicedCost", thisTotal + result.getDouble("invoicedCost"));
          }
        }
        result.put("subscriptionDateRange", getRanges(invoices,
            "subscriptionStart", "subscriptionEnd"));
        return Future.succeededFuture();
      }));
      // https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/fiscal_year.json
      // https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/ledger.json
      // https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/fund.json
      // poline -> fund -> ledger -> fiscal_year
    }
    return GenericCompositeFuture.all(futures).map(result);
  }

  static String getRange(JsonObject o, String startProp, String endProp) {
    String start = o.getString(startProp);
    String end = o.getString(endProp);
    // only one range for now..
    if (start != null) {
      return "[" + start + "," + (end != null ? end : "today") + "]";
    }
    return null;
  }

  static String getRanges(JsonArray ar, String startProp, String endProp) {
    if (ar == null) {
      return null;
    }
    for (int i = 0; i < ar.size(); i++) {
      String range = getRange(ar.getJsonObject(i), startProp, endProp);
      if (range != null) {
        return range;
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
      return parsePoLines(poLines, ctx)
          .compose(poResult -> createTitleFromAgreement(pool, con, kbTitleId, ctx)
              .compose(x -> createPackageFromAgreement(pool, con, kbPackageId, kbPackageName, ctx))
              .compose(x -> {
                UUID id = UUID.randomUUID();
                Number encumberedCost = poResult.getDouble("encumberedCost");
                Number invoicedCost = poResult.getDouble("invoicedCost");
                String subScriptionDateRange = poResult.getString("subscriptionDateRange");
                String fiscalYearRange = poResult.getString("fiscalYear");
                return con.preparedQuery("INSERT INTO " + agreementEntriesTable(pool)
                    + "(id, kbTitleId, kbPackageId, type,"
                    + " agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,"
                    + " fiscalYearRange, subscriptionDateRange, coverageDateRanges)"
                    + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)")
                    .execute(Tuple.of(id, kbTitleId, kbPackageId, type,
                        agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,
                        fiscalYearRange, subScriptionDateRange, coverageDateRanges))
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
  public Future<Router> createRouter(Vertx vertx, WebClient webClient) {
    this.webClient = webClient;
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("getReportTitles")
              .handler(ctx -> getReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)))
              .failureHandler(EusageReportsApi::failHandler);
          routerBuilder
              .operation("postReportTitles")
              .handler(ctx -> postReportTitles(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)))
              .failureHandler(EusageReportsApi::failHandler);
          routerBuilder
              .operation("postFromCounter")
              .handler(ctx -> postFromCounter(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)))
              .failureHandler(EusageReportsApi::failHandler);
          routerBuilder
              .operation("getTitleData")
              .handler(ctx -> getTitleData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)))
              .failureHandler(EusageReportsApi::failHandler);
          routerBuilder
              .operation("getReportData")
              .handler(ctx -> getReportData(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)))
              .failureHandler(EusageReportsApi::failHandler);
          routerBuilder
              .operation("postFromAgreement")
              .handler(ctx -> postFromAgreement(vertx, ctx)
                  .onFailure(cause -> failHandler(400, ctx, cause)))
              .failureHandler(EusageReportsApi::failHandler);
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
            + "kbManualMatch boolean, "
            + "printISSN text, "
            + "onlineISSN text, "
            + "ISBN text, "
            + "DOI text"
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
}
