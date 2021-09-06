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
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.TenantInitHooks;
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.util.LongAdder;
import org.folio.tlib.util.ResourceUtil;
import org.folio.tlib.util.TenantUtil;

public class EusageReportsApi implements RouterCreator, TenantInitHooks {
  private static final Logger log = LogManager.getLogger(EusageReportsApi.class);

  private WebClient webClient;

  static DecimalFormat costDecimalFormat = new DecimalFormat("#.00");

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
        .compose(pq ->
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
              stream.endHandler(end -> sqlConnection.query(cnt).execute()
                  .onSuccess(cntRes -> {
                    Integer count = cntRes.iterator().next().getInteger(0);
                    resultFooter(ctx, count, null);
                  })
                  .onFailure(f -> {
                    log.error(f.getMessage(), f);
                    resultFooter(ctx, 0, f.getMessage());
                  })
                  .eventually(x -> tx.commit().compose(y -> sqlConnection.close())));
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
      PgCqlQuery pgCqlQuery = PgCqlQuery.query();
      pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
      pgCqlQuery.addField(new PgCqlField("id", PgCqlField.Type.UUID));
      pgCqlQuery.addField(new PgCqlField("counterReportTitle", PgCqlField.Type.FULLTEXT));
      pgCqlQuery.addField(new PgCqlField("ISBN", PgCqlField.Type.TEXT));
      pgCqlQuery.addField(new PgCqlField("printISSN", PgCqlField.Type.TEXT));
      pgCqlQuery.addField(new PgCqlField("onlineISSN", PgCqlField.Type.TEXT));
      pgCqlQuery.addField(new PgCqlField("kbTitleId", PgCqlField.Type.UUID));
      pgCqlQuery.addField(new PgCqlField("kbManualMatch", PgCqlField.Type.BOOLEAN));

      RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      final String tenant = stringOrNull(params.headerParameter(XOkapiHeaders.TENANT));
      final String counterReportId = stringOrNull(params.queryParameter("counterReportId"));
      final String providerId = stringOrNull(params.queryParameter("providerId"));
      final TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
      final String distinct = titleEntriesTable(pool) + ".id";

      pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));
      String cqlWhere = pgCqlQuery.getWhereClause();

      String from = titleEntriesTable(pool);
      if (counterReportId != null) {
        from = from + " INNER JOIN " + titleDataTable(pool)
            + " ON titleEntryId = " + titleEntriesTable(pool) + ".id"
            + " WHERE counterReportId = '" + UUID.fromString(counterReportId) + "'";
        if (cqlWhere != null) {
          from = from + " AND " + cqlWhere;
        }
      } else if (providerId != null) {
        from = from + " INNER JOIN " + titleDataTable(pool)
            + " ON titleEntryId = " + titleEntriesTable(pool) + ".id"
            + " WHERE providerId = '" + UUID.fromString(providerId) + "'";
        if (cqlWhere != null) {
          from = from + " AND " + cqlWhere;
        }
      } else {
        if (cqlWhere != null) {
          from = from + " WHERE " + cqlWhere;
        }
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
                                    UUID providerId, LocalDate publicationDate,
                                    String usageDateRange,
                                    int uniqueAccessCount, int totalAccessCount) {
    return con.preparedQuery("INSERT INTO " + titleDataTable(pool)
        + "(id, titleEntryId,"
        + " counterReportId, counterReportTitle, providerId,"
        + " publicationDate, usageDateRange,"
        + " uniqueAccessCount, totalAccessCount, openAccess)"
        + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")
        .execute(Tuple.of(UUID.randomUUID(), titleEntryId,
            counterReportId, counterReportTitle, providerId,
            publicationDate, usageDateRange,
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
    String yop = reportItem.getString("YOP");
    final String usageDateRange = getUsageDate(reportItem);
    final JsonObject identifiers = getIssnIdentifiers(reportItem);
    final String onlineIssn = identifiers.getString("onlineISSN");
    final String printIssn = identifiers.getString("printISSN");
    final String isbn = identifiers.getString("ISBN");
    final String doi = identifiers.getString("DOI");
    String publicationDate = identifiers.getString("Publication_Date");

    DateTimeFormatter format = new DateTimeFormatterBuilder()
        .appendPattern("yyyy")
        .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
        .toFormatter();

    final LocalDate pubdate =
        publicationDate != null ? LocalDate.parse(publicationDate) :
            yop != null ? LocalDate.parse(yop, format) :
                null;

    log.debug("handleReport title={} match={}", counterReportTitle, onlineIssn);
    final int totalAccessCount = getTotalCount(reportItem, "Total_Item_Requests");
    final int uniqueAccessCount = getTotalCount(reportItem, "Unique_Item_Requests");
    return upsertTitleEntryCounterReport(pool, con, ctx, counterReportTitle,
        printIssn, onlineIssn, isbn, doi)
        .compose(titleEntryId -> insertTdEntry(pool, con, titleEntryId, counterReportId,
            counterReportTitle, providerId, pubdate, usageDateRange,
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
              .put("orderType", row.getString(12))
              .put("invoiceNumber", row.getString(13))
              .put("poLineNumber", row.getString(14))
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
   * Fetch purchase order by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-orders-storage/schemas/purchase_order.json">
   * purchase order schema</a>
   * @param id purchase order ID.
   * @param ctx routing context.
   * @return purchase order object.
   */
  Future<JsonObject> lookupPurchaseOrderLine(UUID id, RoutingContext ctx) {
    String uri = "/orders/composite-orders/" + id;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Get orderType from purchase order.
   * @param poLine po line object.
   * @param ctx routing context.
   * @param result JSON object with "orderType" is being set.
   * @return future result.
   */
  Future<Void> getOrderType(JsonObject poLine, RoutingContext ctx, JsonObject result) {
    String purchaseOrderId = poLine.getString("purchaseOrderId");
    if (purchaseOrderId == null) {
      result.put("orderType", "Ongoing");
      return Future.succeededFuture();
    }
    return lookupPurchaseOrderLine(UUID.fromString(purchaseOrderId), ctx)
        .onSuccess(purchase ->
          result.put("orderType", purchase.getString("orderType", "Ongoing")))
        .mapEmpty();
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
        .map(HttpResponse::bodyAsJsonObject);
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
        result.put("poLineNumber", poLine.getString("poLineNumber"));
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
        return getFiscalYear(poLine, ctx, result)
            .compose(x -> getOrderType(poLine, ctx, result));
      }));
      futures.add(lookupInvoiceLines(poLineId, ctx).compose(invoiceResponse -> {
        JsonArray invoices = invoiceResponse.getJsonArray("invoiceLines");
        for (int j = 0; j < invoices.size(); j++) {
          JsonObject invoiceLine = invoices.getJsonObject(j);
          String invoiceNumber = invoiceLine.getString("invoiceLineNumber");
          if (invoiceNumber != null) {
            result.put("invoiceNumber", invoiceNumber);
          }
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
                String orderType = poResult.getString("orderType");
                String invoiceNumber = poResult.getString("invoiceNumber");
                Number encumberedCost = poResult.getDouble("encumberedCost");
                Number invoicedCost = poResult.getDouble("invoicedCost");
                String subScriptionDateRange = poResult.getString("subscriptionDateRange");
                String fiscalYearRange = poResult.getString("fiscalYear");
                String poLineNumber = poResult.getString("poLineNumber");
                return con.preparedQuery("INSERT INTO " + agreementEntriesTable(pool)
                    + "(id, kbTitleId, kbPackageId, type,"
                    + " agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,"
                    + " fiscalYearRange, subscriptionDateRange, coverageDateRanges, orderType,"
                    + " invoiceNumber,poLineNumber)"
                    + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)")
                    .execute(Tuple.of(id, kbTitleId, kbPackageId, type,
                        agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,
                        fiscalYearRange, subScriptionDateRange, coverageDateRanges, orderType,
                        invoiceNumber, poLineNumber))
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

  Future<Void> postFromAgreement(Vertx vertx, RoutingContext ctx) {
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

  static void getUseTotalsCsv(JsonObject json, boolean groupByPublicationYear,
                              boolean periodOfUse, CSVPrinter writer,
                              String lead) throws IOException {
    writer.print("Totals - " + lead + " item requests");
    writer.print(null);
    writer.print(null);
    if (periodOfUse) {
      writer.print(null);
    }
    if (groupByPublicationYear) {
      writer.print(null);
    }
    writer.print(null);
    writer.print(null);
    writer.print(json.getLong(lead + "ItemRequestsTotal"));
    Long[] totalItemRequestsPeriod = (Long[]) json.getValue(lead + "ItemRequestsByPeriod");
    for (Long requestsPeriod : totalItemRequestsPeriod) {
      writer.print(requestsPeriod);
    }
    writer.println();
  }

  static void getUseOverTime2Csv(JsonObject json, boolean groupByPublicationYear,
                                 boolean periodOfUse, Appendable appendable)
      throws IOException {
    CSVPrinter writer = new CSVPrinter(appendable, CSVFormat.EXCEL);
    writer.print("Title");
    writer.print("Print ISSN");
    writer.print("Online ISSN");
    if (periodOfUse) {
      writer.print("Period of use");
    }
    if (groupByPublicationYear) {
      writer.print("Year of publication");
    }
    writer.print("Access type");
    writer.print("Metric Type");
    writer.print("Reporing period total");
    JsonArray accessCountPeriods = json.getJsonArray("accessCountPeriods");
    for (int i = 0; i < accessCountPeriods.size(); i++) {
      // TODO .. If year  prefix with "Published "
      // MMMM-DD should be converted to "MON-YYYY"
      writer.print(accessCountPeriods.getString(i));
    }
    writer.println();

    getUseTotalsCsv(json, groupByPublicationYear, periodOfUse, writer, "total");
    getUseTotalsCsv(json, groupByPublicationYear, periodOfUse, writer, "unique");

    JsonArray items = json.getJsonArray("items");
    for (int j = 0; j < items.size(); j++) {
      JsonObject item = items.getJsonObject(j);
      writer.print(item.getString("title"));
      writer.print(item.getString("printISSN"));
      writer.print(item.getString("onlineISSN"));
      if (groupByPublicationYear) {
        writer.print(item.getLong("publicationYear"));
      }
      if (periodOfUse) {
        writer.print(item.getString("periodOfUse"));
      }
      writer.print(item.getString("accessType"));
      writer.print(item.getString("metricType"));
      writer.print(item.getLong("accessCountTotal"));
      JsonArray accessCountsByPeriod = item.getJsonArray("accessCountsByPeriod");
      for (int i = 0; i < accessCountsByPeriod.size(); i++) {
        writer.print(accessCountsByPeriod.getLong(i));
      }
      writer.println();
    }
  }

  Future<Void> getUseOverTime(Vertx vertx, RoutingContext ctx) {
    String format = ctx.request().params().get("format");
    boolean isJournal;

    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    switch (format) {
      case "JOURNAL":
        isJournal = true;
        break;
      case "BOOK":
        isJournal = false;
        break;
      default:
        throw new IllegalArgumentException("format = " + format);
    }

    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));

    return getUseOverTime(pool, isJournal, includeOA, false, agreementId,
        accessCountPeriod, start, end, csv)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<String> getUseOverTime(TenantPgPool pool, boolean isJournal, boolean includeOA,
                                boolean groupByPublicationYear, String agreementId,
                                String accessCountPeriod, String start, String end, boolean csv) {
    return getUseOverTime(pool, isJournal, includeOA, groupByPublicationYear, agreementId,
        accessCountPeriod, start, end)
        .compose(json -> {
          if (!csv) {
            return Future.succeededFuture(json.encodePrettily());
          }
          try {
            StringWriter stringWriter = new StringWriter();
            getUseOverTime2Csv(json, groupByPublicationYear, false, stringWriter);
            return Future.succeededFuture(stringWriter.toString());
          } catch (IOException e) {
            return Future.failedFuture(e);
          }
        });
  }

  Future<JsonObject> getUseOverTime(TenantPgPool pool,
      boolean isJournal, boolean includeOA, boolean groupByPublicationYear,
      String agreementId, String accessCountPeriod, String start, String end) {

    Periods periods = new Periods(start, end, accessCountPeriod);
    Tuple tuple = Tuple.of(agreementId);
    periods.addStartDates(tuple);
    periods.addEnd(tuple);

    StringBuilder sql = new StringBuilder();
    if (includeOA) {
      useOverTime(sql, pool, isJournal, true, true, groupByPublicationYear, periods.size());
      sql.append(" UNION ");
      useOverTime(sql, pool, isJournal, true, false, groupByPublicationYear, periods.size());
      sql.append(" UNION ");
    }
    useOverTime(sql, pool, isJournal, false, true, groupByPublicationYear, periods.size());
    sql.append(" UNION ");
    useOverTime(sql, pool, isJournal, false, false, groupByPublicationYear, periods.size());
    if (groupByPublicationYear) {
      sql.append(" ORDER BY title, kbId, accessType, publicationYear, metricType");
    } else {
      sql.append(" ORDER BY title, kbId, accessType, metricType");
    }

    return pool.preparedQuery(sql.toString()).execute(tuple).map(rowSet -> {
      JsonArray items = new JsonArray();
      LongAdder [] totalItemRequestsByPeriod = LongAdder.arrayOfLength(periods.size());
      LongAdder [] uniqueItemRequestsByPeriod = LongAdder.arrayOfLength(periods.size());
      rowSet.forEach(row -> {
        JsonArray accessCountsByPeriod = new JsonArray();
        LongAdder accessCountTotal = new LongAdder();
        boolean unique = "Unique_Item_Requests".equals(row.getString("metrictype"));
        final int journalColumnsToSkip = groupByPublicationYear ? 7 : 6;
        final int bookColumnsToSkip = 5;
        int pos0 = isJournal ? journalColumnsToSkip : bookColumnsToSkip;
        for (int i = 0; i < periods.size(); i++) {
          Long l = row.getLong(pos0 + i);
          accessCountsByPeriod.add(l);
          accessCountTotal.add(l);
          if (unique) {
            uniqueItemRequestsByPeriod[i].add(l);
          } else {
            totalItemRequestsByPeriod[i].add(l);
          }
        }
        JsonObject json = new JsonObject()
            .put("kbId", row.getUUID("kbid"))
            .put("title", row.getString("title"));
        if (isJournal) {
          json
              .put("printISSN", row.getString("printissn"))
              .put("onlineISSN", row.getString("onlineissn"));
        } else {
          json.put("ISBN", row.getString("isbn"));
        }
        if (groupByPublicationYear) {
          json.put("publicationYear", row.getInteger("publicationyear"));
        }
        json
            .put("accessType", row.getString("accesstype"))
            .put("metricType", row.getString("metrictype"))
            .put("accessCountTotal", accessCountTotal.get())
            .put("accessCountsByPeriod", accessCountsByPeriod);
        items.add(json);
      });
      return new JsonObject()
          .put("agreementId", agreementId)
          .put("accessCountPeriods", periods.getAccessCountPeriods())
          .put("totalItemRequestsTotal", LongAdder.sum(totalItemRequestsByPeriod))
          .put("totalItemRequestsByPeriod", LongAdder.longArray(totalItemRequestsByPeriod))
          .put("uniqueItemRequestsTotal", LongAdder.sum(uniqueItemRequestsByPeriod))
          .put("uniqueItemRequestsByPeriod", LongAdder.longArray(uniqueItemRequestsByPeriod))
          .put("items", items);
    });
  }

  /**
   * Append to <code>sql</code>. The appended SELECT statement is like this for kb titles:
   *
   * <pre>
   * SELECT title_entries.kbTitleId AS kbId,
   *        kbTitleName, printISSN, onlineISSN,
   *        publicationYear,
   *        'OA_Gold' AS accessType, 'Unique_Item_Requests' AS metricType,
   *        n0, n1
   * FROM agreement_entries
   * JOIN title_entries USING (kbTitleId)
   * LEFT JOIN (
   *   SELECT titleEntryId,
   *          extract(year from publicationDate)::integer AS publicationYear,
   *          sum(uniqueAccessCount) AS n0
   *   FROM title_data
   *   WHERE daterange($2, $3) @> lower(usageDateRange) AND openAccess
   *   GROUP BY 1, 2
   * ) t0 ON t0.titleEntryId = title_entries.id
   * LEFT JOIN (
   *   SELECT titleEntryId,
   *          extract(year from publicationDate)::integer AS publicationYear,
   *          sum(uniqueAccessCount) AS n1
   *   FROM title_data
   *   WHERE daterange($3, $4) @> lower(usageDateRange) AND openAccess
   *   GROUP BY 1, 2
   * ) t1 ON t1.titleEntryId = title_entries.id
   * WHERE agreementId = $1
   *   AND NOT (printISSN IS NULL AND onlineISSN IS NULL)
   * </pre>
   */
  private static void useOverTime(StringBuilder sql, TenantPgPool pool,
                                  boolean isJournal, boolean openAccess, boolean unique,
                                  boolean groupByPublicationYear, int periods) {

    sql.append("SELECT title_entries.kbTitleId AS kbId, kbTitleName AS title,")
        .append(isJournal ? " printISSN, onlineISSN, "
            : " ISBN, ")
        .append(groupByPublicationYear ? (periods > 0 ? "t0.publicationYear AS publicationYear, "
            : "null AS publicationYear, ") : "")
        .append(openAccess ? "'OA_Gold' AS accessType, "
            : "'Controlled' AS accessType, ")
        .append(unique ? "'Unique_Item_Requests' AS metricType "
            : "'Total_Item_Requests' AS metricType ");
    for (int i = 0; i < periods; i++) {
      sql.append(", n").append(i);
    }
    sql
        .append(" FROM ").append(agreementEntriesTable(pool))
        .append(" LEFT JOIN ").append(packageEntriesTable(pool))
        .append(" USING (kbPackageId)")
        .append(" JOIN ").append(titleEntriesTable(pool)).append(" ON")
        .append(" title_entries.kbTitleId = agreement_entries.kbTitleId OR")
        .append(" title_entries.kbTitleId = package_entries.kbTitleId");
    for (int i = 0; i < periods; i++) {
      sql.append(" LEFT JOIN (")
      .append(" SELECT titleEntryId, ")
        .append(groupByPublicationYear
            ? "extract(year FROM publicationDate)::integer AS publicationYear, "
            : "")
        .append("sum(")
        .append(unique ? "uniqueAccessCount" : "totalAccessCount")
        .append(") AS n").append(i)
      .append(" FROM ").append(titleDataTable(pool))
      .append(" WHERE daterange($").append(2 + i).append(", $").append(2 + i + 1)
        .append(") @> lower(usageDateRange)")
        .append(openAccess ? " AND openAccess" : " AND NOT openAccess")
      .append(groupByPublicationYear ? " GROUP BY 1, 2" : " GROUP BY 1")
      .append(" ) t").append(i).append(" ON t").append(i).append(".titleEntryId = ")
        .append(titleEntriesTable(pool)).append(".id");
    }
    sql.append(" WHERE agreementId = $1 AND ")
        .append(isJournal ? "NOT " : "")
        .append("(printISSN IS NULL AND onlineISSN IS NULL)");
  }

  Future<Void> getReqsByDateOfUse(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));

    return getUseOverTime(pool, true, includeOA, true, agreementId,
        accessCountPeriod, start, end, csv)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<Void> getReqsByPubYear(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");
    String periodOfUse = ctx.request().params().get("periodOfUse");

    return getReqsByPubYear(pool, includeOA, agreementId,
        accessCountPeriod, start, end, periodOfUse, csv)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<String> getReqsByPubYear(TenantPgPool pool, boolean includeOA, String agreementId,
                                  String accessCountPeriod, String start, String end,
                                  String periodOfUse, boolean csv) {
    return getReqsByPubYear(pool, includeOA, agreementId,
        accessCountPeriod, start, end, periodOfUse)
        .compose(json -> {
          if (!csv) {
            return Future.succeededFuture(json.encodePrettily());
          }
          try {
            StringWriter stringWriter = new StringWriter();
            getUseOverTime2Csv(json, false, true, stringWriter);
            return Future.succeededFuture(stringWriter.toString());
          } catch (IOException e) {
            return Future.failedFuture(e);
          }
        });
  }

  Future<JsonObject> getReqsByPubYear(TenantPgPool pool, boolean includeOA, String agreementId,
      String accessCountPeriod, String start, String end, String periodOfUse) {

    Periods usePeriods = new Periods(start, end, periodOfUse);
    int pubPeriodsInMonths = accessCountPeriod == null || "auto".equals(accessCountPeriod)
        ? 12 : Periods.getPeriodInMonths(accessCountPeriod);
    return getPubPeriods(pool, agreementId, usePeriods, pubPeriodsInMonths)
        .compose(pubYears -> {
          StringBuilder sql = new StringBuilder();

          for (int i = 0; i < usePeriods.size(); i++) {
            if (i > 0) {
              sql.append(" UNION ");
            }
            if (includeOA) {
              reqsByPubYear(sql, pool, true, true, pubYears.size(), i);
              sql.append(" UNION ");
              reqsByPubYear(sql, pool, true, false, pubYears.size(), i);
              sql.append(" UNION ");
            }
            reqsByPubYear(sql, pool, false, true, pubYears.size(), i);
            sql.append(" UNION ");
            reqsByPubYear(sql, pool, false, false, pubYears.size(), i);
          }
          sql.append(" ORDER BY title, kbId, accessType, periodOfUse, metricType");

          JsonArray pubYearStrings = new JsonArray();
          Tuple tuple = Tuple.of(agreementId);
          if (! pubYears.isEmpty()) {
            pubYears.forEach(year -> {
              tuple.addLocalDate(year);
              tuple.addLocalDate(year.plusMonths(pubPeriodsInMonths));
              String s1 = year.toString();
              String s2 = year.plusMonths(pubPeriodsInMonths - 1).toString();
              if (s1.endsWith("-01-01") && s2.endsWith("-12-01")) {
                s1 = s1.substring(0, 4);
                s2 = s2.substring(0, 4);
              } else {
                s1 = s1.substring(0, 7);
                s2 = s2.substring(0, 7);
              }
              if (s1.equals(s2)) {
                pubYearStrings.add(s1);
              } else {
                pubYearStrings.add(s1 + "-" + s2);
              }
            });
          }
          LocalDate date = usePeriods.startDate;
          do {
            tuple.addLocalDate(date);
            tuple.addString(usePeriods.periodLabel(date));
            date = date.plus(usePeriods.period);
          } while (date.isBefore(usePeriods.endDate));
          usePeriods.addEnd(tuple);

          System.out.println(sql);
          System.out.println(tuple.deepToString());
          return pool.preparedQuery(sql.toString()).execute(tuple).map(rowSet -> {
            JsonArray items = new JsonArray();
            LongAdder [] totalItemRequestsByPub = LongAdder.arrayOfLength(pubYears.size());
            LongAdder [] uniqueItemRequestsByPub = LongAdder.arrayOfLength(pubYears.size());
            rowSet.forEach(row -> {
              JsonArray accessCountsByPub = new JsonArray();
              LongAdder accessCountTotal = new LongAdder();
              boolean unique = "Unique_Item_Requests".equals(row.getString("metrictype"));
              final int columnsToSkip = 7;
              for (int i = 0; i < pubYears.size(); i++) {
                Long l = row.getLong(columnsToSkip + i);
                accessCountsByPub.add(l);
                accessCountTotal.add(l);
                if (unique) {
                  uniqueItemRequestsByPub[i].add(l);
                } else {
                  totalItemRequestsByPub[i].add(l);
                }
              }
              JsonObject json = new JsonObject()
                  .put("kbId", row.getUUID("kbid"))
                  .put("title", row.getString("title"))
                  .put("printISSN", row.getString("printissn"))
                  .put("onlineISSN", row.getString("onlineissn"))
                  .put("periodOfUse", row.getString("periodofuse"))
                  .put("accessType", row.getString("accesstype"))
                  .put("metricType", row.getString("metrictype"))
                  .put("accessCountTotal", accessCountTotal.get())
                  .put("accessCountsByPeriod", accessCountsByPub);
              items.add(json);
            });

            return new JsonObject()
                .put("agreementId", agreementId)
                .put("accessCountPeriods", pubYearStrings)
                .put("totalItemRequestsTotal", LongAdder.sum(totalItemRequestsByPub))
                .put("totalItemRequestsByPeriod", LongAdder.longArray(totalItemRequestsByPub))
                .put("uniqueItemRequestsTotal", LongAdder.sum(uniqueItemRequestsByPub))
                .put("uniqueItemRequestsByPeriod", LongAdder.longArray(uniqueItemRequestsByPub))
                .put("items", items);
          });
        });
  }

  /**
   * Distinct publication periods (like quarters or years), sorted.
   * @return first day of publication period
   */
  Future<List<LocalDate>> getPubPeriods(TenantPgPool pool, String agreementId,
      Periods usePeriods, int pubPeriodLengthInMonths) {

    String sql =
        "SELECT distinct(" + pool.getSchema() + ".floor_months(publicationDate, $4::integer))"
        + " FROM " + agreementEntriesTable(pool)
        + " LEFT JOIN " + packageEntriesTable(pool) + " USING (kbPackageId)"
        + " JOIN " + titleEntriesTable(pool) + " ON"
        + " title_entries.kbTitleId = agreement_entries.kbTitleId OR"
        + " title_entries.kbTitleId = package_entries.kbTitleId"
        + " JOIN " + titleDataTable(pool) + " ON titleEntryId = title_entries.id"
        + " WHERE agreementId = $1"
        + "   AND publicationDate IS NOT NULL"
        + "   AND (printISSN IS NOT NULL OR onlineISSN IS NOT NULL)"
        + "   AND daterange($2, $3) @> lower(usageDateRange)"
        + " ORDER BY 1";
    return pool.preparedQuery(sql)
        .collecting(Collectors.mapping(row -> row.getLocalDate(0), Collectors.toList()))
        .execute(Tuple.of(agreementId, usePeriods.startDate, usePeriods.endDate,
            pubPeriodLengthInMonths))
        .map(SqlResult::value);
  }

  /**
   * Append to <code>sql</code>. The appended statement is like this:
   *
   * <pre>
   * SELECT title_entries.kbTitleId AS kbId,
   *        kbTitleName, printISSN, onlineISSN,
   *        $7 AS periodOfUse,
   *        'OA_Gold' AS accessType, 'Unique_Item_Requests' AS metricType,
   *        n0, n1
   * FROM agreement_entries
   * JOIN title_entries ON agreement_entries.kbTitleId = title_entries.id
   * LEFT JOIN (
   *   SELECT titleEntryId, sum(uniqueAcessCount) AS n0
   *   FROM title_data
   *   WHERE daterange($2, $3) @> publicationDate
   *     AND daterange($5, $7) @> lower(usageDateRange)
   *     AND openAccess
   *   GROUP BY titleEntryId
   * ) t0 ON t0.titleEntryId = title_entries.id
   * LEFT JOIN (
   *   SELECT titleEntryId, sum(uniqueAccessCount) AS n1
   *   FROM title_data
   *   WHERE daterange($4, $5) @> publicationDate
   *     AND daterange($5, $7) @> lower(usageDateRange)
   *     AND openAccess
   *   GROUP BY titleEntryId
   * ) t1 ON t1.titleEntryId = title_entries.id
   * WHERE agreementId = $1
   *   AND (printISSN IS NOT NULL OR onlineISSN IS NOT NULL)
   * </pre>
   *
   * <p>This is for $1 = agreementId,
   * <br>$2, $3 = 1st publication year start and end
   * <br>$4, $5 = 2nd publication year start and end
   * <br>$6 = start of usage range 1
   * <br>$7 = label for usage range 1
   * <br>$8 = end of usage range 1
   * ($8 is also start of usage range 2 if there were more usage ranges)
   */
  private static void reqsByPubYear(StringBuilder sql, TenantPgPool pool,
      boolean openAccess, boolean unique, int publicationPeriods, int usePeriod) {

    final int pubPeriodPos = 2;
    final int usePeriodPos = pubPeriodPos + 2 * publicationPeriods;
    sql
        .append("SELECT title_entries.kbTitleId AS kbId, kbTitleName AS title,")
        .append(" printISSN, onlineISSN, $")
        .append(usePeriodPos + 2 * usePeriod + 1).append(" AS periodOfUse,")
        .append(openAccess ? " 'OA_Gold' AS accessType,"
                           : " 'Controlled' AS accessType,")
        .append(unique ? " 'Unique_Item_Requests' AS metricType"
                       : " 'Total_Item_Requests' AS metricType");
    for (int i = 0; i < publicationPeriods; i++) {
      sql.append(", n").append(i);
    }
    sql
        .append(" FROM ").append(agreementEntriesTable(pool))
        .append(" LEFT JOIN ").append(packageEntriesTable(pool)).append(" USING (kbPackageId)")
        .append(" JOIN ").append(titleEntriesTable(pool)).append(" ON")
        .append(" title_entries.kbTitleId = agreement_entries.kbTitleId OR")
        .append(" title_entries.kbTitleId = package_entries.kbTitleId");
    for (int i = 0; i < publicationPeriods; i++) {
      sql
      .append(" LEFT JOIN (")
      .append(" SELECT titleEntryId, sum(")
        .append(unique ? "uniqueAccessCount" : "totalAccessCount").append(") AS n").append(i)
      .append(" FROM ").append(titleDataTable(pool))
      .append(" WHERE daterange($")
          .append(pubPeriodPos + 2 * i).append("::date, $")
          .append(pubPeriodPos + 2 * i + 1).append("::date) @> publicationDate")
        .append(" AND daterange($")
          .append(usePeriodPos + 2 * usePeriod).append("::date, $")
          .append(usePeriodPos + 2 * usePeriod + 2).append("::date) @> lower(usageDateRange)")
        .append(openAccess ? " AND openAccess" : " AND NOT openAccess")
      .append(" GROUP BY titleEntryId")
      .append(" ) t").append(i)
      .append(" ON t").append(i).append(".titleEntryId = ")
        .append(titleEntriesTable(pool)).append(".id");
    }
    sql
        .append(" WHERE agreementId = $1::uuid")
        .append("   AND (printISSN IS NOT NULL OR onlineISSN IS NOT NULL)");
  }

  static Number formatCost(Double n) {
    return Double.parseDouble(costDecimalFormat.format(n));
  }

  static void getCostPerUse2Csv(JsonObject json, Appendable appendable)
      throws IOException {
    CSVPrinter writer = new CSVPrinter(appendable, CSVFormat.EXCEL);
    writer.print("Agreement line");
    writer.print("Publication Type");
    writer.print("Print ISSN");
    writer.print("Online ISSN");
    writer.print("ISBN");
    writer.print("Order type");
    writer.print("Purchase order line");
    writer.print("Invoice number");
    writer.print("Fiscal start");
    writer.print("Fiscal end");
    writer.print("Subscription start");
    writer.print("Subscription end");
    writer.print("Reporting period");
    writer.print("Amount encumbered");
    writer.print("Amount paid");
    writer.print("Total item requests");
    writer.print("Unique item requests");
    writer.print("Cost per request - total");
    writer.print("Cost per request - unique");
    writer.println();
  }

  private static void costPerUse(StringBuilder sql, TenantPgPool pool,
                                 boolean includeOA, int periods) {
    sql
        .append("SELECT title_entries.kbTitleId AS kbId, kbTitleName AS title,")
        .append(" printISSN, onlineISSN, ISBN, orderType, poLineNumber, invoiceNumber,")
        .append(" fiscalYearRange, subscriptionDateRange,")
        .append(" encumberedCost, invoicedCost");

    for (int i = 0; i < periods; i++) {
      sql.append(", total").append(i);
      sql.append(", unique").append(i);
    }
    sql
        .append(" FROM ").append(agreementEntriesTable(pool))
        .append(" LEFT JOIN ").append(packageEntriesTable(pool))
        .append(" USING (kbPackageId)")
        .append(" JOIN ").append(titleEntriesTable(pool)).append(" ON")
        .append(" title_entries.kbTitleId = agreement_entries.kbTitleId OR")
        .append(" title_entries.kbTitleId = package_entries.kbTitleId");
    for (int i = 0; i < periods; i++) {
      sql.append(" LEFT JOIN (")
          .append(" SELECT titleEntryId")
          .append(", sum(totalAccessCount) as total").append(i)
          .append(", sum(uniqueAccessCount) as unique").append(i)
          .append(" FROM ").append(titleDataTable(pool))
          .append(" WHERE daterange($").append(2 + i).append(", $").append(2 + i + 1)
          .append(") @> lower(usageDateRange)")
          .append(includeOA ? "" : " AND NOT openAccess")
          .append(" GROUP BY 1")
          .append(" ) t").append(i).append(" ON t").append(i).append(".titleEntryId = ")
          .append(titleEntriesTable(pool)).append(".id")
          .append(" AND (subscriptionDateRange IS NULL")
          .append(" OR subscriptionDateRange @> daterange($")
          .append(2 + i).append(", $").append(2 + i + 1).append("))")
      ;

      // TODO  .append(" AND coverageDateRanges @> t").append(i).append(".publicationDate")

    }
    sql.append(" WHERE agreementId = $1");
  }

  Future<JsonObject> costPerUse(TenantPgPool pool, boolean includeOA, String agreementId,
                                String accessCountPeriod, String start, String end) {

    Periods periods = new Periods(start, end, accessCountPeriod);
    Tuple tuple = Tuple.of(agreementId);
    periods.addStartDates(tuple);
    periods.addEnd(tuple);

    StringBuilder sql = new StringBuilder();
    costPerUse(sql, pool, includeOA, periods.size());
    sql.append(" ORDER BY title");
    log.info("AD: costPerUse SQL={}", sql.toString());

    return pool.preparedQuery(sql.toString()).execute(tuple).map(rowSet -> {
      JsonArray paidByPeriod = new JsonArray();
      JsonArray totalRequests = new JsonArray();
      JsonArray uniqueRequests = new JsonArray();
      JsonArray titleCountByPeriod = new JsonArray();
      for (int i = 0; i < periods.size(); i++) {
        paidByPeriod.add(0.0);
        totalRequests.add(0L);
        uniqueRequests.add(0L);
        titleCountByPeriod.add(0L);
      }
      rowSet.forEach(row -> {
        for (int i = 0; i < periods.size(); i++) {
          Long totalAccessCount = row.getLong(12 + i * 2);
          Long uniqueAccessCount = row.getLong(13 + i * 2);
          log.debug("Inspecting i={} totalAccessCount={} uniqueAccessCount={}",
              i, totalAccessCount, uniqueAccessCount);
          if (totalAccessCount == null || uniqueAccessCount == null) {
            continue;
          }
          titleCountByPeriod.set(i, titleCountByPeriod.getLong(i) + 1L);
        }
      });
      AtomicLong totalTitles = new AtomicLong();
      for (int i = 0; i < periods.size(); i++) {
        totalTitles.addAndGet(titleCountByPeriod.getLong(i));
      }
      JsonArray items = new JsonArray();
      rowSet.forEach(row -> {
        log.info("AD: row={}", row.deepToString());
        for (int i = 0; i < periods.size(); i++) {
          Long totalAccessCount = row.getLong(12 + i * 2);
          Long uniqueAccessCount = row.getLong(13 + i * 2);
          if (totalAccessCount == null || uniqueAccessCount == null) {
            continue;
          }
          JsonObject item = new JsonObject()
              .put("kbId", row.getUUID(0))
              .put("title", row.getString(1))
              .put("derivedTitle", false);
          String printIssn = row.getString(2);
          if (printIssn != null) {
            item.put("printISSN", printIssn);
          }
          String onlineIssn = row.getString(3);
          if (onlineIssn != null) {
            item.put("onlineISSN", onlineIssn);
          }
          String isbn = row.getString(4);
          if (isbn != null) {
            item.put("ISBN", isbn);
          }
          String orderType = row.getString(5);
          item.put("orderType", orderType != null ? orderType : "Ongoing");
          String poLineNumber = row.getString(6);
          if (poLineNumber != null) {
            item.put("poLineIDs", new JsonArray().add(poLineNumber));
          }
          String invoiceNumbers = row.getString(7);
          if (invoiceNumbers != null) {
            item.put("invoiceNumbers", new JsonArray().add(invoiceNumbers));
          }
          String fiscalYearRange = row.getString(8);
          if (fiscalYearRange != null) {
            DateRange dateRange = new DateRange(fiscalYearRange);
            item.put("fiscalDateStart", dateRange.getStart());
            item.put("fiscalDateEnd", dateRange.getEnd());
          }
          String subscriptionDateRange = row.getString(9);
          if (subscriptionDateRange != null) {
            DateRange dateRange = new DateRange(subscriptionDateRange);
            item.put("subscriptionDateStart", dateRange.getStart());
            item.put("subscriptionDateEnd", dateRange.getEnd());
          }
          Number encumberedCost = row.getNumeric(10);
          if (encumberedCost != null) {
            item.put("amountEncumbered", formatCost(
                encumberedCost.doubleValue() / totalTitles.get()));
          }
          Number amountPaid = row.getNumeric(11);
          if (amountPaid != null) {
            item.put("amountPaid", formatCost(
                amountPaid.doubleValue() / totalTitles.get()));
          }
          item.put("totalItemRequests", totalAccessCount);
          item.put("uniqueItemRequests", uniqueAccessCount);
          totalRequests.set(i, totalRequests.getLong(i) + totalAccessCount);
          uniqueRequests.set(i, uniqueRequests.getLong(i) + uniqueAccessCount);

          if (amountPaid != null) {
            paidByPeriod.set(i, amountPaid.doubleValue());
            if (totalAccessCount > 0) {
              Double costPerTotalRequest = amountPaid.doubleValue()
                  / (totalAccessCount * totalTitles.get());
              item.put("costPerTotalRequest", formatCost(costPerTotalRequest));
            }
            if (uniqueAccessCount > 0) {
              Double costPerUniqueRequest = amountPaid.doubleValue()
                  / (uniqueAccessCount * totalTitles.get());
              item.put("costPerUniqueRequest", formatCost(costPerUniqueRequest));
            }
            items.add(item);
          }
        }
      });
      JsonArray totalItemCostsPerRequestsByPeriod = new JsonArray();
      JsonArray uniqueItemCostsPerRequestsByPeriod = new JsonArray();
      for (int i = 0; i < periods.size(); i++) {
        Long d = titleCountByPeriod.getLong(i);
        Double p = paidByPeriod.getDouble(i);
        Long n = totalRequests.getLong(i);
        long c = totalTitles.get();
        if (n > 0) {
          log.info("totalItemCostsPerRequestsByPerid {} {}*{}/({}*{})",
              i, d, p, n, c);
          totalItemCostsPerRequestsByPeriod.add(formatCost(d * p / (n * c)));
        } else {
          totalItemCostsPerRequestsByPeriod.addNull();
        }
        n = uniqueRequests.getLong(i);
        if (n > 0) {
          log.info("uniqueItemCostsPerRequestsByPerid {} {}*{}/({}*{})",
              i, d, paidByPeriod.getDouble(i), n, c);
          uniqueItemCostsPerRequestsByPeriod.add(formatCost(d * p / (n * c)));
        } else {
          uniqueItemCostsPerRequestsByPeriod.addNull();
        }
      }
      JsonObject json = new JsonObject();
      json.put("accessCountPeriods", periods.getAccessCountPeriods());
      json.put("totalItemCostsPerRequestsByPeriod", totalItemCostsPerRequestsByPeriod);
      json.put("uniqueItemCostsPerRequestsByPeriod", uniqueItemCostsPerRequestsByPeriod);
      json.put("titleCountByPeriod", titleCountByPeriod);
      json.put("items", items);
      return json;
    });
  }

  Future<String> getCostPerUse(TenantPgPool pool, boolean includeOA, String agreementId,
                               String accessCountPeriod, String start, String end, boolean csv) {
    return costPerUse(pool, includeOA, agreementId, accessCountPeriod, start, end)
        .compose(json -> {
          if (!csv) {
            return Future.succeededFuture(json.encodePrettily());
          }
          try {
            StringWriter stringWriter = new StringWriter();
            getCostPerUse2Csv(json, stringWriter);
            return Future.succeededFuture(stringWriter.toString());
          } catch (IOException e) {
            return Future.failedFuture(e);
          }
        });
  }

  Future<Void> getCostPerUse(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");

    return getCostPerUse(pool, includeOA, agreementId, accessCountPeriod, start, end, csv)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  private void add(RouterBuilder routerBuilder,
      String operationId, Function<RoutingContext, Future<Void>> function) {

    routerBuilder
    .operation(operationId)
    .handler(ctx -> {
      try {
        function.apply(ctx)
            .onFailure(cause -> failHandler(400, ctx, cause));
      } catch (Throwable t) {
        failHandler(400, ctx, t);
      }
    }).failureHandler(EusageReportsApi::failHandler);
  }

  @Override
  public Future<Router> createRouter(Vertx vertx, WebClient webClient) {
    this.webClient = webClient;
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .map(routerBuilder -> {
          add(routerBuilder, "getReportTitles", ctx -> getReportTitles(vertx, ctx));
          add(routerBuilder, "postReportTitles", ctx -> postReportTitles(vertx, ctx));
          add(routerBuilder, "postFromCounter", ctx -> postFromCounter(vertx, ctx));
          add(routerBuilder, "getTitleData", ctx -> getTitleData(vertx, ctx));
          add(routerBuilder, "getReportData", ctx -> getReportData(vertx, ctx));
          add(routerBuilder, "postFromAgreement", ctx -> postFromAgreement(vertx, ctx));
          add(routerBuilder, "getUseOverTime", ctx -> getUseOverTime(vertx, ctx));
          add(routerBuilder, "getReqsByDateOfUse", ctx -> getReqsByDateOfUse(vertx, ctx));
          add(routerBuilder, "getReqsByPubYear", ctx -> getReqsByPubYear(vertx, ctx));
          add(routerBuilder, "getCostPerUse", ctx -> getCostPerUse(vertx, ctx));
          return routerBuilder.createRouter();
        });
  }

  @Override
  public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
    if (!tenantAttributes.containsKey("module_to")) {
      return Future.succeededFuture(); // doing nothing for disable
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, tenant);
    return pool.execute(List.of(
        "SET search_path TO " + pool.getSchema(),
        "CREATE TABLE IF NOT EXISTS " + titleEntriesTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "counterReportTitle text UNIQUE, "
            + "kbTitleName text, "
            + "kbTitleId UUID, "
            + "kbManualMatch boolean, "
            + "printISSN text, "
            + "onlineISSN text, "
            + "ISBN text, "
            + "DOI text"
            + ")",
        "CREATE INDEX IF NOT EXISTS title_entries_kbTitleId ON "
            + titleEntriesTable(pool) + " USING btree(kbTitleId)",
        "CREATE TABLE IF NOT EXISTS " + packageEntriesTable(pool) + " ( "
            + "kbPackageId UUID, "
            + "kbPackageName text, "
            + "kbTitleId UUID "
            + ")",
        "CREATE INDEX IF NOT EXISTS package_entries_kbTitleId ON "
            + packageEntriesTable(pool) + " USING btree(kbTitleId)",
        "CREATE INDEX IF NOT EXISTS package_entries_kbPackageId ON "
            + packageEntriesTable(pool) + " USING btree(kbPackageId)",
        "CREATE TABLE IF NOT EXISTS " + titleDataTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "titleEntryId UUID, "
            + "counterReportId UUID, "
            + "counterReportTitle text, "
            + "providerId UUID, "
            + "publicationDate date, "
            + "usageDateRange daterange,"
            + "uniqueAccessCount integer, "
            + "totalAccessCount integer, "
            + "openAccess boolean NOT NULL"
            + ")",
        "CREATE INDEX IF NOT EXISTS title_data_entries_titleEntryId ON "
            + titleDataTable(pool) + " USING btree(titleEntryId)",
        "CREATE INDEX IF NOT EXISTS title_data_entries_counterReportId ON "
            + titleDataTable(pool) + " USING btree(counterReportId)",
        "CREATE INDEX IF NOT EXISTS title_data_entries_providerId ON "
            + titleDataTable(pool) + " USING btree(providerId)",
        "CREATE TABLE IF NOT EXISTS " + agreementEntriesTable(pool) + " ( "
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
            + "coverageDateRanges daterange,"
            + "orderType text,"
            + "invoiceNumber text,"
            + "poLineNumber text"
            + ")",
        "CREATE INDEX IF NOT EXISTS agreement_entries_agreementId ON "
            + agreementEntriesTable(pool) + " USING btree(agreementId)",
        "CREATE OR REPLACE FUNCTION " + pool.getSchema() + ".floor_months(date, integer)"
            + " RETURNS date AS $$\n"
            + "-- floor_months(date, n) returns the start of the period date belongs to,\n"
            + "-- periods are n months long. Examples:\n"
            + "-- Begin of quarter (3 months): floor_months('2019-05-17', 3) = '2019-04-01'.\n"
            + "-- Begin of decade (10 years): floor_months('2019-05-17', 120) = '2010-01-01'.\n"
            + "  SELECT make_date(m / 12, m % 12 + 1, 1)\n"
            + "  FROM (SELECT ((12 * extract(year FROM $1)::integer\n"
            + "                 + extract(month FROM $1)::integer - 1) / $2) * $2) AS x(m)\n"
            + "$$ LANGUAGE SQL IMMUTABLE STRICT"
      ));
  }
}
