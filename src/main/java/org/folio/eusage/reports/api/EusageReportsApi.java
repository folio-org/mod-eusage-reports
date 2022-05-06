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
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
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
import org.folio.tlib.postgres.PgCqlField;
import org.folio.tlib.postgres.PgCqlQuery;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.util.TenantUtil;

public class EusageReportsApi implements RouterCreator, TenantInitHooks {
  private static final Logger log = LogManager.getLogger(EusageReportsApi.class);

  private WebClient webClient;

  static final String LIMIT_ALL = "?limit=2147483647";

  public EusageReportsApi(WebClient webClient) {
    this.webClient = webClient;
  }

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

  static String statusTable(TenantPgPool pool) {
    return pool.getSchema() + ".status";
  }

  static void failHandler(RoutingContext ctx) {
    Throwable t = ctx.failure();
    // both semantic errors and syntax errors are from same pile ... Choosing 400 over 422.
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

  static void resultFooter(RoutingContext ctx, RowSet<Row> rowSet, List<String[]> facets,
      String diagnostic) {

    JsonObject resultInfo = new JsonObject();
    int count = 0;
    JsonArray facetArray = new JsonArray();
    if (rowSet != null) {
      int pos = 0;
      Row row = rowSet.iterator().next();
      count = row.getInteger(pos);
      for (String [] facetEntry : facets) {
        pos++;
        JsonObject facetObj = null;
        final String facetType = facetEntry[0];
        final String facetValue = facetEntry[1];
        for (int i = 0; i < facetArray.size(); i++) {
          facetObj = facetArray.getJsonObject(i);
          if (facetType.equals(facetObj.getString("type"))) {
            break;
          }
          facetObj = null;
        }
        if (facetObj == null) {
          facetObj = new JsonObject();
          facetObj.put("type", facetType);
          facetObj.put("facetValues", new JsonArray());
          facetArray.add(facetObj);
        }
        JsonArray facetValues = facetObj.getJsonArray("facetValues");
        facetValues.add(new JsonObject()
            .put("value", facetValue)
            .put("count", row.getInteger(pos)));
      }
    }
    resultInfo.put("totalRecords", count);
    JsonArray diagnostics = new JsonArray();
    if (diagnostic != null) {
      diagnostics.add(new JsonObject().put("message", diagnostic));
    }
    resultInfo.put("diagnostics", diagnostics);
    resultInfo.put("facets", facetArray);
    ctx.response().write("], \"resultInfo\": " + resultInfo.encode() + "}");
    ctx.response().end();
  }

  static Future<Void> streamResult(RoutingContext ctx, SqlConnection sqlConnection,
      String query, String cnt, String property, List<String[]> facets,
      Function<Row, JsonObject> handler) {

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
                  .onSuccess(cntRes -> resultFooter(ctx, cntRes, facets, null))
                  .onFailure(f -> {
                    log.error(f.getMessage(), f);
                    resultFooter(ctx, null, facets, f.getMessage());
                  })
                  .eventually(x -> tx.commit().compose(y -> sqlConnection.close())));
              stream.exceptionHandler(e -> {
                log.error("stream error {}", e.getMessage(), e);
                resultFooter(ctx, null, facets, e.getMessage());
                tx.commit().compose(y -> sqlConnection.close());
              });
              return Future.succeededFuture();
            })
        );
  }

  static Future<Void> streamResult(RoutingContext ctx, TenantPgPool pool, String distinct,
      String from, String orderByClause, String property, Function<Row, JsonObject> handler) {

    return streamResult(ctx, pool, distinct, distinct, List.of(from), Collections.EMPTY_LIST,
        orderByClause, property, handler);
  }

  static Future<Void> streamResult(RoutingContext ctx, TenantPgPool pool,
      String distinctMain, String distinctCount,
      List<String> fromList, List<String[]> facets, String orderByClause,
      String property,
      Function<Row, JsonObject> handler) {

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    Integer offset = params.queryParameter("offset").getInteger();
    Integer limit = params.queryParameter("limit").getInteger();
    String query = "SELECT " + (distinctMain != null ? "DISTINCT ON (" + distinctMain + ")" : "")
        + " * FROM " + fromList.get(0)
        + (orderByClause == null ?  "" : " ORDER BY " + orderByClause)
        + " LIMIT " + limit + " OFFSET " + offset;
    log.info("query={}", query);
    StringBuilder countQuery = new StringBuilder("SELECT");
    int pos = 0;
    for (String from : fromList) {
      if (pos > 0) {
        countQuery.append(",\n");
      }
      countQuery.append("(SELECT COUNT("
          + (distinctCount != null ? "DISTINCT " + distinctCount : "*")
          + ") FROM " + from + ") AS cnt" + pos);
      pos++;
    }
    log.info("cnt={}", countQuery.toString());
    return pool.getConnection()
        .compose(sqlConnection -> streamResult(ctx, sqlConnection, query, countQuery.toString(),
            property, facets, handler)
            .onFailure(x -> sqlConnection.close()));
  }

  Future<Void> getReportTitles(Vertx vertx, RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(new PgCqlField("title_entries.id", "id", PgCqlField.Type.UUID));
    // must pass table name as there are joins involved
    pgCqlQuery.addField(new PgCqlField("title_entries.counterreporttitle",
        "counterReportTitle", PgCqlField.Type.FULLTEXT));
    pgCqlQuery.addField(new PgCqlField("title_entries.isbn",
        "ISBN", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("title_entries.printissn",
        "printISSN", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("title_entries.onlineissn",
        "onlineISSN", PgCqlField.Type.TEXT));
    pgCqlQuery.addField(new PgCqlField("title_entries.kbtitleid",
        "kbTitleId", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("title_entries.kbmanualmatch",
        "kbManualMatch", PgCqlField.Type.BOOLEAN));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String counterReportId = stringOrNull(params.queryParameter("counterReportId"));
    final String providerId = stringOrNull(params.queryParameter("providerId"));
    final TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    final String distinctCount = titleEntriesTable(pool) + ".id";
    final String query = stringOrNull(params.queryParameter("query"));
    RequestParameter facetsParameter = params.queryParameter("facets");
    String [] facetsList = facetsParameter == null
        ? new String[0]
        : facetsParameter.getString().split(",");

    boolean includeStatusFacet = false;
    for (String name : facetsList) {
      if ("status".equals(name)) {
        includeStatusFacet = true;
      } else {
        throw new IllegalArgumentException("Unsupported facet: " + name);
      }
    }
    List<String> fromList = new ArrayList<>(); // main query and facet queries
    pgCqlQuery.parse(query);
    String orderByFields = pgCqlQuery.getOrderByFields();
    String distinctMain = distinctCount;
    if (orderByFields != null) {
      distinctMain = orderByFields + "," + distinctMain;
    }
    final String orderByClause = pgCqlQuery.getOrderByClause();
    fromList.add(getFromTitleDataForeignKey(pgCqlQuery, counterReportId, providerId, pool));

    List<String[]> statusValues = new ArrayList<>();
    if (includeStatusFacet) {
      // add query for each status facet
      statusValues.add(new String[]{"status", "matched"});
      statusValues.add(new String[]{"status", "unmatched"});
      statusValues.add(new String[]{"status", "ignored"});

      pgCqlQuery.parse(query, "kbTitleId = \"\"");
      fromList.add(getFromTitleDataForeignKey(pgCqlQuery, counterReportId, providerId, pool));

      pgCqlQuery.parse(query, "kbTitleId <> \"\" AND kbManualMatch = false");
      fromList.add(getFromTitleDataForeignKey(pgCqlQuery, counterReportId, providerId, pool));

      pgCqlQuery.parse(query, "kbTitleId <> \"\" AND kbManualMatch = true");
      fromList.add(getFromTitleDataForeignKey(pgCqlQuery, counterReportId, providerId, pool));
    }
    return streamResult(ctx, pool, distinctMain, distinctCount, fromList, statusValues,
        orderByClause,"titles",
        row -> new JsonObject()
            .put("id", row.getUUID("id"))
            .put("counterReportTitle", row.getString("counterreporttitle"))
            .put("kbTitleName", row.getString("kbtitlename"))
            .put("kbTitleId", row.getUUID("kbtitleid"))
            .put("kbManualMatch", row.getBoolean("kbmanualmatch"))
            .put("printISSN", row.getString("printissn"))
            .put("onlineISSN", row.getString("onlineissn"))
            .put("ISBN", row.getString("isbn"))
            .put("DOI", row.getString("doi"))
            .put("publicationType", row.getString("publicationtype"))
    );
  }

  private String getFromTitleDataForeignKey(PgCqlQuery pgCqlQuery, String counterReportId,
      String providerId, TenantPgPool pool) {

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
    return from;
  }

  Future<Void> postReportTitles(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
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

  Future<Void> getReportPackages(Vertx vertx, RoutingContext ctx) {
    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("kbPackageId", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("kbPackageName", PgCqlField.Type.FULLTEXT));
    pgCqlQuery.addField(new PgCqlField("kbTitleId", PgCqlField.Type.UUID));

    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));

    pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));
    String cqlWhere = pgCqlQuery.getWhereClause();

    String from = packageEntriesTable(pool);
    if (cqlWhere != null) {
      from = from + " WHERE " + cqlWhere;
    }
    return streamResult(ctx, pool, null, from, pgCqlQuery.getOrderByClause(), "packages",
        row -> new JsonObject()
            .put("kbPackageId", row.getUUID("kbpackageid"))
            .put("kbPackageName", row.getString("kbpackagename"))
            .put("kbTitleId", row.getUUID("kbtitleid"))
    );
  }

  Future<Void> getTitleData(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);

    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(new PgCqlField("id", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("counterReportId", PgCqlField.Type.UUID));

    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    String from = titleDataTable(pool);
    pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));
    String cqlWhere = pgCqlQuery.getWhereClause();
    if (cqlWhere != null) {
      from = from + " WHERE " + cqlWhere;
    }

    return streamResult(ctx, pool, null, from, pgCqlQuery.getOrderByClause(), "data",
        row -> {
          JsonObject obj = new JsonObject()
              .put("id", row.getUUID("id"))
              .put("titleEntryId", row.getUUID("titleentryid"))
              .put("counterReportId", row.getUUID("counterreportid"))
              .put("counterReportTitle", row.getString("counterreporttitle"))
              .put("providerId", row.getUUID("providerid"))
              .put("usageDateRange", row.getString("usagedaterange"))
              .put("uniqueAccessCount", row.getInteger("uniqueaccesscount"))
              .put("totalAccessCount", row.getInteger("totalaccesscount"))
              .put("openAccess", row.getBoolean("openaccess"));
          LocalDate publicationDate = row.getLocalDate("publicationdate");
          if (publicationDate != null) {
            obj.put("publicationDate",
                publicationDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
          }
          return obj;
        }
    );
  }

  Future<Void> postFromCounter(Vertx vertx, RoutingContext ctx) {
    return populateCounterReportTitles(vertx, ctx)
        .onFailure(x -> log.error(x.getMessage(), x))
        .compose(x -> {
          if (Boolean.TRUE.equals(x)) {
            ctx.response().setStatusCode(200);
            ctx.response().putHeader("Content-Type", "application/json");
            ctx.response().end("{}");
            return Future.succeededFuture();
          }
          failHandler(404, ctx, "Not Found");
          return Future.succeededFuture();
        });
  }

  static Tuple parseErmTitle(JsonObject resource) {
    UUID titleId = UUID.fromString(resource.getString("id"));
    JsonObject pubObj = resource.getJsonObject("publicationType");
    String publicationType = pubObj == null ? null : pubObj.getString("value");
    return Tuple.of(titleId, resource.getString("name"), publicationType);
  }

  Future<Tuple> ermTitleLookup2(RoutingContext ctx, String identifier, String type) {
    if (identifier == null) {
      return Future.succeededFuture();
    }
    // some titles do not have hyphen in identifier, so try that as well
    String identifierNoHyphen = identifier.replace("-", "");
    return ermTitleLookup(ctx, identifier, type)
        .compose(x -> x != null || identifierNoHyphen.equals(identifier)
            ? Future.succeededFuture(x)
            : ermTitleLookup(ctx, identifierNoHyphen, type)
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
    return ermFetch(ctx, uri)
        .map(ar -> {
          List<UUID> list = new ArrayList<>();
          for (int i = 0; i < ar.size(); i++) {
            JsonObject pti = ar.getJsonObject(i).getJsonObject("pti");
            JsonObject titleInstance = pti.getJsonObject("titleInstance");
            UUID kbTitleId = UUID.fromString(titleInstance.getString("id"));
            list.add(kbTitleId);
          }
          return list;
        });
  }

  Future<JsonArray> ermFetch(RoutingContext ctx, String uri) {
    return ermFetch(ctx, uri, 1, new JsonArray());
  }

  Future<JsonArray> ermFetch(RoutingContext ctx, String uri, int page, JsonArray result) {
    char sep = uri.contains("?") ? '&' : '?';
    final String pageUri = uri + sep + "perPage=100&page=" + page;
    return getRequestSend(ctx, pageUri)
        .compose(res -> {
          JsonArray ar = res.bodyAsJsonArray();
          if (ar.isEmpty()) {
            return Future.succeededFuture(result);
          }
          result.addAll(ar);
          return ermFetch(ctx, uri, page + 1, result);
        });
  }

  Future<UUID> updateTitleEntryByKbTitle(TenantPgPool pool, SqlConnection con, UUID kbTitleId,
      String counterReportTitle, String printIssn, String onlineIssn, String isbn, String doi,
      String publicationType) {

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
          UUID id = row.getUUID("id");
          return con.preparedQuery("UPDATE " + titleEntriesTable(pool)
                  + " SET"
                  + " counterReportTitle = $2,"
                  + " printISSN = $3,"
                  + " onlineISSN = $4,"
                  + " ISBN = $5,"
                  + " DOI = $6,"
                  + " publicationType = $7"
                  + " WHERE id = $1")
              .execute(Tuple.of(id, counterReportTitle, printIssn, onlineIssn, isbn, doi,
                  publicationType))
              .map(id);
        });
  }

  Future<UUID> upsertTitleEntryCounterReport(TenantPgPool pool, SqlConnection con,
      RoutingContext ctx, String counterReportTitle,
      String printIssn, String onlineIssn, String isbn, String doi) {

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
            UUID id = row.getUUID("id");
            Boolean kbManualMatch = row.getBoolean("kbmanualmatch");
            if (row.getUUID("kbtitleid") != null || Boolean.TRUE.equals(kbManualMatch)) {
              return Future.succeededFuture(id);
            }
            return ermTitleLookup2(ctx, identifier, type).compose(erm -> {
              if (erm == null) {
                return Future.succeededFuture(id);
              }
              UUID kbTitleId = erm.getUUID(0);
              String kbTitleName = erm.getString(1);
              String publicationType = erm.getString(2);
              return con.preparedQuery("UPDATE " + titleEntriesTable(pool)
                      + " SET"
                      + " kbTitleName = $2,"
                      + " kbTitleId = $3,"
                      + " publicationType = $4"
                      + " WHERE id = $1")
                  .execute(Tuple.of(id, kbTitleName, kbTitleId, publicationType)).map(id);
            });
          }
          return ermTitleLookup2(ctx, identifier, type).compose(erm -> {
            UUID kbTitleId = erm != null ? erm.getUUID(0) : null;
            String kbTitleName = erm != null ? erm.getString(1) : null;
            String publicationType = erm != null ? erm.getString(2) : null;

            return updateTitleEntryByKbTitle(pool, con, kbTitleId,
                counterReportTitle, printIssn, onlineIssn, isbn, doi, publicationType)
                .compose(id -> {
                  if (id != null) {
                    return Future.succeededFuture(id);
                  }
                  return con.preparedQuery(" INSERT INTO " + titleEntriesTable(pool)
                          + "(id, counterReportTitle,"
                          + " kbTitleName, kbTitleId,"
                          + " kbManualMatch, printISSN, onlineISSN, ISBN, DOI, publicationType)"
                          + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
                          + " ON CONFLICT (counterReportTitle) DO NOTHING")
                      .execute(Tuple.of(UUID.randomUUID(), counterReportTitle,
                          kbTitleName, kbTitleId,
                          false, printIssn, onlineIssn, isbn, doi, publicationType))
                      .compose(x ->
                          con.preparedQuery("SELECT id FROM " + titleEntriesTable(pool)
                                  + " WHERE counterReportTitle = $1")
                              .execute(Tuple.of(counterReportTitle))
                              .map(res2 -> res2.iterator().next().getUUID("id")));
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
          Row row = res.iterator().hasNext() ? res.iterator().next() : null;
          if (row != null && row.getString("publicationtype") != null) {
            return Future.succeededFuture();
          }
          final UUID id = row != null ? row.getUUID("id") : null;
          return ermTitleLookup(ctx, kbTitleId).compose(erm -> {
            String kbTitleName = erm.getString(1);
            String publicationType = erm.getString(2);
            if (id == null) {
              return con.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
                      + "(id, kbTitleName, kbTitleId, kbManualMatch, publicationType)"
                      + " VALUES ($1, $2, $3, $4, $5)")
                  .execute(Tuple.of(UUID.randomUUID(), kbTitleName, kbTitleId, false,
                      publicationType))
                  .mapEmpty();
            } else {
              return con.preparedQuery("UPDATE " + titleEntriesTable(pool)
                      + " SET publicationType = $2 WHERE id = $1")
                  .execute(Tuple.of(id, publicationType))
                  .mapEmpty();
            }
          });
        });
  }

  Future<Void> createPackageFromAgreement(TenantPgPool pool, SqlConnection con, UUID kbPackageId,
      String kbPackageName, RoutingContext ctx) {

    return ermPackageContentLookup(ctx, kbPackageId)
        .compose(list -> {
          Future<Void> future = con.preparedQuery("DELETE FROM " + packageEntriesTable(pool)
              + " WHERE kbPackageId = $1").execute(Tuple.of(kbPackageId)).mapEmpty();
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
      int uniqueAccessCount, int totalAccessCount, boolean openAccess) {
    return con.preparedQuery("INSERT INTO " + titleDataTable(pool)
            + "(id, titleEntryId,"
            + " counterReportId, counterReportTitle, providerId,"
            + " publicationDate, usageDateRange,"
            + " uniqueAccessCount, totalAccessCount, openAccess)"
            + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")
        .execute(Tuple.of(UUID.randomUUID(), titleEntryId,
            counterReportId, counterReportTitle, providerId,
            publicationDate, usageDateRange,
            uniqueAccessCount, totalAccessCount, openAccess))
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
    final String yopString = reportItem.getString("YOP");
    final String accessType = reportItem.getString("Access_Type");
    final String usageDateRange = getUsageDate(reportItem);
    final JsonObject identifiers = getIssnIdentifiers(reportItem);
    final String onlineIssn = identifiers.getString("onlineISSN");
    final String printIssn = identifiers.getString("printISSN");
    final String isbn = identifiers.getString("ISBN");
    final String doi = identifiers.getString("DOI");
    String publicationDate = identifiers.getString("Publication_Date");

    // YOP "0001": unknown, "9999": not yet known.. Also "1" seen ..
    Integer yop = yopString == null ? 1 : Integer.parseInt(yopString);
    final LocalDate pubdate =
        publicationDate != null ? LocalDate.parse(publicationDate)
            : yop != 1 && yop != 9999 ? LocalDate.of(yop, 1, 1)
            : null;

    log.debug("handleReport title={} match={}", counterReportTitle, onlineIssn);
    final int totalAccessCount = getTotalCount(reportItem, "Total_Item_Requests");
    final int uniqueAccessCount = getTotalCount(reportItem, "Unique_Item_Requests");
    final boolean openAccess = "OA_Gold".equals(accessType);
    return upsertTitleEntryCounterReport(pool, con, ctx, counterReportTitle,
        printIssn, onlineIssn, isbn, doi)
        .compose(titleEntryId -> insertTdEntry(pool, con, titleEntryId, counterReportId,
            counterReportTitle, providerId, pubdate, usageDateRange,
            uniqueAccessCount, totalAccessCount, openAccess));
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
        log.error("GET {} returned status code {}: {}", uri, res.statusCode(), res.bodyAsString());
        throw new RuntimeException("GET " + uri + " returned status code " + res.statusCode());
      }
      log.info("{}", res.bodyAsString());
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
    final String okapiUrl = stringOrNull(params.headerParameter(XOkapiHeaders.URL));
    final String id = ctx.getBodyAsJson().getString("counterReportId");
    final String providerId = ctx.getBodyAsJson().getString("providerId");

    if (okapiUrl == null) {
      return Future.failedFuture("Missing " + XOkapiHeaders.URL);
    }
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    return populateCounterReportTitles(ctx, pool, id, providerId,0);
  }

  private Future<Boolean> populateCounterReportTitles(RoutingContext ctx, TenantPgPool pool,
      String id, String providerId, int offset) {

    Promise<Void> promise = Promise.promise();
    String parms = "";
    if (providerId != null) {
      parms = "&query=providerId%3D%3D" + providerId;
    }
    final int limit = 1;
    final String uri = "/counter-reports" + (id != null ? "/" + id
        : "?limit=" + limit + "&offset=" + offset + parms);
    JsonParser parser = JsonParser.newParser();
    AtomicBoolean objectMode = new AtomicBoolean(false);
    List<Future<Void>> futures = new ArrayList<>();
    AtomicInteger pathSize = new AtomicInteger(id != null ? 1 : 0);
    AtomicInteger totalRecords = new AtomicInteger(0);
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
          log.debug("Field = {} pathSize={}", f, pathSize.get());
          if (pathSize.get() == 1 && "totalRecords".equals(f)) {
            totalRecords.set(event.integerValue());
          }
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
          }).eventually(x -> con.close())
          .compose(res -> {
            if (offset + limit >= totalRecords.get()) {
              return Future.succeededFuture(res);
            }
            return populateCounterReportTitles(ctx, pool, id, providerId,offset + limit);
          });
    });
  }

  Future<Void> getReportData(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);

    PgCqlQuery pgCqlQuery = PgCqlQuery.query();
    pgCqlQuery.addField(new PgCqlField("cql.allRecords", PgCqlField.Type.ALWAYS_MATCHES));
    pgCqlQuery.addField(new PgCqlField("id", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("kbTitleId", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("kbPackageId", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("agreementLineId", PgCqlField.Type.UUID));
    pgCqlQuery.addField(new PgCqlField("agreementId", PgCqlField.Type.UUID));

    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    String from = agreementEntriesTable(pool);
    pgCqlQuery.parse(stringOrNull(params.queryParameter("query")));
    String cqlWhere = pgCqlQuery.getWhereClause();
    if (cqlWhere != null) {
      from = from + " WHERE " + cqlWhere;
    }

    return streamResult(ctx, pool, null, from, pgCqlQuery.getOrderByClause(),
        "data", row ->
            new JsonObject()
                .put("id", row.getUUID("id"))
                .put("kbTitleId", row.getUUID("kbtitleid"))
                .put("kbPackageId", row.getUUID("kbpackageid"))
                .put("type", row.getString("type"))
                .put("agreementId", row.getUUID("agreementid"))
                .put("agreementLineId", row.getUUID("agreementlineid"))
                .put("poLineId", row.getUUID("polineid"))
                .put("encumberedCost", row.getNumeric("encumberedcost"))
                .put("invoicedCost", row.getNumeric("invoicedcost"))
                .put("fiscalYearRange", row.getString("fiscalyearrange"))
                .put("subscriptionDateRange", row.getString("subscriptiondaterange"))
                .put("coverageDateRanges", row.getString("coveragedateranges"))
                .put("orderType", row.getString("ordertype"))
                .put("invoiceNumber", row.getString("invoicenumber"))
                .put("poLineNumber", row.getString("polinenumber"))
    );
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
        .onSuccess(purchase -> result.put("orderType", purchase.getString("orderType", "Ongoing")))
        .mapEmpty();
  }

  /**
   * Fetch PO line by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-orders/schemas/composite_po_line.json">
   * po line schema</a>
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-orders-storage/schemas/fund_distribution.json">
   * fund distribution</a>
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
    String uri = "/invoice-storage/invoice-lines" + LIMIT_ALL + "&query=poLineId%3D%3D" + poLineId;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Fetch invoice by invoice ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-invoice-storage/schemas/invoice.json">invoice schema</a>
   * @param invoiceId invoice ID
   * @param ctx Routing context.
   * @return invoice response JSON object.
   */
  Future<JsonObject> lookupInvoice(UUID invoiceId, RoutingContext ctx) {
    String uri = "/invoice-storage/invoices/" + invoiceId;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  /**
   * Fetch budgets tied to fund.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/budget.json">budget schema</a>
   * @param fundId fund identifier.
   * @param ctx routing context.
   * @return budget collection.
   */
  Future<JsonObject> lookupBudgets(UUID fundId, RoutingContext ctx) {
    String uri = "/finance-storage/budgets" + LIMIT_ALL + "&query=fundId%3D%3D" + fundId;
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

  /**
   * Fetch transaction by ID.
   * @see <a
   * href="https://github.com/folio-org/acq-models/blob/master/mod-finance/schemas/transaction.json">
   * transaction schema</a>
   * @param id encumbrance identifier.
   * @param ctx Routing Context.
   * @return Transaction object.
   */
  Future<JsonObject> lookupTransaction(UUID id, RoutingContext ctx) {
    String uri = "/finance-storage/transactions/" + id;
    return getRequestSend(ctx, uri)
        .map(HttpResponse::bodyAsJsonObject);
  }

  Future<Void> getEncumbrance(JsonArray fundDistribution, JsonObject result, RoutingContext ctx) {
    result.put("encumberedCost", 0.0);
    Future<Void> future = Future.succeededFuture();
    if (fundDistribution == null) {
      return future;
    }
    for (int i = 0; i < fundDistribution.size(); i++) {
      JsonObject fund = fundDistribution.getJsonObject(i);
      String encumbrance = fund.getString("encumbrance");
      if (encumbrance != null) {
        future = future.compose(x -> lookupTransaction(UUID.fromString(encumbrance), ctx)
            .map(transaction -> {
              result.put("encumberedCost",
                  result.getDouble("encumberedCost") + transaction.getDouble("amount"));
              return null;
            }));
      }
    }
    return future;
  }

  Future<Void> getAllFiscalYears(JsonObject poLine, JsonObject result, RoutingContext ctx) {
    Future<Void> future = Future.succeededFuture();
    JsonArray fundDistribution = poLine.getJsonArray("fundDistribution");
    JsonArray fiscalYears = new JsonArray();
    result.put("allFiscalYears", fiscalYears);
    if (fundDistribution == null) {
      return Future.succeededFuture();
    }
    for (int i = 0; i < fundDistribution.size(); i++) {
      // fundId is a required property
      UUID fundId = UUID.fromString(fundDistribution.getJsonObject(i).getString("fundId"));
      future = future.compose(x -> lookupBudgets(fundId, ctx)).compose(budgetCollection -> {
        Future<Void> future1 = Future.succeededFuture();
        JsonArray budgets = budgetCollection.getJsonArray("budgets");
        for (int j = 0; j < budgets.size(); j++) {
          JsonObject budget = budgets.getJsonObject(j);
          // fiscalYearId is a required property
          UUID fiscalYearId = UUID.fromString(budget.getString("fiscalYearId"));
          future1 = future1.compose(x -> lookupFiscalYear(fiscalYearId, ctx).map(fiscalYear -> {
            // periodStart, periodEnd are required properties
            fiscalYears.add(getRange(fiscalYear, "periodStart", "periodEnd"));
            return null;
          }));
        }
        return future1;
      });
    }
    return future;
  }

  Future<Integer> getFiscalYearIndex(UUID invoiceId, JsonObject result, RoutingContext ctx) {
    return lookupInvoice(invoiceId, ctx).compose(invoice -> {
      String date = invoice.getString("paymentDate");
      if (date == null) {
        date = invoice.getString("invoiceDate");
      }
      result.put("folioInvoiceNo", invoice.getString("folioInvoiceNo"));
      JsonArray fiscalYears = result.getJsonArray("allFiscalYears");
      LocalDate localDate = LocalDate.parse(date.substring(0, 10));
      for (int i = 0; i < fiscalYears.size(); i++) {
        String fiscalYear = fiscalYears.getString(i);
        if (fiscalYear != null) {
          DateRange d = new DateRange(fiscalYears.getString(i));
          if (d.includes(localDate)) {
            return Future.succeededFuture(i);
          }
        }
      }
      return Future.succeededFuture(null);
    });
  }

  Future<JsonObject> parsePoLine(JsonObject poLine, RoutingContext ctx) {
    JsonObject result = new JsonObject();
    result.put("invoicedCost", 0.0);
    JsonArray subscriptionPeriods = new JsonArray();
    result.put("subscriptionPeriods", subscriptionPeriods);
    JsonArray invoicedPeriods = new JsonArray();
    JsonArray fiscalYears = new JsonArray();
    result.put("fiscalYear", fiscalYears);
    result.put("invoicedPeriods", invoicedPeriods);
    JsonArray invoiceNumbers = new JsonArray();
    result.put("invoiceNumber", invoiceNumbers);
    UUID poLineId = UUID.fromString(poLine.getString("poLineId"));
    return lookupOrderLine(poLineId, ctx).compose(orderLine -> {
      result.put("poLineNumber", orderLine.getString("poLineNumber"));
      JsonObject cost = orderLine.getJsonObject("cost");
      result.put("currency", cost.getString("currency"));
      return getOrderType(orderLine, ctx, result)
          .compose(x -> getEncumbrance(orderLine.getJsonArray("fundDistribution"), result, ctx))
          .compose(x -> getAllFiscalYears(orderLine, result, ctx))
          .compose(x -> lookupInvoiceLines(poLineId, ctx))
          .compose(invoiceResponse -> {
            JsonArray invoices = invoiceResponse.getJsonArray("invoiceLines");
            Future<Void> future = Future.succeededFuture();
            for (int j = 0; j < invoices.size(); j++) {
              JsonObject invoiceLine = invoices.getJsonObject(j);
              // invoiceId is a required property
              UUID invoiceId = UUID.fromString(invoiceLine.getString("invoiceId"));
              future = future.compose(x -> getFiscalYearIndex(invoiceId, result, ctx))
                  .compose(index -> {
                    String fiscalYear = index != null
                        ? result.getJsonArray("allFiscalYears").getString(index) : null;
                    Double thisTotal = invoiceLine.getDouble("total");
                    if (thisTotal != null) {
                      result.put("invoicedCost", thisTotal + result.getDouble("invoicedCost"));
                    }
                    String range = getRange(invoiceLine, "subscriptionStart", "subscriptionEnd");
                    if (range != null || fiscalYear != null) {
                      subscriptionPeriods.add(range);
                      invoicedPeriods.add(thisTotal != null ? thisTotal : 0.0);
                      fiscalYears.add(fiscalYear);
                      StringBuilder invoice = new StringBuilder();
                      String invoicePrefix = result.getString("folioInvoiceNo");
                      if (invoicePrefix != null) {
                        invoice.append(invoicePrefix);
                        invoice.append("-");
                      }
                      invoice.append(invoiceLine.getString("invoiceLineNumber"));
                      invoiceNumbers.add(invoice.toString());
                    }
                    return Future.succeededFuture();
                  });
            }
            return future.map(result);
          });
    });
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

  static Future<Void> clearAgreement(TenantPgPool pool, SqlConnection con, UUID agreementId) {
    return con.preparedQuery("DELETE FROM " + agreementEntriesTable(pool)
            + " WHERE agreementId = $1")
        .execute(Tuple.of(agreementId))
        .mapEmpty();
  }

  Future<Void> populateAgreementLine(TenantPgPool pool, SqlConnection con,
      JsonObject agreementLine, UUID agreementId, RoutingContext ctx) {

    try {
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
      Future<Void> future = createTitleFromAgreement(pool, con, kbTitleId, ctx);
      if (kbPackageId != null) {
        future = future.compose(x -> createPackageFromAgreement(pool, con, kbPackageId,
            kbPackageName, ctx));
      }
      JsonArray poLines = agreementLine.getJsonArray("poLines");
      JsonObject currencyObj = new JsonObject();
      if (poLines.isEmpty()) {
        return future.compose(x -> insertAgreementLine(pool, con, agreementId, agreementLineId,
          coverageDateRanges, type, kbTitleId, kbPackageId));
      }
      for (int i = 0; i < poLines.size(); i++) {
        JsonObject poLine = poLines.getJsonObject(i);
        UUID poLineId = UUID.fromString(poLine.getString("poLineId"));
        future = future
            .compose(x -> parsePoLine(poLine, ctx))
            .compose(poResult -> {
              String currency = currencyObj.getString("currency");
              String newCurrency = poResult.getString("currency");
              if (currency != null && !currency.equals(newCurrency)) {
                return Future.failedFuture("Mixed currencies (" + currency + ", " + newCurrency
                    + ") in PO lines " + poLine.encodePrettily());
              }
              currencyObj.put("currency", newCurrency);
              return populateParsedPoLine(pool, con, agreementId, agreementLineId,
                  coverageDateRanges, type, kbTitleId, kbPackageId, poLineId, poResult);
            });
      }
      return future;
    } catch (Exception e) {
      log.error("Failed to decode agreementLine: {}", e.getMessage(), e);
      return Future.failedFuture("Failed to decode agreement line: " + e.getMessage());
    }
  }

  private Future<Void> populateParsedPoLine(TenantPgPool pool, SqlConnection con, UUID agreementId,
      UUID agreementLineId, String coverageDateRanges, String type, UUID kbTitleId,
      UUID kbPackageId, UUID poLineId, JsonObject poResult) {

    JsonArray fiscalYears = poResult.getJsonArray("fiscalYear");
    JsonArray subscriptionPeriods = poResult.getJsonArray("subscriptionPeriods");
    JsonArray invoicedPeriods = poResult.getJsonArray("invoicedPeriods");
    JsonArray invoiceNumbers = poResult.getJsonArray("invoiceNumber");
    if (subscriptionPeriods.isEmpty()) {
      return insertAgreementLine(pool, con, agreementId, agreementLineId,
          coverageDateRanges, type, kbTitleId, kbPackageId, poLineId, poResult,
          null, null, null, null);
    }
    Future<Void> future = Future.succeededFuture();
    for (int i = 0; i < subscriptionPeriods.size(); i++) {
      String subscriptionDateRange = subscriptionPeriods.getString(i);
      String fiscalYear = fiscalYears.getString(i);
      Double invoicedCost = invoicedPeriods.getDouble(i);
      String invoiceNumber = invoiceNumbers.getString(i);
      future = future.compose(x -> insertAgreementLine(pool, con, agreementId, agreementLineId,
          coverageDateRanges, type, kbTitleId, kbPackageId, poLineId, poResult,
          invoicedCost, invoiceNumber, fiscalYear, subscriptionDateRange));
    }
    return future;
  }

  private Future<Void> insertAgreementLine(TenantPgPool pool, SqlConnection con, UUID agreementId,
      UUID agreementLineId, String coverageDateRanges, String type, UUID kbTitleId,
      UUID kbPackageId) {

    UUID id = UUID.randomUUID();
    return con.preparedQuery(
            "INSERT INTO " + agreementEntriesTable(pool)
                + "(id, kbTitleId, kbPackageId, type, agreementId, agreementLineId,"
                + " coverageDateRanges)"
                + " VALUES($1, $2, $3, $4, $5, $6, $7)")
        .execute(Tuple.of(id, kbTitleId, kbPackageId, type, agreementId, agreementLineId,
            coverageDateRanges))
        .mapEmpty();
  }

  private Future<Void> insertAgreementLine(TenantPgPool pool, SqlConnection con, UUID agreementId,
      UUID agreementLineId, String coverageDateRanges, String type, UUID kbTitleId,
      UUID kbPackageId, UUID poLineId, JsonObject poResult, Number invoicedCost,
      String invoiceNumber, String fiscalYearRange, String subscriptionDateRange) {

    UUID id = UUID.randomUUID();
    String orderType = poResult.getString("orderType");
    Double encumberedCost = poResult.getDouble("encumberedCost");
    String poLineNumber = poResult.getString("poLineNumber");
    return con.preparedQuery(
                    "INSERT INTO " + agreementEntriesTable(pool)
                        + "(id, kbTitleId, kbPackageId, type,"
                        + " agreementId, agreementLineId, poLineId,"
                        + " encumberedCost, invoicedCost,"
                        + " fiscalYearRange, subscriptionDateRange,"
                        + " coverageDateRanges, orderType,"
                        + " invoiceNumber,poLineNumber) VALUES"
                        + " ($1, $2, $3, $4, $5, $6, $7, $8, $9,"
                        + " $10, $11, $12, $13, $14, $15)")
                .execute(Tuple.of(id, kbTitleId, kbPackageId, type,
                    agreementId, agreementLineId, poLineId, encumberedCost, invoicedCost,
                    fiscalYearRange, subscriptionDateRange, coverageDateRanges, orderType,
                    invoiceNumber, poLineNumber))
                .mapEmpty();
  }

  Future<Integer> populateAgreement(Vertx vertx, RoutingContext ctx) {
    RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
    final String agreementIdStr = ctx.getBodyAsJson().getString("agreementId");
    if (agreementIdStr == null) {
      return Future.failedFuture("Missing agreementId property");
    }
    final UUID agreementId = UUID.fromString(agreementIdStr);
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    return pool.getConnection().compose(con -> con.begin()
        .compose(tx ->
            agreementExists(ctx, agreementId)
                .compose(exists -> {
                  if (!exists) {
                    return Future.succeededFuture(null);
                  }
                  // expand agreement to get agreement lines, now that we know the ID is good.
                  // the call below returns 500 with a stacktrace if agreement ID is no good.
                  // example: /erm/entitlements?filters=owner%3D3b6623de-de39-4b43-abbc-998bed892025
                  String uri = "/erm/entitlements?filters=owner%3D" + agreementId;
                  return populateStatus(pool, agreementId, true)
                      .compose(x -> clearAgreement(pool, con, agreementId))
                      .compose(x -> ermFetch(ctx, uri))
                      .compose(items -> {
                        Future<Void> future = Future.succeededFuture();
                        for (int i = 0; i < items.size(); i++) {
                          JsonObject agreementLine = items.getJsonObject(i);
                          future = future.compose(v ->
                              populateAgreementLine(pool, con, agreementLine, agreementId, ctx));
                        }
                        return future.compose(x -> tx.commit()).map(items.size());
                      })
                      .eventually(x -> populateStatus(pool, agreementId, false));
                })
        )
        .eventually(x -> con.close())
    );
  }

  Future<Void> populateStatus(TenantPgPool pool, UUID agreementId, boolean active) {
    log.info("populateStatus begin");
    JsonObject status = new JsonObject()
        .put("id", agreementId.toString())
        .put("lastUpdated", LocalDateTime.now(ZoneOffset.UTC).toString())
        .put("active", active);
    return pool.execute("INSERT INTO " + statusTable(pool)
            + "(id, status) VALUES($1, $2) ON CONFLICT(id) DO UPDATE SET status = $2",
        Tuple.of(agreementId, status)).mapEmpty();
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

  Boolean getJournalFromFormat(RoutingContext ctx, String def) {
    String format = ctx.request().params().get("format");
    if (format == null) {
      format = def;
    }
    switch (format) {
      case "JOURNAL":
        return Boolean.TRUE;
      case "BOOK":
        return Boolean.FALSE;
      case "ALL":
        return null;
      default:
        throw new IllegalArgumentException("format = " + format);
    }
  }

  Future<Void> getUseOverTime(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    Boolean isJournal = getJournalFromFormat(ctx, "ALL");
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    boolean full = !"false".equalsIgnoreCase(ctx.request().params().get("full"));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));

    return getUseOverTime(pool, isJournal, includeOA, agreementId, accessCountPeriod,
        start, end, csv, full)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<String> getUseOverTime(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end, boolean csv,
      boolean full) {

    return getUseOverTime(pool, isJournal, includeOA, agreementId,
        accessCountPeriod, start, end)
        .map(json -> {
          if (!full) {
            json.remove("items");
          }
          if (!csv) {
            return json.encodePrettily();
          }
          return CsvReports.getUseOverTime2Csv(json, false, false);
        });
  }

  Future<JsonObject> getUseOverTime(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end) {

    Periods periods = new Periods(start, end, accessCountPeriod);
    long t1 = System.nanoTime();
    return getTitles(pool, isJournal, includeOA, agreementId, periods,
        "title, publicationDate, openAccess")
        .map(rowSet -> {
          long t2 = System.nanoTime();
          JsonObject res = UseOverTime.titlesToJsonObject(rowSet, agreementId, periods);
          long t3 = System.nanoTime();
          res.put("execution", new JsonObject()
              .put("getTitles", t2 - t1)
              .put("parseTitles", t3 - t2)
          );
          return res;
        });
  }

  Future<String> getReqsByDateOfUse(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod,
      String start, String end, String yopInterval, boolean csv, boolean full) {

    return getReqsByDateOfUse(pool, isJournal, includeOA, agreementId,
        accessCountPeriod, start, end, yopInterval)
        .map(json -> {
          if (!full) {
            json.remove("items");
          }
          if (!csv) {
            return json.encodePrettily();
          }
          return CsvReports.getUseOverTime2Csv(json, true, false);
        });
  }

  Future<Void> getReqsByDateOfUse(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    boolean full = !"false".equalsIgnoreCase(ctx.request().params().get("full"));
    Boolean isJournal = getJournalFromFormat(ctx, "JOURNAL");
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");
    String yopInterval = ctx.request().params().get("yopInterval");
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));

    return getReqsByDateOfUse(pool, isJournal, includeOA, agreementId,
        accessCountPeriod, start, end, yopInterval, csv, full)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<JsonObject> getReqsByDateOfUse(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end, String yopInterval) {

    Periods usePeriods = new Periods(start, end, accessCountPeriod);
    int pubPeriodsInMonths = yopInterval == null || "auto".equals(yopInterval)
        ? 12 : Periods.getPeriodInMonths(yopInterval);
    return getTitles(pool, isJournal, includeOA, agreementId, usePeriods,
        "title, publicationDate, openAccess")
        .map(rowSet -> ReqsByDateOfUse.titlesToJsonObject(rowSet, agreementId,
            usePeriods, pubPeriodsInMonths));
  }

  Future<Void> getReqsByPubYear(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    Boolean isJournal = getJournalFromFormat(ctx, "JOURNAL");
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    boolean full = !"false".equalsIgnoreCase(ctx.request().params().get("full"));
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");
    String periodOfUse = ctx.request().params().get("periodOfUse");

    return getReqsByPubYear(pool, isJournal, includeOA, agreementId,
        accessCountPeriod, start, end, periodOfUse, csv, full)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<String> getReqsByPubYear(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end,
      String periodOfUse, boolean csv, boolean full) {

    return getReqsByPubYear(pool, isJournal, includeOA, agreementId,
        accessCountPeriod, start, end, periodOfUse)
        .map(json -> {
          if (!full) {
            json.remove("items");
          }
          if (!csv) {
            return json.encodePrettily();
          }
          return CsvReports.getUseOverTime2Csv(json, false, true);
        });
  }

  Future<JsonObject> getReqsByPubYear(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end, String periodOfUse) {

    Periods usePeriods = new Periods(start, end, periodOfUse);
    int pubPeriodsInMonths = accessCountPeriod == null || "auto".equals(accessCountPeriod)
        ? 12 : Periods.getPeriodInMonths(accessCountPeriod);

    return getTitles(pool, isJournal, includeOA, agreementId, usePeriods,
        "title, usageDateRange, openAccess")
        .map(rowSet -> ReqsByPubYear.titlesToJsonObject(rowSet, agreementId,
            usePeriods, pubPeriodsInMonths));
  }

  static Future<RowSet<Row>> getTitles(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, Periods usePeriods, String orderBy) {

    String sql = "SELECT title_entries.kbTitleId AS kbId, kbTitleName AS title,"
        + " kbPackageId, kbPackageName, printISSN, onlineISSN, ISBN,"
        + " NULL AS publicationDate, NULL AS usageDateRange,"
        + " NULL AS uniqueAccessCount, NULL AS totalAccessCount, TRUE AS openAccess"
        + " FROM " + agreementEntriesTable(pool)
        + " LEFT JOIN " + packageEntriesTable(pool) + " USING (kbPackageId)"
        + " JOIN " + titleEntriesTable(pool) + " ON"
        + " title_entries.kbTitleId = agreement_entries.kbTitleId OR"
        + " title_entries.kbTitleId = package_entries.kbTitleId"
        + " WHERE agreementId = $1"
        + limitJournal(isJournal)
        + " UNION "
        + "SELECT title_entries.kbTitleId AS kbId, kbTitleName AS title,"
        + " kbPackageId, kbPackageName, printISSN, onlineISSN, ISBN,"
        + " publicationDate, usageDateRange, uniqueAccessCount, totalAccessCount, openAccess"
        + " FROM " + agreementEntriesTable(pool)
        + " LEFT JOIN " + packageEntriesTable(pool) + " USING (kbPackageId)"
        + " JOIN " + titleEntriesTable(pool) + " ON"
        + " title_entries.kbTitleId = agreement_entries.kbTitleId OR"
        + " title_entries.kbTitleId = package_entries.kbTitleId"
        + " JOIN " + titleDataTable(pool) + " ON titleEntryId = title_entries.id"
        + " WHERE agreementId = $1"
        + limitJournal(isJournal)
        + "   AND daterange($2, $3) @> lower(usageDateRange)"
        +  (includeOA ? "" : " AND NOT openAccess")
        + " ORDER BY " + orderBy;

    return pool.execute(sql, Tuple.of(agreementId, usePeriods.startDate, usePeriods.endDate));
  }

  static String limitJournal(Boolean isJournal) {
    if (isJournal == null) {
      return "";
    }
    return isJournal ? " AND publicationType = 'serial'"
        : " AND publicationType = 'monograph'";
  }

  static Future<RowSet<Row>> getTitlesCost(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, Periods usePeriods) {

    String sql = "SELECT "
        + " title_entries.kbTitleId AS kbId, kbTitleName AS title,"
        + " kbPackageId, kbPackageName, printISSN, onlineISSN, ISBN,"
        + " NULL AS publicationDate, NULL AS usageDateRange,"
        + " NULL AS uniqueAccessCount, NULL AS totalAccessCount, TRUE AS openAccess,"
        + " orderType, poLineNumber, invoiceNumber,"
        + " fiscalYearRange, subscriptionDateRange,"
        + " encumberedCost, invoicedCost"
        + " FROM " + agreementEntriesTable(pool)
        + " LEFT JOIN " + packageEntriesTable(pool) + " USING (kbPackageId)"
        + " JOIN " + titleEntriesTable(pool) + " ON"
        + " title_entries.kbTitleId = agreement_entries.kbTitleId OR"
        + " title_entries.kbTitleId = package_entries.kbTitleId"
        + " WHERE agreementId = $1"
        + limitJournal(isJournal)
        + " UNION"
        + " SELECT "
        + " title_entries.kbTitleId AS kbId, kbTitleName AS title,"
        + " kbPackageId, kbPackageName, printISSN, onlineISSN, ISBN,"
        + " publicationDate, usageDateRange, uniqueAccessCount, totalAccessCount, openAccess,"
        + " orderType, poLineNumber, invoiceNumber,"
        + " fiscalYearRange, subscriptionDateRange,"
        + " encumberedCost, invoicedCost"
        + " FROM " + agreementEntriesTable(pool)
        + " LEFT JOIN " + packageEntriesTable(pool) + " USING (kbPackageId)"
        + " JOIN " + titleEntriesTable(pool) + " ON"
        + " title_entries.kbTitleId = agreement_entries.kbTitleId OR"
        + " title_entries.kbTitleId = package_entries.kbTitleId"
        + " JOIN " + titleDataTable(pool) + " ON titleEntryId = title_entries.id"
        + " WHERE agreementId = $1"
        + limitJournal(isJournal)
        + "   AND daterange($2, $3) @> lower(usageDateRange)"
        +  (includeOA ? "" : " AND NOT openAccess");

    return pool.execute(sql + " ORDER BY title, publicationDate, openAccess, usageDateRange",
            Tuple.of(agreementId, usePeriods.startDate, usePeriods.endDate));
  }

  Future<JsonObject> costPerUse(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end) {

    Periods periods = new Periods(start, end, accessCountPeriod);
    long t1 = System.nanoTime();
    return getTitlesCost(pool, isJournal, includeOA, agreementId, periods)
        .map(rowSet -> {
          long t2 = System.nanoTime();
          JsonObject res = CostPerUse.titlesToJsonObject(rowSet, periods);
          long t3 = System.nanoTime();
          res.put("execution", new JsonObject()
              .put("getTitles", t2 - t1)
              .put("parseTitles", t3 - t2)
          );
          return res;
        });
  }

  Future<String> getCostPerUse(TenantPgPool pool, Boolean isJournal, boolean includeOA,
      String agreementId, String accessCountPeriod, String start, String end,
      boolean csv, boolean full) {

    return costPerUse(pool, isJournal, includeOA, agreementId, accessCountPeriod, start, end)
        .map(json -> {
          if (!full) {
            json.remove("items");
          }
          if (!csv) {
            return json.encodePrettily();
          }
          return CsvReports.getCostPerUse2Csv(json);
        });
  }

  Future<Void> getCostPerUse(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    Boolean isJournal = getJournalFromFormat(ctx, "ALL");
    boolean csv = "true".equalsIgnoreCase(ctx.request().params().get("csv"));
    boolean full = !"false".equalsIgnoreCase(ctx.request().params().get("full"));
    boolean includeOA = "true".equalsIgnoreCase(ctx.request().params().get("includeOA"));
    String agreementId = ctx.request().params().get("agreementId");
    String accessCountPeriod = ctx.request().params().get("accessCountPeriod");
    String start = ctx.request().params().get("startDate");
    String end = ctx.request().params().get("endDate");

    return getCostPerUse(pool, isJournal, includeOA, agreementId, accessCountPeriod,
        start, end, csv, full)
        .map(res -> {
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", csv ? "text/csv" : "application/json");
          ctx.response().end(res);
          return null;
        });
  }

  Future<Void> getReportStatus(Vertx vertx, RoutingContext ctx) {
    TenantPgPool pool = TenantPgPool.pool(vertx, TenantUtil.tenant(ctx));
    UUID id = UUID.fromString(ctx.request().getParam("id"));
    return pool.execute("SELECT status from " + statusTable(pool) + " WHERE id = $1",
            Tuple.of(id))
        .map(rowSet -> {
          RowIterator<Row> iterator = rowSet.iterator();
          if (!iterator.hasNext()) {
            ctx.response().setStatusCode(404);
            ctx.response().end("No status found for id " + id);
            return null;
          }
          ctx.response().setStatusCode(200);
          ctx.response().putHeader("Content-Type", "application/json");
          ctx.response().end(iterator.next().getJsonObject("status").encodePrettily());
          return null;
        });
  }

  private void add(RouterBuilder routerBuilder, String operationId,
      Function<RoutingContext, Future<Void>> function) {

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
  public Future<Router> createRouter(Vertx vertx) {
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .map(routerBuilder -> {
          add(routerBuilder, "getReportTitles", ctx -> getReportTitles(vertx, ctx));
          add(routerBuilder, "postReportTitles", ctx -> postReportTitles(vertx, ctx));
          add(routerBuilder, "getReportPackages", ctx -> getReportPackages(vertx, ctx));
          add(routerBuilder, "postFromCounter", ctx -> postFromCounter(vertx, ctx));
          add(routerBuilder, "getTitleData", ctx -> getTitleData(vertx, ctx));
          add(routerBuilder, "getReportData", ctx -> getReportData(vertx, ctx));
          add(routerBuilder, "postFromAgreement", ctx -> postFromAgreement(vertx, ctx));
          add(routerBuilder, "getUseOverTime", ctx -> getUseOverTime(vertx, ctx));
          add(routerBuilder, "getReqsByDateOfUse", ctx -> getReqsByDateOfUse(vertx, ctx));
          add(routerBuilder, "getReqsByPubYear", ctx -> getReqsByPubYear(vertx, ctx));
          add(routerBuilder, "getCostPerUse", ctx -> getCostPerUse(vertx, ctx));
          add(routerBuilder, "getReportStatus", ctx -> getReportStatus(vertx, ctx));
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
            + "DOI text, "
            + "publicationType text"
            + ")",
        "CREATE INDEX IF NOT EXISTS title_entries_kbTitleId ON "
            + titleEntriesTable(pool) + " USING btree(kbTitleId)",
        "CREATE INDEX IF NOT EXISTS title_entries_counterReportTitle ON "
            + titleEntriesTable(pool) + " USING btree(counterReportTitle)",
        "CREATE INDEX IF NOT EXISTS title_entries_kbTitleName ON "
            + titleEntriesTable(pool) + " USING btree(kbTitleName)",
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
        "CREATE INDEX IF NOT EXISTS agreement_entries_kbTitleId ON "
            + agreementEntriesTable(pool) + " USING btree(kbTitleId)",
        "CREATE INDEX IF NOT EXISTS agreement_entries_kbPackageId ON "
            + agreementEntriesTable(pool) + " USING btree(kbPackageId)",
        "CREATE TABLE IF NOT EXISTS " + statusTable(pool) + " ( "
            + "id UUID PRIMARY KEY, "
            + "status json"
            + ")",
        "CREATE OR REPLACE FUNCTION " + pool.getSchema() + ".floor_months(date, integer)"
            + " RETURNS date AS $$\n"
            + "-- floor_months(date, n) returns the start of the period date belongs to,\n"
            + "-- periods are n months long. Examples:\n"
            + "-- Begin of quarter (3 months): floor_months('2019-05-17', 3) = '2019-04-01'.\n"
            + "-- Begin of decade (10 years): floor_months('2019-05-17', 120) = '2010-01-01'.\n"
            + "  SELECT make_date(GREATEST(1, m / 12), m % 12 + 1, 1)\n"
            + "  FROM (SELECT ((12 * extract(year FROM $1)::integer\n"
            + "                 + extract(month FROM $1)::integer - 1) / $2) * $2) AS x(m)\n"
            + "$$ LANGUAGE SQL IMMUTABLE STRICT"
    ));
  }
}
