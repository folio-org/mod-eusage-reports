package org.folio.eusage.reports;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import io.vertx.sqlclient.Tuple;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.eusage.reports.postgres.TenantPgPool;
import org.folio.okapi.common.XOkapiHeaders;

public class TenantInitDb {
  private static final Logger log = LogManager.getLogger(TenantInitDb.class);

  static void failHandlerText(RoutingContext ctx, int code, String msg) {
    ctx.response().setStatusCode(code);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end(msg);
  }

  static void failHandler400(RoutingContext ctx, String msg) {
    failHandlerText(ctx, 400, msg);
  }

  static void failHandler404(RoutingContext ctx, String msg) {
    failHandlerText(ctx, 404, msg);
  }

  static void failHandler500(RoutingContext ctx, Throwable e) {
    log.error("{}", e.getMessage(), e);
    failHandlerText(ctx, 500, e.getMessage());
  }

  private static Future<JsonObject> createJob(Vertx vertx, String tenant,
                                              JsonObject tenantAttributes) {
    log.info("postTenant got {}", tenantAttributes.encode());
    TenantPgPool tenantPgPool = TenantPgPool.tenantPgPool(vertx, tenant);
    List<String> cmds = new LinkedList<>();
    if (Boolean.TRUE.equals(tenantAttributes.getBoolean("purge"))) {
      cmds.add("DROP SCHEMA IF EXISTS {schema} CASCADE");
      cmds.add("DROP ROLE IF EXISTS {schema}");
    } else {
      cmds.add("CREATE ROLE {schema} PASSWORD 'tenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN");
      cmds.add("GRANT {schema} TO CURRENT_USER");
      cmds.add("CREATE SCHEMA {schema} AUTHORIZATION {schema}");
      cmds.add("CREATE TABLE IF NOT EXISTS {schema}.job "
          + "(id UUID PRIMARY KEY, jsonb JSONB NOT NULL)");
    }
    return tenantPgPool.execute(cmds)
        .compose(res -> {
          if (Boolean.TRUE.equals(tenantAttributes.getBoolean("purge"))) {
            return Future.succeededFuture(null);
          }
          UUID jobId = UUID.randomUUID();
          JsonObject tenantJob = new JsonObject();
          tenantJob.put("id", jobId.toString());
          tenantJob.put("complete", false);
          tenantJob.put("tenant", tenant);
          tenantJob.put("tenantAttributes", tenantAttributes);
          return tenantPgPool.preparedQuery("INSERT INTO {schema}.job VALUES ($1, $2)")
              .execute(Tuple.of(jobId, tenantJob))
              .map(tenantJob);
        });
  }

  private static Future<JsonObject> getJob(Vertx vertx, String tenant, UUID jobId) {
    TenantPgPool tenantPgPool = TenantPgPool.tenantPgPool(vertx, tenant);
    return tenantPgPool.preparedQuery("SELECT jsonb FROM {schema}.job WHERE ID= $1")
        .execute(Tuple.of(jobId))
        .compose(res -> {
          if (!res.iterator().hasNext()) {
            return Future.succeededFuture(null);
          }
          return Future.succeededFuture(res.iterator().next().getJsonObject(0));
        });
  }

  private static Future<Boolean> deleteJob(Vertx vertx, String tenant, UUID jobId) {
    TenantPgPool tenantPgPool = TenantPgPool.tenantPgPool(vertx, tenant);
    return tenantPgPool.preparedQuery("DELETE FROM {schema}.job WHERE ID= $1")
        .execute(Tuple.of(jobId))
        .compose(res -> {
          if (res.rowCount() == 0) {
            return Future.succeededFuture(Boolean.FALSE);
          }
          return Future.succeededFuture(Boolean.TRUE);
        });
  }


  static void handlers(Vertx vertx, RouterBuilder routerBuilder) {
    log.info("setting up tenant handlers ... begin");
    routerBuilder
        .operation("postTenant")
        .handler(ctx -> {
          try {
            RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
            log.info("postTenant handler {}", params.toJson().encode());
            JsonObject tenantAttributes = ctx.getBodyAsJson();
            String tenant = params.headerParameter(XOkapiHeaders.TENANT).getString();
            createJob(vertx, tenant, tenantAttributes)
                .onSuccess(tenantJob -> {
                  if (tenantJob == null) {
                    ctx.response().setStatusCode(204);
                    ctx.response().end();
                    return;
                  }
                  ctx.response().setStatusCode(201);
                  ctx.response().putHeader("Location", "/_/tenant/" + tenantJob.getString("id"));
                  ctx.response().putHeader("Content-Type", "application/json");
                  ctx.response().end(tenantJob.encode());
                })
                .onFailure(e -> failHandler500(ctx, e));
          } catch (Exception e) {
            failHandler400(ctx, e.getMessage());
          }
        })
        .failureHandler(ctx -> TenantInitDb.failHandler400(ctx, "Failure"));
    routerBuilder
        .operation("getTenantJob")
        .handler(ctx -> {
          try {
            RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
            String id = params.pathParameter("id").getString();
            String tenant = params.headerParameter(XOkapiHeaders.TENANT).getString();
            RequestParameter wait = params.queryParameter("wait");
            log.info("getTenantJob handler id={} wait={}", id,
                wait != null ? wait.getInteger() : "null");
            getJob(vertx, tenant, UUID.fromString(id))
                .onSuccess(res -> {
                  if (res == null) {
                    failHandler404(ctx, "Not found: " + id);
                    return;
                  }
                  ctx.response().setStatusCode(200);
                  ctx.response().putHeader("Content-Type", "application/json");
                  ctx.response().end(res.encode());
                })
                .onFailure(e -> failHandler500(ctx, e));
          } catch (Exception e) {
            failHandler400(ctx, e.getMessage());
          }
        })
        .failureHandler(ctx -> TenantInitDb.failHandler400(ctx, "Failure"));
    routerBuilder
        .operation("deleteTenantJob")
        .handler(ctx -> {
          try {
            RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
            String id = params.pathParameter("id").getString();
            String tenant = params.headerParameter(XOkapiHeaders.TENANT).getString();
            log.info("deleteTenantJob handler id={}", id);
            deleteJob(vertx, tenant, UUID.fromString(id))
                .onSuccess(res -> {
                  if (Boolean.FALSE.equals(res)) {
                    failHandler404(ctx, "Not found: " + id);
                    return;
                  }
                  ctx.response().setStatusCode(204);
                  ctx.response().end();
                })
                .onFailure(e -> failHandler500(ctx, e));
          } catch (Exception e) {
            failHandler400(ctx, e.getMessage());
          }
        })
        .failureHandler(ctx -> TenantInitDb.failHandler400(ctx, "Failure"));
    log.info("setting up tenant handlers ... done");
  }
}
