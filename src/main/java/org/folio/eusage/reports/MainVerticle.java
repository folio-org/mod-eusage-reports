package org.folio.eusage.reports;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger("MainVerticle");

  @Override
  public void start(Promise<Void> promise) {
    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-eusage-reports");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    int port = Integer.parseInt(Config.getSysConf("http.port", "port", "8081", config()));
    log.info("Port {}", port);

    Router router = Router.router(vertx);

    Future<Void> future = Future.succeededFuture();
    future = future.compose(x -> createRouterTenantApi())
        .onSuccess(x -> router.mountSubRouter("/", x)).mapEmpty();
    future = future.compose(x -> createRoutereUsageReports(m.getVersion()))
        .onSuccess(x -> router.mountSubRouter("/", x)).mapEmpty();

    future = future.compose(x -> {
      HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
      return vertx.createHttpServer(so)
          .requestHandler(router)
          .listen(port).mapEmpty();
    });
    future.onComplete(promise);
  }

  Future<Router> createRouterTenantApi() {
    return RouterBuilder.create(vertx, "src/main/resources/openapi/tenant-2.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("postTenant")
              .handler(ctx -> {
                log.info("postTenant handler");
                ctx.response().setStatusCode(201);
                ctx.response().putHeader("Content-Type", "application/json");
                JsonObject tenantJob = new JsonObject();
                tenantJob.put("id", "1234");
                ctx.response().end(tenantJob.encode());
              })
              .failureHandler(ctx -> {
                log.info("postTenant failureHandler");
                ctx.response().setStatusCode(400);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end("Failure");
              });
          routerBuilder
              .operation("getTenantJob")
              .handler(ctx -> {
                RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
                String id = params.pathParameter("id").getString();
                RequestParameter wait = params.queryParameter("wait");
                log.info("getTenantJob handler id={} wait={}", id,
                    wait != null ? wait.getInteger() : "null");
                ctx.response().setStatusCode(200);
                ctx.response().putHeader("Content-Type", "application/json");
                ctx.response().end(new JsonObject().put("id", id).encode());
              })
              .failureHandler(ctx -> {
                log.info("getTenantJob failureHandler");
                ctx.response().setStatusCode(400);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end("Failure");
              });
          routerBuilder
              .operation("deleteTenantJob")
              .handler(ctx -> {
                RequestParameters params = ctx.get(ValidationHandler.REQUEST_CONTEXT_KEY);
                String id = params.pathParameter("id").getString();
                log.info("deleteTenantJob handler id={}", id);
                ctx.response().setStatusCode(204);
                ctx.response().end();
              })
              .failureHandler(ctx -> {
                log.info("deleteTenantJob failureHandler");
                ctx.response().setStatusCode(400);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end("Failure");
              });
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

  Future<Router> createRoutereUsageReports(String version) {
    return RouterBuilder.create(vertx, "src/main/resources/openapi/eusage-reports-1.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("getVersion")
              .handler(ctx -> {
                log.info("getVersion handler");
                ctx.response().setStatusCode(200);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end(version == null ? "0.0" : version);
              })
              .failureHandler(ctx -> {
                log.info("getVersion failureHandler");
                ctx.response().setStatusCode(400);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end("Failure");
              });
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

}
