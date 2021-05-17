package org.folio.eusage.reports;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameter;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.eusage.reports.postgres.TenantPgPool;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger("MainVerticle");

  @Override
  public void start(Promise<Void> promise) {
    TenantPgPool.setModule("mod-eusage-reports");
    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-eusage-reports");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    final int port = Integer.parseInt(
        Config.getSysConf("http.port", "port", "8081", config()));

    Router router = Router.router(vertx);

    Future<Void> future = Future.succeededFuture();
    future = future.compose(x -> createRouterTenantApi())
        .onSuccess(x -> router.mountSubRouter("/", x)).mapEmpty();
    future = future.compose(x -> createRoutereUsageReports(m.getVersion()))
        .onSuccess(x -> router.mountSubRouter("/", x)).mapEmpty();

    router.route(HttpMethod.GET, "/admin/health").handler(ctx -> {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().end("OK");
    });

    future = future.compose(x -> {
      HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
      return vertx.createHttpServer(so)
          .requestHandler(router)
          .listen(port).mapEmpty();
    });
    future.onComplete(promise);
  }

  void failHandler(RoutingContext ctx) {
    ctx.response().setStatusCode(400);
    ctx.response().putHeader("Content-Type", "text/plain");
    ctx.response().end("Failure");
  }

  Future<Router> createRouterTenantApi() {
    return RouterBuilder.create(vertx, "openapi/tenant-2.0.yaml")
        .map(routerBuilder -> {
          TenantInitDb.handlers(vertx, routerBuilder);
          return routerBuilder.createRouter();
        });
  }

  Future<Router> createRoutereUsageReports(String version) {
    return RouterBuilder.create(vertx, "openapi/eusage-reports-1.0.yaml")
        .compose(routerBuilder -> {
          routerBuilder
              .operation("getVersion")
              .handler(ctx -> {
                log.info("getVersion handler");
                ctx.response().setStatusCode(200);
                ctx.response().putHeader("Content-Type", "text/plain");
                ctx.response().end(version == null ? "0.0" : version);
              })
              .failureHandler(this::failHandler);
          return Future.succeededFuture(routerBuilder.createRouter());
        });
  }

}
