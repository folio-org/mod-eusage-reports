package org.folio.tenant.rest.impl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import org.folio.eusage.rest.impl.EusageApiImpl;
import org.folio.tenant.rest.resource.DefaultApiHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerVerticle extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(HttpServerVerticle.class);
  private static final String TENANT_SPEC_FILE =
      "src/main/resources/openapi/tenant-2.0.yaml";
  private static final String EUSAGE_SPEC_FILE =
      "src/main/resources/openapi/eusage-reports-1.0.yaml";

  private final DefaultApiHandler defaultHandler = new DefaultApiHandler(new TenantApiImpl());
  private final org.folio.eusage.rest.resource.DefaultApiHandler eusageHandler =
      new org.folio.eusage.rest.resource.DefaultApiHandler(new EusageApiImpl());

  Future<Router> createTenantRouter() {
    return RouterBuilder.create(vertx, TENANT_SPEC_FILE)
        .map(builder -> {
          builder.setOptions(new RouterBuilderOptions()
              // For production use case, you need to enable this flag and provide
              // the proper security handler
              .setRequireSecurityHandlers(false)
          );

          defaultHandler.mount(builder);

          Router router = builder.createRouter();
          router.errorHandler(400, this::validationFailureHandler);
          return router;
        });
  }

  Future<Router> createEusageRouter() {
    return RouterBuilder.create(vertx, EUSAGE_SPEC_FILE)
        .map(builder -> {
          builder.setOptions(new RouterBuilderOptions()
              // For production use case, you need to enable this flag and provide
              // the proper security handler
              .setRequireSecurityHandlers(false)
          );
          eusageHandler.mount(builder);

          Router router = builder.createRouter();
          router.errorHandler(400, this::validationFailureHandler);
          return router;
        });
  }

  @Override
  public void start(Promise<Void> startPromise) {

    Router router = Router.router(vertx);

    router.route(HttpMethod.GET, "/admin/health").handler(ctx -> {
      ctx.response().putHeader("Content-Type", "text/plain");
      ctx.response().end("OK");
    });

    Future<Void> future = Future.succeededFuture();
    future = future.compose(x -> createTenantRouter())
        .onSuccess(x -> router.mountSubRouter("/", x)).mapEmpty();
    future = future.compose(x -> createEusageRouter())
        .onSuccess(x -> router.mountSubRouter("/", x)).mapEmpty();

    future = future.compose(x ->
      vertx.createHttpServer()
          .requestHandler(router)
          .listen(8080).mapEmpty()
    );
    future
        .onSuccess(server -> logger.info("Http verticle deploy successful"))
        .onFailure(t -> logger.error("Http verticle failed to deploy", t))
        // Complete the start promise
        .onComplete(startPromise);
  }

  private void validationFailureHandler(RoutingContext rc) {
    rc.response().setStatusCode(400)
        .end("Bad Request : " + rc.failure().getMessage());
  }
}
