package org.folio.eusage.reports;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.eusage.reports.api.EusageReportsApi;
import org.folio.okapi.common.Config;
import org.folio.okapi.common.ModuleVersionReporter;
import org.folio.tlib.RouterCreator;
import org.folio.tlib.api.HealthApi;
import org.folio.tlib.api.Tenant2Api;
import org.folio.tlib.postgres.TenantPgPool;

public class MainVerticle extends AbstractVerticle {
  final Logger log = LogManager.getLogger(MainVerticle.class);

  @Override
  public void start(Promise<Void> promise) {
    TenantPgPool.setModule("mod-eusage-reports");
    ModuleVersionReporter m = new ModuleVersionReporter("org.folio/mod-eusage-reports");
    log.info("Starting {} {} {}", m.getModule(), m.getVersion(), m.getCommitId());

    final int port = Integer.parseInt(
        Config.getSysConf("http.port", "port", "8081", config()));

    EusageReportsApi eusageReportsApi = new EusageReportsApi();
    RouterCreator [] routerCreators = {
        eusageReportsApi,
        new Tenant2Api(eusageReportsApi),
        new HealthApi(),
    };

    RouterCreator.mountAll(vertx, WebClient.create(vertx), routerCreators)
        .compose(router -> {
          HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
          return vertx.createHttpServer(so)
              .requestHandler(router)
              .listen(port).mapEmpty();
        })
        .onComplete(x -> promise.handle(x.mapEmpty()));
  }
}
