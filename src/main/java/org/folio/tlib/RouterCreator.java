package org.folio.tlib;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

public interface RouterCreator {

  Future<Router> createRouter(Vertx vertx);
}