package org.folio.eusage.reports.api;

import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Tuple;
import java.util.HashMap;
import java.util.Map;
import org.folio.tlib.postgres.TenantPgPool;

public class CounterReportContext {
  Map<String, Tuple> entries = new HashMap<>();

  final RoutingContext ctx;

  final TenantPgPool pool;

  CounterReportContext(RoutingContext ctx, TenantPgPool pool) {
    this.ctx = ctx;
    this.pool = pool;
  }

  void addErmTitle(String type, String identifier, Tuple value) {
    entries.put(type + "-" + identifier, value);
  }

  boolean containsErmTitle(String type, String identifier) {
    return entries.containsKey(type + "-" + identifier);
  }

  Tuple getErmTitle(String type, String identifier) {
    return entries.get(type + "-" + identifier);
  }
}
