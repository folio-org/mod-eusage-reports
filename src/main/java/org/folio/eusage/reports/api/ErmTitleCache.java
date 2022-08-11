package org.folio.eusage.reports.api;

import io.vertx.sqlclient.Tuple;
import java.util.HashMap;
import java.util.Map;

public class ErmTitleCache {
  Map<String, Tuple> entries = new HashMap<>();

  void add(String type, String identifier, Tuple value) {
    entries.put(type + "-" + identifier, value);
  }

  boolean contains(String type, String identifier) {
    return entries.containsKey(type + "-" + identifier);
  }

  Tuple get(String type, String identifier) {
    return entries.get(type + "-" + identifier);
  }
}
