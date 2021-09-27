package org.folio.eusage.reports.api;

import io.vertx.core.MultiMap;

public class ContentType {
  private boolean csv;
  private boolean full;

  ContentType(boolean csv) {
    this.csv = csv;
    full = true;
  }

  ContentType(MultiMap params) {
    csv = "true".equalsIgnoreCase(params.get("csv"));
    full = !"false".equalsIgnoreCase(params.get("full"));
  }

  String getContentType() {
    return csv ? "text/csv" : "application/json";
  }

  boolean isFull() {
    return full;
  }

  boolean isCsv() {
    return csv;
  }

}
