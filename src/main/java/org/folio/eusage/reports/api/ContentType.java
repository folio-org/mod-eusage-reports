package org.folio.eusage.reports.api;

import io.vertx.core.MultiMap;

public class ContentType {
  private boolean csv;
  private boolean full;
  private String responseContentType;

  ContentType(boolean csv) {
    this.csv = csv;
    full = true;
  }

  ContentType(MultiMap params) {
    csv = false;
    full = true;
    String contentType = params.get("contentType");
    if (contentType == null) {
      contentType = "true".equalsIgnoreCase(params.get("csv")) ? "text/csv" : "application/json";
    }
    responseContentType = contentType;
    switch (contentType) {
      case "application/summary+json":
        full = false;
        break;
      case "application/json":
      case "application/full+json":
        break;
      case "text/full+csv":
      case "text/csv":
        csv = true;
        break;
      case "text/summary+csv":
        full = false;
        csv = true;
        break;
      default:
        throw new IllegalArgumentException("Bad value for contentType: " + contentType);
    }
  }

  String getContentType() {
    return responseContentType;
  }

  boolean isFull() {
    return full;
  }

  boolean isCsv() {
    return csv;
  }

}
