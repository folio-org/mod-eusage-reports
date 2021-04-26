package org.folio.eusage.rest.impl;

import io.vertx.core.Future;
import org.folio.eusage.rest.ApiResponse;
import org.folio.eusage.rest.resource.DefaultApi;

// Implement this class

public class EusageApiImpl implements DefaultApi {
  public Future<ApiResponse<String>> getVersion() {
    return Future.succeededFuture(new ApiResponse<>(200, "0.0"));
  }

}
