package org.folio.tenant.rest.impl;

import io.swagger.annotations.Api;
import io.vertx.core.Future;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.tenant.main.TenantAttributes;
import org.folio.tenant.main.TenantJob;
import org.folio.tenant.rest.ApiResponse;
import org.folio.tenant.rest.resource.DefaultApi;

// Implement this class

public class TenantApiImpl implements DefaultApi {

  /**
   * delete tenant job.
   * @param id job identifier.
   * @return api result.
   */
  public Future<ApiResponse<Void>> deleteTenantJob(String id) {
    return Future.succeededFuture(new ApiResponse<>(204));
  }

  /**
   * get tenant job.
   * @param id job identifier.
   * @param wait wait in milliconds for job change; 0 for no wait.
   * @return api result.
   */
  public Future<ApiResponse<TenantJob>> getTenantJob(String id, Integer wait) {
    TenantJob tenantJob = new TenantJob(id, "testlib", null,
        false, null, null);
    return Future.succeededFuture(new ApiResponse<>(200, tenantJob));
  }

  /**
   * create tenant job.
   * @param tenantAttributes attributes for job.
   * @return api result.
   */
  public Future<ApiResponse<TenantJob>> postTenant(TenantAttributes tenantAttributes) {
    TenantJob tenantJob = new TenantJob("1234", "testlib", null,
        false, null, null);
    return Future.succeededFuture(new ApiResponse<>(201, tenantJob));
  }

}
