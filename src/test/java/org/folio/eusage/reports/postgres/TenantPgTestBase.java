package org.folio.eusage.reports.postgres;

import io.vertx.pgclient.PgConnectOptions;
import org.testcontainers.containers.PostgreSQLContainer;

public class TenantPgTestBase {

  public static final PostgreSQLContainer<?> postgresSQLContainer;

  static {
    postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine");
    postgresSQLContainer.start();
    TenantPgPool.setDefaultConnectOptions(getPgConnectOptions());
  }

  private static PgConnectOptions getPgConnectOptions() {
    return new PgConnectOptions()
        .setPort(postgresSQLContainer.getFirstMappedPort())
        .setHost(postgresSQLContainer.getHost())
        .setDatabase(postgresSQLContainer.getDatabaseName())
        .setUser(postgresSQLContainer.getUsername())
        .setPassword(postgresSQLContainer.getPassword());
  }

}
