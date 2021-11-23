package org.folio.eusage.reports;

import io.vertx.pgclient.PgConnectOptions;
import org.folio.tlib.postgres.TenantPgPool;
import org.testcontainers.containers.PostgreSQLContainer;

public final class CreateContainer {

  public static PostgreSQLContainer<?> create() {
    return create("postgres:12-alpine");
  }

  public static PostgreSQLContainer<?> create(String image) {
    PostgreSQLContainer<?> postgresSQLContainer = new PostgreSQLContainer<>(image);
    postgresSQLContainer.start();

    TenantPgPool.setDefaultConnectOptions(new PgConnectOptions()
        .setPort(postgresSQLContainer.getFirstMappedPort())
        .setHost(postgresSQLContainer.getHost())
        .setDatabase(postgresSQLContainer.getDatabaseName())
        .setUser(postgresSQLContainer.getUsername())
        .setPassword(postgresSQLContainer.getPassword()));
    return postgresSQLContainer;
  }
}
