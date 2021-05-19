package org.folio.tlib.postgres.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.TenantPgPool;

public class TenantPgPoolImpl implements TenantPgPool {

  private static final Logger log = LogManager.getLogger(TenantPgPoolImpl.class);
  static Map<PgConnectOptions, PgPool> pgPoolMap = new HashMap<>();

  static String host = System.getProperty("DB_HOST");
  static String port = System.getProperty("DB_PORT");
  static String user = System.getProperty("DB_USERNAME");
  static String password = System.getProperty("DB_PASSWORD");
  static String database = System.getProperty("DB_DATABASE");
  static String maxPoolSize = System.getProperty("DB_MAXPOOLSIZE");
  static String serverPem = System.getProperty("DB_SERVER_PEM");
  static String module;
  static PgConnectOptions pgConnectOptions = new PgConnectOptions();

  String tenant;
  PgPool pgPool;

  static String substTenant(String v, String tenant) {
    return v.replace("{tenant}", tenant);
  }

  static String sanitize(String v) {
    if (v.contains("'") || v.contains("\"")) {
      throw new IllegalArgumentException(v);
    }
    return v.replace("-", "_").replace(".", "_");
  }

  public static PgConnectOptions getDefaultConnectOptions() {
    return TenantPgPoolImpl.pgConnectOptions;
  }

  public static void setDefaultConnectOptions(PgConnectOptions connectOptions) {
    TenantPgPoolImpl.pgConnectOptions = connectOptions;
  }

  public static void setModule(String module) {
    TenantPgPoolImpl.module = module != null ? sanitize(module) : null;
  }

  public static void setServerPem(String serverPem) {
    pgConnectOptions.setSslMode(SslMode.DISABLE);
    TenantPgPoolImpl.serverPem = serverPem;
  }

  public static void setMaxPoolSize(String maxPoolSize) {
    TenantPgPoolImpl.maxPoolSize = maxPoolSize;
  }

  public String getSchema() {
    return tenant + "_" + module;
  }

  private TenantPgPoolImpl() {
  }

  /**
   * Create pool for Tenant.
   *
   * <p>The returned pool implements PgPool interface so this cab be used like PgPool as usual.
   * But queries being substituted before usage. The literal "{schema}" is substituted with the
   * module+schema schema.
   * PgPool.setMmodule *must* be called before the queries are executed, since schema is based
   * on module name.
   * @param vertx Vert.x handle
   * @param tenant Tenant
   * @return pool with PgPool semantics
   */
  public static TenantPgPoolImpl tenantPgPool(Vertx vertx, String tenant) {
    if (module == null) {
      throw new IllegalStateException("TenantPgPool.setModule must be called");
    }
    PgConnectOptions connectOptions = pgConnectOptions;
    if (host != null) {
      connectOptions.setHost(substTenant(host, tenant));
    }
    if (port != null) {
      connectOptions.setPort(Integer.parseInt(port));
    }
    if (user != null) {
      connectOptions.setUser(substTenant(user, tenant));
    }
    if (password != null) {
      connectOptions.setPassword(password);
    }
    if (database != null) {
      connectOptions.setDatabase(substTenant(database, tenant));
    }
    if (serverPem != null) {
      connectOptions.setSslMode(SslMode.VERIFY_FULL);
      connectOptions.setHostnameVerificationAlgorithm("HTTPS");
      connectOptions.setPemTrustOptions(
          new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)));
      connectOptions.setEnabledSecureTransportProtocols(Collections.singleton("TLSv1.3"));
      connectOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
    }
    TenantPgPoolImpl tenantPgPool = new TenantPgPoolImpl();
    tenantPgPool.tenant = sanitize(tenant);
    tenantPgPool.pgPool = pgPoolMap.computeIfAbsent(connectOptions, key -> {
      PoolOptions poolOptions = new PoolOptions();
      if (maxPoolSize != null) {
        poolOptions.setMaxSize(Integer.parseInt(maxPoolSize));
      }
      return PgPool.pool(vertx, connectOptions, poolOptions);
    });
    return tenantPgPool;
  }

  @Override
  public void getConnection(Handler<AsyncResult<SqlConnection>> handler) {
    pgPool.getConnection(handler);
  }

  @Override
  public Future<SqlConnection> getConnection() {
    return pgPool.getConnection();
  }

  private String subst(String s) {
    return s.replace("{schema}", getSchema());
  }

  @Override
  public Query<RowSet<Row>> query(String s) {
    return pgPool.query(subst(s));
  }

  @Override
  public PreparedQuery<RowSet<Row>> preparedQuery(String s) {
    return pgPool.preparedQuery(subst(s));
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    // release our pool from the map
    while (pgPoolMap.values().remove(pgPool)) { }
    pgPool.close(handler);
  }

  @Override
  public Future<Void> close() {
    // release our pool from the map
    while (pgPoolMap.values().remove(pgPool)) { }
    return pgPool.close();
  }

  /**
   * Execute a list of queries.
   * @param queries executed in order; processing is stopped if any queries fail.
   * @return async result.
   */
  @Override
  public Future<Void> execute(List<String> queries) {
    Future<Void> future = Future.succeededFuture();
    for (String cmd : queries) {
      future = future.compose(res -> query(cmd).execute()
          .onSuccess(x -> log.info("{}", cmd))
          .onFailure(x -> log.warn("{} FAIL: {}", cmd, x.getMessage()))
          .mapEmpty());
    }
    return future;
  }

}
