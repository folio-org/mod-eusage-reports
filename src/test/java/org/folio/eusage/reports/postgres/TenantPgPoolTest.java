package org.folio.eusage.reports.postgres;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Tuple;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.eusage.reports.postgres.impl.TenantPgPoolImpl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

@RunWith(VertxUnitRunner.class)
public class TenantPgPoolTest {
  private final static Logger log = LogManager.getLogger("MainVerticleTest");

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  static final String KEY_PATH = "/var/lib/postgresql/data/server.key";
  static final String CRT_PATH = "/var/lib/postgresql/data/server.crt";
  static final String CONF_PATH = "/var/lib/postgresql/data/postgresql.conf";
  static final String CONF_BAK_PATH = "/var/lib/postgresql/data/postgresql.conf.bak";

  static Vertx vertx;

  private static PgConnectOptions pgConnectOptions;

  // execute commands in container (stolen from Okapi's PostgresHandleTest
  static void exec(String... command) {
    try {
      Container.ExecResult execResult = postgresSQLContainer.execInContainer(command);
      if (execResult.getExitCode() != 0) {
        log.info("{} {}", execResult.getExitCode(), String.join(" ", command));
        log.info("stderr: {}", execResult.getStderr());
        log.info("stdout: {}", execResult.getStdout());
      }
    } catch (InterruptedException|IOException|UnsupportedOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Append each entry to postgresql.conf and reload it into postgres.
   * Appending a key=value entry has precedence over any previous entries of the same key.
   * Stolen from Okapi's PostgresHandleTest
   */
  static void configure(String... configEntries) {
    exec("cp", "-p", CONF_BAK_PATH, CONF_PATH);  // start with unaltered config
    for (String configEntry : configEntries) {
      exec("sh", "-c", "echo '" + configEntry + "' >> " + CONF_PATH);
    }
    exec("su-exec", "postgres", "pg_ctl", "reload");
  }

  @BeforeClass
  public static void beforeClass() {
    vertx = Vertx.vertx();

    MountableFile serverKeyFile = MountableFile.forClasspathResource("server.key");
    MountableFile serverCrtFile = MountableFile.forClasspathResource("server.crt");
    postgresSQLContainer.copyFileToContainer(serverKeyFile, KEY_PATH);
    postgresSQLContainer.copyFileToContainer(serverCrtFile, CRT_PATH);
    exec("chown", "postgres.postgres", KEY_PATH, CRT_PATH);
    exec("chmod", "400", KEY_PATH, CRT_PATH);
    exec("cp", "-p", CONF_PATH, CONF_BAK_PATH);

    pgConnectOptions = TenantPgPool.getDefaultConnectOptions();
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Before
  public void before() {
    TenantPgPoolImpl.setServerPem(null);
    TenantPgPoolImpl.setModule("mod-a");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadModule() {
    TenantPgPoolImpl.setModule("mod'a");
  }

  @Test(expected = IllegalStateException.class)
  public void testNoSetModule() {
    TenantPgPoolImpl.setModule(null);
    TenantPgPoolImpl.tenantPgPool(vertx, "diku");
  }

  @Test
  public void queryOk(TestContext context) {
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");
    pool.query("SELECT count(*) FROM pg_database")
        .execute()
        .compose(x -> pool.close())
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void preparedQueryOk(TestContext context) {
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");
    pool.preparedQuery("SELECT * FROM pg_database WHERE datname=$1")
        .execute(Tuple.of("postgres"))
        .onComplete(context.asyncAssertSuccess(res ->
          pool.close(context.asyncAssertSuccess())
        ));
  }
  @Test
  public void getConnection1(TestContext context) {
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");
    pool.getConnection()
        .compose(con -> con.query("SELECT count(*) FROM pg_database")
            .execute()
            .eventually(c -> con.close()))
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void getConnection2(TestContext context) {
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");
    pool.getConnection(
        context.asyncAssertSuccess(
            con -> con.query("SELECT count(*) FROM pg_database")
                .execute()
                .eventually(c -> con.close())
                .onComplete(context.asyncAssertSuccess())));
  }

  @Test
  public void execute1(TestContext context) {
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");

    List<String> list = new LinkedList<>();
    list.add("CREATE TABLE a (year int)");
    list.add("SELECT * FROM a");
    list.add("DROP TABLE a");
    pool.execute(list).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void execute2(TestContext context) {
    log.info("SSL={}", pgConnectOptions.getSslMode());
    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");

    // execute not using a transaction as this test shows.
    List<String> list = new LinkedList<>();
    list.add("CREATE TABLE a (year int)");
    list.add("SELECT * FROM a");
    list.add("DROP TABLOIDS a"); // fails
    pool.execute(list).onComplete(context.asyncAssertFailure(c -> {
      List<String> list2 = new LinkedList<>();
      list2.add("DROP TABLE a"); // better now
      pool.execute(list2).onComplete(context.asyncAssertSuccess());
    }));
  }

  @Test
  public void testSSL(TestContext context) throws IOException {
    configure("ssl=on");
    TenantPgPool.setMaxPoolSize("3");
    TenantPgPool.setServerPem(new String(TenantPgPoolTest.class.getClassLoader()
        .getResourceAsStream("server.crt").readAllBytes()));

    TenantPgPool pool = TenantPgPool.pool(vertx, "diku");
    pool.query("SELECT version FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .execute()
        .onComplete(context.asyncAssertSuccess(rowSet -> {
          context.assertEquals("TLSv1.3", rowSet.iterator().next().getString(0));
          pool.close(context.asyncAssertSuccess(res -> configure("ssl=off")));
        }));
  }

}
