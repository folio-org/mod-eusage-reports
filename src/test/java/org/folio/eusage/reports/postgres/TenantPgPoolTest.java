package org.folio.eusage.reports.postgres;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Tuple;
import java.util.LinkedList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

@RunWith(VertxUnitRunner.class)
public class TenantPgPoolTest {

  static Vertx vertx;

  private static PostgreSQLContainer<?> postgresSQLContainer;
  private static final PgConnectOptions pgConnectOptions = new PgConnectOptions();


  @BeforeClass
  public static void beforeClass() {
    vertx = Vertx.vertx();
    postgresSQLContainer = new PostgreSQLContainer<>("postgres:12-alpine");
    postgresSQLContainer.start();
    pgConnectOptions.setHost(postgresSQLContainer.getHost());
    pgConnectOptions.setPort(postgresSQLContainer.getFirstMappedPort());
    pgConnectOptions.setUser(postgresSQLContainer.getUsername());
    pgConnectOptions.setPassword(postgresSQLContainer.getPassword());
    pgConnectOptions.setDatabase(postgresSQLContainer.getDatabaseName());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    postgresSQLContainer.close();
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testDefault(TestContext context) {
    TenantPgPool.setModule(null);
    context.assertNull(TenantPgPool.module);
    TenantPgPool.setModule("a-b.c");
    context.assertEquals("a_b_c", TenantPgPool.module);
    TenantPgPool tenantPgPool = TenantPgPool.tenantPgPool(vertx, "diku");
    context.assertNotNull(tenantPgPool.pgPool);
    context.assertEquals("diku_a_b_c", tenantPgPool.getSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadModule() {
    TenantPgPool.setModule("mod'a");
  }

  @Test(expected = IllegalStateException.class)
  public void testNoSetModule() {
    TenantPgPool.setModule(null);
    TenantPgPool.tenantPgPool(vertx, "diku");
  }

  @Test
  public void testAll(TestContext context) {
    TenantPgPool.setModule("mod-a");
    TenantPgPool.host = "host_val";
    TenantPgPool.port = "9765";
    TenantPgPool.database = "database_val";
    TenantPgPool.user = "user_val";
    TenantPgPool.password = "password_val";
    TenantPgPool.maxPoolSize = "5";
    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");
    context.assertEquals("diku_mod_a", pool.getSchema());
    TenantPgPool.host = null;
    TenantPgPool.port = null;
    TenantPgPool.database = null;
    TenantPgPool.user = null;
    TenantPgPool.password = null;
    TenantPgPool.maxPoolSize = null;
  }

  @Test
  public void testUserDefined(TestContext context) {
    PgConnectOptions userDefined = new PgConnectOptions();
    userDefined.setHost("localhost2");
    TenantPgPool.setDefaultConnectOptions(userDefined);
    context.assertEquals(userDefined, TenantPgPool.pgConnectOptions);
    context.assertEquals("localhost2", userDefined.getHost());
    userDefined = new PgConnectOptions();
    TenantPgPool.setDefaultConnectOptions(userDefined);
    context.assertEquals(userDefined, TenantPgPool.pgConnectOptions);
    context.assertNotEquals("localhost2", userDefined.getHost());
  }

  @Test
  public void testPoolReuse(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool pool1 = TenantPgPool.tenantPgPool(vertx, "diku1");
    context.assertEquals("diku1_mod_a", pool1.getSchema());
    TenantPgPool pool2 = TenantPgPool.tenantPgPool(vertx, "diku2");
    context.assertEquals("diku2_mod_a", pool2.getSchema());
    context.assertNotEquals(pool1, pool2);
    context.assertEquals(pool1.pgPool, pool2.pgPool);
  }

  @Test
  public void queryOk(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);

    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");
    pool.query("SELECT count(*) FROM pg_database")
        .execute()
        .compose(x -> pool.close())
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void preparedQueryOk(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);

    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");
    pool.preparedQuery("SELECT * FROM pg_database WHERE datname=$1")
        .execute(Tuple.of("postgres"))
        .onComplete(context.asyncAssertSuccess(res ->
          pool.close(context.asyncAssertSuccess())
        ));
  }
  @Test
  public void getConnection1(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);

    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");
    pool.getConnection()
        .compose(con -> con.query("SELECT count(*) FROM pg_database")
            .execute()
            .eventually(c -> con.close()))
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void getConnection2(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");
    pool.getConnection(
        context.asyncAssertSuccess(
            con -> con.query("SELECT count(*) FROM pg_database")
                .execute()
                .eventually(c -> con.close())
                .onComplete(context.asyncAssertSuccess())));
  }

  @Test
  public void execute1(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");

    List<String> list = new LinkedList<>();
    list.add("CREATE TABLE a (year int)");
    list.add("SELECT * FROM a");
    list.add("DROP TABLE a");
    pool.execute(list).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void execute2(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");

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

}
