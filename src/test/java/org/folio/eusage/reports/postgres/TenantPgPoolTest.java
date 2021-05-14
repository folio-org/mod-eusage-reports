package org.folio.eusage.reports.postgres;

import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class TenantPgPoolTest {

  Vertx vertx;

  @Before
  public void setup(TestContext context) {
    vertx = Vertx.vertx();
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

  @Test
  public void testAll(TestContext context) {
    TenantPgPool.setModule("mod_a");
    TenantPgPool.host = "host_val";
    TenantPgPool.port = "9765";
    TenantPgPool.database = "database_val";
    TenantPgPool.user = "user_val";
    TenantPgPool.password = "password_val";
    TenantPgPool pool = TenantPgPool.tenantPgPool(vertx, "diku");
    context.assertEquals("diku_mod_a", pool.getSchema());
    TenantPgPool.host = null;
    TenantPgPool.port = null;
    TenantPgPool.database = null;
    TenantPgPool.user = null;
    TenantPgPool.password = null;
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
}
