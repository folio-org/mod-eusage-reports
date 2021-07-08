package org.folio.tlib.api;

import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import io.vertx.pgclient.PgConnectOptions;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.TenantPgPoolContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class Tenant2ApiTest {
  private final static Logger log = LogManager.getLogger(Tenant2ApiTest.class);

  static Vertx vertx;
  static int port = 9230;

  private static int getFreePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      serverSocket.setReuseAddress(true);
      return serverSocket.getLocalPort();
    }
  }

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  static class TenantInitHooks implements org.folio.tlib.TenantInitHooks {

    Promise<Void> preInitPromise;
    Promise<Void> postInitPromise;

    @Override
    public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
      postInitPromise = Promise.promise();
      if (preInitPromise == null) {
        return Future.succeededFuture();
      }
      Future<Void> future = preInitPromise.future();
      preInitPromise = null;
      return future;
    }

    @Override
    public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
      return postInitPromise.future();
    }
  }

  static TenantInitHooks hooks = new TenantInitHooks();

  @BeforeClass
  public static void beforeClass(TestContext context) {
    TenantPgPool.setModule("mod-tenant");
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = port;

    new Tenant2Api(hooks).createRouter(vertx, WebClient.create(vertx))
        .compose(router -> {
          HttpServerOptions so = new HttpServerOptions().setHandle100ContinueAutomatically(true);
          return vertx.createHttpServer(so)
              .requestHandler(router)
              .listen(port).mapEmpty();
        })
        .onComplete(context.asyncAssertSuccess());
  }

  @AfterClass
  public static void afterClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testPostTenantBadTenant1(TestContext context) {
    String tenant = "test'lib";
    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is(tenant));
  }

  @Test
  public void testPostTenantBadTenant2(TestContext context) {
    String tenant = "test\"lib";
    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(is(tenant));
  }

  @Test
  public void testPostTenantBadPort(TestContext context) throws IOException {
    String tenant = "testlib";
    PgConnectOptions bad = new PgConnectOptions();
    PgConnectOptions pgConnectOptions = TenantPgPool.getDefaultConnectOptions();
    bad.setHost(pgConnectOptions.getHost());
    bad.setPort(getFreePort());
    bad.setUser(pgConnectOptions.getUser());
    bad.setPassword(pgConnectOptions.getPassword());
    bad.setDatabase(pgConnectOptions.getDatabase());
    TenantPgPool.setDefaultConnectOptions(bad);
    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("DB_PORT="));
    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
  }

  @Test
  public void testPostTenantBadDatabase(TestContext context) {
    String tenant = "testlib";
    PgConnectOptions bad = new PgConnectOptions();
    PgConnectOptions pgConnectOptions = TenantPgPool.getDefaultConnectOptions();
    bad.setHost(pgConnectOptions.getHost());
    bad.setPort(pgConnectOptions.getPort());
    bad.setUser(pgConnectOptions.getUser());
    bad.setPassword(pgConnectOptions.getPassword());
    bad.setDatabase(pgConnectOptions.getDatabase() + "_foo");
    TenantPgPool.setDefaultConnectOptions(bad);
    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(500)
        .header("Content-Type", is("text/plain"))
        .body(containsString("database"));

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get("/_/tenant/" + UUID.randomUUID().toString())
        .then().statusCode(500)
        .header("Content-Type", is("text/plain"))
        .body(containsString("database"));

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete("/_/tenant/" + UUID.randomUUID().toString())
        .then().statusCode(500)
        .header("Content-Type", is("text/plain"))
        .body(containsString("database"));

    TenantPgPool.setDefaultConnectOptions(pgConnectOptions);
  }

  @Test
  public void testPostTenantOK(TestContext context) {
    String tenant = "testlib";
    log.info("AD: POST begin");

    // init
    String error = tenantOp(context, tenant, new JsonObject()
        .put("module_to", "mod-eusage-reports-1.0.0")
    );
    context.assertNull(error);

    // upgrade
    error = tenantOp(context, tenant, new JsonObject()
        .put("module_from", "mod-eusage-reports-1.0.0")
        .put("module_to", "mod-eusage-reports-1.0.1")
    );
    context.assertNull(error);

    // disable
    error = tenantOp(context, tenant, new JsonObject()
        .put("module_from", "mod-eusage-reports-1.0.1")
    );
    context.assertNull(error);

    // purge
    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("module_from", "mod-eusage-reports-1.0.1")
            .put("purge", true)
            .encode())
        .post("/_/tenant")
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body(new JsonObject()
            .put("module_from", "mod-eusage-reports-1.0.1")
            .put("purge", true)
            .encode())
        .post("/_/tenant")
        .then().statusCode(204);
  }

  @Test
  public void testPostTenantPreInitFail(TestContext context) {
    String tenant = "testlib";
    hooks.preInitPromise = Promise.promise();
    hooks.preInitPromise.fail("pre init failure");
    ExtractableResponse<Response> response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(500)
        .header("Content-Type", is("text/plain"))
        .extract();

    context.assertEquals("pre init failure", response.body().asString());
  }

  String tenantOp(TestContext context, String tenant, JsonObject body) {
    log.info("AD: POST begin");
    ExtractableResponse<Response> response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body(body.encode())
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    log.info("AD: POST completed");
    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location)
        .then().statusCode(200)
        .extract();
    boolean complete = response.path("complete");
    context.assertFalse(complete);
    while (!complete) {
      response = RestAssured.given()
          .header("X-Okapi-Tenant", tenant)
          .get(location + "?wait=1")
          .then().statusCode(200)
          .extract();
      complete = response.path("complete");
      hooks.postInitPromise.tryComplete();
    }

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(404);
    return response.path("error");
  }

  @Test
  public void testPostTenantPostInitFail(TestContext context) {
    String tenant = "testlib";
    ExtractableResponse<Response> response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    hooks.postInitPromise.fail("post init failure");
    String s = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location)
        .then().statusCode(200)
        .extract().body().asString();
    JsonObject tenantJob2 = new JsonObject(s);
    context.assertTrue(tenantJob2.getBoolean("complete"));
    context.assertEquals("post init failure", tenantJob2.getString("error"));

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\", \"purge\":true}")
        .post("/_/tenant")
        .then().statusCode(204);
  }

  @Test
  public void testPostTenantPostInitFailNull(TestContext context) {
    String tenant = "testlib";
    ExtractableResponse<Response> response = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json"))
        .body("tenant", is(tenant))
        .extract();

    String location = response.header("Location");
    JsonObject tenantJob = new JsonObject(response.asString());
    context.assertEquals("/_/tenant/" + tenantJob.getString("id"), location);

    hooks.postInitPromise.fail((String) null);
    String s = RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location)
        .then().statusCode(200)
        .extract().body().asString();
    JsonObject tenantJob2 = new JsonObject(s);
    context.assertTrue(tenantJob2.getBoolean("complete"));
    context.assertTrue(tenantJob2.containsKey("error"));
    context.assertEquals("io.vertx.core.impl.NoStackTraceThrowable", tenantJob2.getString("error"));

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\", \"purge\":true}")
        .post("/_/tenant")
        .then().statusCode(204);
  }

  @Test
  public void testPostMissingTenant(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetMissingTenant(TestContext context){
    String id = UUID.randomUUID().toString();
    RestAssured.given()
        .get("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testGetBadId(TestContext context){
    String id = "1234";
    RestAssured.given()
        .header("X-Okapi-Tenant", "testlib")
        .get("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Invalid UUID string"));
  }

  @Test
  public void testDeleteMissingTenant(TestContext context){
    String id = UUID.randomUUID().toString();
    RestAssured.given()
        .delete("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testDeleteBadId(TestContext context){
    String id = "1234";
    RestAssured.given()
        .header("X-Okapi-Tenant", "testlib")
        .delete("/_/tenant/" + id)
        .then().statusCode(400)
        .header("Content-Type", is("text/plain"))
        .body(containsString("Invalid UUID string"));
  }

  @Test
  public void testPostTenantBadJson(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"")
        .post("/_/tenant")
        .then().statusCode(400);
  }

  @Test
  public void testPostTenantBadType(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : true}")
        .post("/_/tenant")
        .then().statusCode(400);
  }

  @Test
  public void testPostTenantAdditional(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\", \"extra\":true}")
        .post("/_/tenant")
        .then().statusCode(400);
  }

}
