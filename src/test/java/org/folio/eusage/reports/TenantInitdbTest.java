package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnectOptions;
import java.util.UUID;
import org.folio.eusage.reports.postgres.TenantPgPool;
import org.folio.eusage.reports.postgres.TenantPgTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class TenantInitdbTest extends TenantPgTestBase {

  static Vertx vertx;
  static int port = 9230;

  static class TenantInitHooks implements TenantInit {
    @Override
    public Future<Void> preInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
      return Future.succeededFuture();
    }

    @Override
    public Future<Void> postInit(Vertx vertx, String tenant, JsonObject tenantAttributes) {
      return Future.succeededFuture();
    }
  }

  static TenantInitHooks hooks = new TenantInitHooks();

  @BeforeClass
  public static void beforeClass(TestContext context) {
    TenantPgPool.setModule("mod-tenant");
    vertx = Vertx.vertx();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = port;

    new TenantInitDb(hooks).createRouter(vertx)
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
    postgresSQLContainer.close();
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

    Boolean complete = false;
    while (!complete) {
      complete = RestAssured.given()
          .header("X-Okapi-Tenant", tenant)
          .get(location + "?wait=100")
          .then().statusCode(200)
          .extract().path("complete");
    }

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(204);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .delete(location)
        .then().statusCode(404);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .get(location)
        .then().statusCode(404);

    RestAssured.given()
        .header("X-Okapi-Tenant", tenant)
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\", \"purge\":true}")
        .post("/_/tenant")
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
