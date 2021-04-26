package org.folio.eusage.reports;

import io.restassured.RestAssured;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import org.folio.tenant.rest.impl.HttpServerVerticle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class HttpServerVerticleTest {

  Vertx vertx;
  WebClient webClient;
  int port = 9230;

  @Before
  public void setup(TestContext context) {
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.port = port;
    vertx = Vertx.vertx();
    webClient = WebClient.create(vertx);

    DeploymentOptions deploymentOptions = new DeploymentOptions();
    deploymentOptions.setConfig(new JsonObject().put("port", Integer.toString(port)));
    vertx.deployVerticle(new HttpServerVerticle(), deploymentOptions).onComplete(context.asyncAssertSuccess());
  }

  @After
  public void after(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testAdminHealth(TestContext context) {
    RestAssured.given()
        .get("/admin/health")
        .then().statusCode(200)
        .header("Content-Type", is("text/plain"));
  }

  @Test
  public void testPostTenantOK(TestContext context) {
    RestAssured.given()
        .header("Content-Type", "application/json")
        .body("{\"module_to\" : \"mod-eusage-reports-1.0.0\"}")
        .post("/_/tenant")
        .then().statusCode(201)
        .header("Content-Type", is("application/json; charset=utf-8"))
        .body("id", is("1234"));
  }

  @Test
  public void testPostTenant2(TestContext context) {
    webClient.post(port, "localhost","/_/tenant")
        .sendJsonObject(new JsonObject()
            .put("module_to", "mod-eusage-reports-1.0.0"))
        .compose(res -> {
          context.assertEquals(201, res.statusCode());
          return Future.succeededFuture();
        }).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testGetTenantOK(TestContext context) {
    RestAssured.given()
        .get("/_/tenant/121")
        .then().statusCode(200)
        .header("Content-Type", is("application/json; charset=utf-8"))
        .body("id", is("121"));
  }

  @Test
  public void testGetTenantOKWait100(TestContext context) {
    RestAssured.given()
        .get("/_/tenant/121?wait=100")
        .then().statusCode(200)
        .header("Content-Type", is("application/json; charset=utf-8"))
        .body("id", is("121"));
  }

  @Test
  public void testDeleteTenantOK(TestContext context) {
    RestAssured.given()
        .delete("/_/tenant/121")
        .then().statusCode(204);
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

  @Test
  public void testEUsageVersionOK(TestContext context) {
    RestAssured.given()
        .get("/eusage/version")
        .then().statusCode(200)
        .body(containsString("0.0"));
  }
}
