package org.folio.eusage.reports.api;

import static io.restassured.RestAssured.given;
import io.restassured.RestAssured;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.eusage.reports.MainVerticle;
import org.folio.tlib.postgres.TenantPgPoolContainer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class UseOverTimeApiTest {

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  private static Vertx vertx;
  private UUID a1 = UUID.randomUUID();
  private UUID a2 = UUID.randomUUID();

  @BeforeClass
  public static void setUp(TestContext context) {
    RestAssured.baseURI = "http://localhost:8081";
    vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle(), context.asyncAssertSuccess(x -> {
      postTenant(context.asyncAssertSuccess());
    }));
  }

  private static Future<Void> postTenant() {
    String location =
        given().
          header("X-Okapi-Tenant", "diku").
          header("Content-Type", "application/json").
          body("{}").
        when().post("/_/tenant").
        then().statusCode(201).
        extract().header("Location");

    location += "?wait=1";

    for (int i=0; i<10; i++) {
      boolean complete =
          given().header("X-Okapi-Tenant", "diku").
          when().get(location).
          then().statusCode(200).
          extract().path("complete");
      if (complete) {
        return Future.succeededFuture();
      }
    };

    return Future.failedFuture("postTenant timeout");
  }

  private static void postTenant(Handler<AsyncResult<Void>> resultHandler) {
    // run blocking RestAssured on worker pool thread in parallel to the MainVerticle thread
    vertx.executeBlocking(promise -> promise.handle(postTenant()), resultHandler);
  }

  @Test
  public void testPopulateAgreementLine(TestContext context) {
    var useOverTimeApi = new UseOverTimeApi();
   //  useOverTimeApi.
  }

}
