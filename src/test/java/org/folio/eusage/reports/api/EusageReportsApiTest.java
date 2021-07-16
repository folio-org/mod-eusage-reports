package org.folio.eusage.reports.api;

import static org.folio.eusage.reports.api.EusageReportsApi.agreementEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.packageEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleDataTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleEntriesTable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.TenantPgPoolContainer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.testcontainers.containers.PostgreSQLContainer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EusageReportsApiTest {

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @ClassRule
  public static RunTestOnContext runTestOnContext = new RunTestOnContext();

  @ClassRule
  public static PostgreSQLContainer<?> postgresSQLContainer = TenantPgPoolContainer.create();

  private static Vertx vertx;
  private static TenantPgPool pool;
  private static String tenant = "tenant";

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = runTestOnContext.vertx();
    TenantPgPool.setModule("mod-eusage-reports-api-test");
    pool = TenantPgPool.pool(vertx, tenant);
    pool.execute(List.of(
        "DROP SCHEMA IF EXISTS {schema} CASCADE",
        "DROP ROLE IF EXISTS {schema}",
        "CREATE ROLE {schema} PASSWORD 'tenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN",
        "GRANT {schema} TO CURRENT_USER",
        "CREATE SCHEMA {schema} AUTHORIZATION {schema}"
    )).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testPopulateAgreementLine(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    UUID agreementId = UUID.randomUUID();

    api.populateAgreementLine( null, null, new JsonObject(), agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertTrue(x.getMessage().contains("Failed to decode agreement line:"), x.getMessage())));
  }

  @Test
  public void useOverTimeStartDateAfterEndDate() {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.request().getHeader("X-Okapi-Tenant")).thenReturn("foo");
    when(ctx.request().params().get("startDate")).thenReturn("2020-04-01");
    when(ctx.request().params().get("endDate")).thenReturn("2020-02-01");
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
        new EusageReportsApi().getUseOverTime(vertx, ctx));
    assertThat(t.getMessage(), is("startDate=2020-04-01 is after endDate=2020-02-01"));
  }

  // agreementId
  String a1  = "10000000-0000-4000-8000-000000000000";
  String a2  = "20000000-0000-4000-8000-000000000000";
  // kbTitleId
  String t11 = "11000000-0000-4000-8000-000000000000";
  String t12 = "12000000-0000-4000-8000-000000000000";
  String t21 = "21000000-0000-4000-8000-000000000000";
  String t22 = "22000000-0000-4000-8000-000000000000";
  // kbPackageId
  String p11 = "1100000a-0000-4000-8000-000000000000";
  String p12 = "1200000a-0000-4000-8000-000000000000";
  String p21 = "2100000a-0000-4000-8000-000000000000";
  String p22 = "2200000a-0000-4000-8000-000000000000";
  // titleEntryId
  String te11 = "1100000e-0000-4000-8000-000000000000";
  String te12 = "1200000e-0000-4000-8000-000000000000";
  String te21 = "2100000e-0000-4000-8000-000000000000";
  String te22 = "2200000e-0000-4000-8000-000000000000";

  private Future<RowSet<Row>> insertAgreement(String agreementId, String titleId, String packageId) {
    return pool.preparedQuery("INSERT INTO " + agreementEntriesTable(pool)
        + "(id, agreementId, kbTitleId, kbPackageId) VALUES ($1, $2, $3, $4)")
    .execute(Tuple.of(UUID.randomUUID(), agreementId, titleId, packageId));
  }

  private Future<RowSet<Row>> insertPackageEntry(String packageId, String packageName, String titleId) {
    return pool.preparedQuery("INSERT INTO " + packageEntriesTable(pool)
        + "(kbPackageId, kbPackageName, kbTitleId) VALUES ($1, $2, $3)")
    .execute(Tuple.of(packageId, packageName, titleId));
  }

  private Future<RowSet<Row>> insertTitleEntry(String titleEntryId,
      String titleName, String titleId) {
    return pool.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
        + "(id, kbTitleName, kbTitleId) VALUES ($1, $2, $3)")
    .execute(Tuple.of(titleEntryId, titleName, titleId));
  }

  private Future<RowSet<Row>> insertTitleData(String titleEntryId,
      String dateStart, String dateEnd, int uniqueAccessCount, int totalAccessCount) {
    return pool.preparedQuery("INSERT INTO " + titleDataTable(pool)
        + "(id, titleEntryId, usageDateRange, uniqueAccessCount, totalAccessCount) "
        + "VALUES ($1, $2, daterange($3::text::date, $4::text::date), $5, $6)")
    .execute(Tuple.of(UUID.randomUUID(), titleEntryId, dateStart, dateEnd, uniqueAccessCount, totalAccessCount));
  }

  private static List<String> deepToString(RowSet<Row> rowSet) {
    List<String> rows = new ArrayList<>();
    rowSet.forEach(row -> rows.add(row.deepToString()));
    return rows;
  }

  @Test
  public void useOverTimeSql(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    api.postInit(vertx, tenant, new JsonObject().put("module_to", "1.1.1"))
    .compose(x -> insertAgreement(a1, t11, p11))
    .compose(x -> insertAgreement(a1, t12, p12))
    .compose(x -> insertAgreement(a2, t21, p21))
    .compose(x -> insertAgreement(a2, t22, p22))
    .compose(x -> insertPackageEntry(p11, "Package 11", t11))
    .compose(x -> insertPackageEntry(p12, "Package 12", t21))
    .compose(x -> insertPackageEntry(p21, "Package 21", t12))
    .compose(x -> insertPackageEntry(p22, "Package 22", t22))
    .compose(x -> insertTitleEntry(te11, "Title 11", t11))
    .compose(x -> insertTitleEntry(te12, "Title 12", t12))
    .compose(x -> insertTitleEntry(te21, "Title 21", t21))
    .compose(x -> insertTitleEntry(te22, "Title 22", t22))
    .compose(x -> insertTitleData(te11, "2020-03-01", "2020-04-01", 1, 2))
    .compose(x -> insertTitleData(te11, "2020-04-01", "2020-04-15", 2, 3))
    .compose(x -> insertTitleData(te11, "2020-04-15", "2020-05-01", 3, 3))
    .compose(x -> insertTitleData(te11, "2020-05-01", "2020-06-01", 4, 12))
    .compose(x -> insertTitleData(te12, "2020-03-01", "2020-04-01", 11, 12))
    .compose(x -> insertTitleData(te12, "2020-04-01", "2020-05-01", 15, 16))
    .compose(x -> insertTitleData(te12, "2020-05-01", "2020-06-01", 14, 22))
    .compose(x -> insertTitleData(te21, "2020-03-01", "2020-04-01", 0, 0))
    .compose(x -> insertTitleData(te21, "2020-05-01", "2020-06-01", 20, 40))
    .compose(x -> api.journal(pool, a1, LocalDate.parse("2020-04-01"), LocalDate.parse("2020-05-01")))
    .onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(deepToString(rowSet), contains(
          "[11000000-0000-4000-8000-000000000000,Title 11,5]",
          "[12000000-0000-4000-8000-000000000000,Title 12,15]"));
    }))
    .compose(x -> api.journal(pool, a2, LocalDate.parse("2020-05-01"), LocalDate.parse("2020-06-01")))
    .onComplete(context.asyncAssertSuccess(rowSet -> {
      assertThat(deepToString(rowSet), contains(
          "[21000000-0000-4000-8000-000000000000,Title 21,20]",
          "[22000000-0000-4000-8000-000000000000,Title 22,0]"));
    }));
  }

  @Test
  public void useOverTime() {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.request().getHeader("X-Okapi-Tenant")).thenReturn("foo");
    when(ctx.request().params().get("startDate")).thenReturn("2020-03-01");
    when(ctx.request().params().get("endDate")).thenReturn("2020-05-01");
    new EusageReportsApi().getUseOverTime(vertx, ctx);
  }

}
