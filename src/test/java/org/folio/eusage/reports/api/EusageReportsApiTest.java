package org.folio.eusage.reports.api;

import static org.folio.eusage.reports.api.EusageReportsApi.agreementEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.packageEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleDataTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleEntriesTable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;
import org.testcontainers.containers.PostgreSQLContainer;
import java.time.LocalDate;
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
    ))
    .compose(x -> new EusageReportsApi().postInit(vertx, tenant, new JsonObject().put("module_to", "1.1.1")))
    .compose(x -> loadSampleData())
    .onComplete(context.asyncAssertSuccess());
  }

  @Before
  public void setUp(TestContext context) {
    vertx.exceptionHandler(context.exceptionHandler()); // Report uncaught exceptions
  }

  @Test
  public void testPopulateAgreementLine(TestContext context) {
    EusageReportsApi api = new EusageReportsApi();
    UUID agreementId = UUID.randomUUID();

    api.populateAgreementLine( null, null, new JsonObject(), agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertTrue(x.getMessage().contains("Failed to decode agreement line:"), x.getMessage())));
  }

  private Future<String> getUseOverTime(String format, String startDate, String endDate) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(ctx.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(ctx.request().params().get("format")).thenReturn(format);
    when(ctx.request().params().get("agreementId")).thenReturn(UUID.randomUUID().toString());
    when(ctx.request().params().get("startDate")).thenReturn(startDate);
    when(ctx.request().params().get("endDate")).thenReturn(endDate);
    return new EusageReportsApi().getUseOverTime(vertx, ctx)
    .map(x -> {
      ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
      verify(ctx.response()).end(argument.capture());
      return argument.getValue();
    });
  }

  @Test
  public void useOverTimeStartDateAfterEndDateJournalMonth() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("JOURNAL", "2020-04", "2020-02"));
    assertThat(t.getMessage(), is("startDate=2020-04 is after endDate=2020-02"));
  }

  @Test
  public void useOverTimeStartDateAfterEndDateBookYear() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("BOOK", "2021", "2020"));
    assertThat(t.getMessage(), is("startDate=2021 is after endDate=2020"));
  }

  @Test
  public void useOverTimeStartDateEndDateLengthMismatch() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("JOURNAL", "2019-05", "2021"));
    assertThat(t.getMessage(), is("startDate and endDate must have same length: 2019-05 2021"));
  }

  @Test
  public void useOverTimeDatabase() {
    assertThat(getUseOverTime("DATABASE", "2020-03-01", "2020-04-01").result(), containsString("2020-03"));
  }

  @Test
  public void useOverTimeUnknownFormat() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("FOO", "2020-04-01", "2020-02-01"));
    assertThat(t.getMessage(), containsString("format = FOO"));
  }

  // agreementId
  static String a1  = "10000000-0000-4000-8000-000000000000";
  static String a2  = "20000000-0000-4000-8000-000000000000";
  static String a3  = "30000000-0000-4000-8000-000000000000";
  // kbTitleId
  static String t11 = "11000000-0000-4000-8000-000000000000";
  static String t12 = "12000000-0000-4000-8000-000000000000";
  static String t21 = "21000000-0000-4000-8000-000000000000";
  static String t22 = "22000000-0000-4000-8000-000000000000";
  static String t31 = "31000000-0000-4000-8000-000000000000";
  static String t32 = "32000000-0000-4000-8000-000000000000";
  // kbPackageId
  static String p11 = "1100000a-0000-4000-8000-000000000000";
  // titleEntryId
  static String te11 = "1100000e-0000-4000-8000-000000000000";
  static String te12 = "1200000e-0000-4000-8000-000000000000";
  static String te21 = "2100000e-0000-4000-8000-000000000000";
  static String te22 = "2200000e-0000-4000-8000-000000000000";
  static String te31 = "3100000e-0000-4000-8000-000000000000";
  static String te32 = "3200000e-0000-4000-8000-000000000000";

  private static Future<RowSet<Row>> insertAgreement(String agreementId, String titleId, String packageId) {
    return pool.preparedQuery("INSERT INTO " + agreementEntriesTable(pool)
        + "(id, agreementId, kbTitleId, kbPackageId) VALUES ($1, $2, $3, $4)")
    .execute(Tuple.of(UUID.randomUUID(), agreementId, titleId, packageId));
  }

  private static Future<RowSet<Row>> insertPackageEntry(String packageId, String packageName, String titleId) {
    return pool.preparedQuery("INSERT INTO " + packageEntriesTable(pool)
        + "(kbPackageId, kbPackageName, kbTitleId) VALUES ($1, $2, $3)")
    .execute(Tuple.of(packageId, packageName, titleId));
  }

  private static Future<RowSet<Row>> insertTitleEntry(String titleEntryId,
      String titleId, String titleName, String printISSN, String onlineISSN) {
    return pool.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
        + "(id, kbTitleId, kbTitleName, printISSN, onlineISSN) VALUES ($1, $2, $3, $4, $5)")
    .execute(Tuple.of(titleEntryId, titleId, titleName, printISSN, onlineISSN));
  }

  private static Future<RowSet<Row>> insertTitleEntry(String titleEntryId,
      String titleId, String titleName, String isbn) {
    return pool.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
        + "(id, kbTitleId, kbTitleName, ISBN) VALUES ($1, $2, $3, $4)")
    .execute(Tuple.of(titleEntryId, titleId, titleName, isbn));
  }

  private static Future<RowSet<Row>> insertTitleData(String titleEntryId,
      String dateStart, String dateEnd, int publicationYear,
      boolean openAccess, int uniqueAccessCount, int totalAccessCount) {

    return pool.preparedQuery("INSERT INTO " + titleDataTable(pool)
        + "(id, titleEntryId, usageDateRange, publicationDate, openAccess, uniqueAccessCount, totalAccessCount) "
        + "VALUES ($1, $2, daterange($3::text::date, $4::text::date), $5::text::date, $6, $7, $8)")
    .execute(Tuple.of(UUID.randomUUID(), titleEntryId, dateStart, dateEnd, publicationYear + "-01-01",
        openAccess, uniqueAccessCount, totalAccessCount));
  }

  private static Future<Void> loadSampleData() {
    return insertAgreement(a1, t11, null)
        .compose(x -> insertAgreement(a1, t12, null))
        .compose(x -> insertAgreement(a2, t21, null))
        .compose(x -> insertAgreement(a2, t22, null))
        .compose(x -> insertAgreement(a2, t31, null))
        .compose(x -> insertAgreement(a2, t32, null))
        .compose(x -> insertAgreement(a3, null, p11))
        .compose(x -> insertPackageEntry(p11, "Package 11", t11))
        .compose(x -> insertPackageEntry(p11, "Package 11", t12))
        .compose(x -> insertTitleEntry(te11, t11, "Title 11", "1111-1111", "1111-2222"))
        .compose(x -> insertTitleEntry(te12, t12, "Title 12", "1212-1111", "1212-2222"))
        .compose(x -> insertTitleEntry(te21, t21, "Title 21", "2121-1111", null       ))
        .compose(x -> insertTitleEntry(te22, t22, "Title 22", null,        "2222-2222"))
        .compose(x -> insertTitleEntry(te31, t31, "Title 31", "3131313131"))
        .compose(x -> insertTitleEntry(te32, t32, "Title 32", "3232323232"))
        .compose(x -> insertTitleData(te11, "2020-03-01", "2020-04-01", 1999, false, 1, 2))
        .compose(x -> insertTitleData(te11, "2020-04-01", "2020-04-15", 1999, false, 2, 3))
        .compose(x -> insertTitleData(te11, "2020-04-15", "2020-05-01", 2000, false, 3, 3))
        .compose(x -> insertTitleData(te11, "2020-05-01", "2020-06-01", 2000, false, 4, 12))
        .compose(x -> insertTitleData(te12, "2020-03-01", "2020-04-01", 2010, false, 11, 12))
        .compose(x -> insertTitleData(te12, "2020-04-01", "2020-05-01", 2010, false, 15, 16))
        .compose(x -> insertTitleData(te12, "2020-05-01", "2020-06-01", 2010, false, 14, 22))
        .compose(x -> insertTitleData(te21, "2020-03-01", "2020-04-01", 2010, false, 0, 0))
        .compose(x -> insertTitleData(te21, "2020-05-01", "2020-06-01", 2010, false, 20, 40))
        .compose(x -> insertTitleData(te21, "2020-06-01", "2020-07-01", 2010, true, 1, 2))
        .compose(x -> insertTitleData(te31, "2020-05-01", "2020-06-01", 2010, false, 20, 40))
        .compose(x -> insertTitleData(te32, "2020-06-01", "2020-07-01", 2010, true, 1, 2))
        .mapEmpty();
  }

  private Future<JsonObject> getUseOverTime(boolean isJournal, boolean includeOA, String agreementId, String start, String end) {
    return new EusageReportsApi().getUseOverTime(pool, isJournal, includeOA, false, agreementId, start, end);
  }

  @Test
  public void useOverTime(TestContext context) {
    getUseOverTime(true, true, a1, "2020-04", "2020-05")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getString("agreementId"), is(a1));
      assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
      assertThat(json.getLong("totalItemRequestsTotal"), is(56L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(38L));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining(22L, 34L)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining(20L, 18L)));
      assertThat(json.getJsonArray("items").size(), is(8));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 18)
              .put("accessCountsByPeriod", new JsonArray("[ 6, 12 ]"))
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(1).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Unique_Item_Requests")
              .put("accessCountTotal", 9)
              .put("accessCountsByPeriod", new JsonArray("[ 5, 4 ]"))
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("accessType", "OA_Gold")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", null)
              .put("accessCountsByPeriod", new JsonArray("[ null, null ]"))
              .encodePrettily()));
    })).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimePackage(TestContext context) {
    // similar to useOverTime test since a3 has same titles as a1.
    getUseOverTime(true, true, a3, "2020-04", "2020-05")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getString("agreementId"), is(a3));
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(56L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(38L));
          assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining(22L, 34L)));
          assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining(20L, 18L)));
          assertThat(json.getJsonArray("items").size(), is(8));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 18)
                  .put("accessCountsByPeriod", new JsonArray("[ 6, 12 ]"))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(1).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Unique_Item_Requests")
                  .put("accessCountTotal", 9)
                  .put("accessCountsByPeriod", new JsonArray("[ 5, 4 ]"))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "OA_Gold")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", null)
                  .put("accessCountsByPeriod", new JsonArray("[ null, null ]"))
                  .encodePrettily()));
        })).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimeOpenAccess(TestContext context) {
    getUseOverTime(true, true, a2, "2020-06", "2020-06")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(2L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(1L));
      JsonObject item2 = json.getJsonArray("items").getJsonObject(2);
      JsonObject item3 = json.getJsonArray("items").getJsonObject(3);
      assertThat(item2.getString("accessType"), is("OA_Gold"));
      assertThat(item3.getString("accessType"), is("OA_Gold"));
      assertThat(item2.getString("metricType"), is("Total_Item_Requests"));
      assertThat(item2.getLong("accessCountTotal"), is(2L));
      assertThat(item3.getString("metricType"), is("Unique_Item_Requests"));
      assertThat(item3.getLong("accessCountTotal"), is(1L));
    }));
  }

  @Test
  public void useOverTimeNoData(TestContext context) {
    // time periods without any data, totals should be null
    getUseOverTime(true, true, a2, "1999", "1999")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(nullValue()));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(nullValue()));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining((Long)null)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining((Long)null)));
    }));
  }

  @Test
  public void useOverTimeBook(TestContext context) {
    getUseOverTime(/* journal = */ false, true, a2, "2020-05", "2020-06")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining(40L, 2L)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining(20L, 1L)));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "31000000-0000-4000-8000-000000000000")
              .put("title", "Title 31")
              .put("ISBN", "3131313131")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 40L)
              .put("accessCountsByPeriod", new JsonArray("[ 40, null ]"))
              .encodePrettily()));
    }));
  }

  @Test
  public void reqsByDateOfUse(TestContext context) {
    new EusageReportsApi().getUseOverTime(pool, true, true, true, a2, "2020-05", "2020-06")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining(40L, 2L)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining(20L, 1L)));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "21000000-0000-4000-8000-000000000000")
              .put("title", "Title 21")
              .put("printISSN", "2121-1111")
              .put("onlineISSN", null)
              .put("publicationYear", 2010)
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 40L)
              .put("accessCountsByPeriod", new JsonArray("[ 40, null ]"))
              .encodePrettily()));
    }));
  }

  @Test
  public void reqsByDateOfUseWithRoutingContext(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("foo")).thenReturn("bar");
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi().getReqsByDateOfUse(vertx, routingContext)
    .onComplete(context.asyncAssertSuccess(x -> {
      ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
      verify(routingContext.response()).end(body.capture());
      JsonObject json = new JsonObject(body.getValue());
      assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
      assertThat(json.getJsonArray("totalItemRequestsByPeriod"), contains(40, 2));
      assertThat(json.getJsonArray("uniqueItemRequestsByPeriod"), contains(20, 1));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "21000000-0000-4000-8000-000000000000")
              .put("title", "Title 21")
              .put("printISSN", "2121-1111")
              .put("onlineISSN", null)
              .put("publicationYear", 2010)
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 40)
              .put("accessCountsByPeriod", new JsonArray("[ 40, null ]"))
              .encodePrettily()));
    }));
  }

  private void floorMonths(TestContext context, String date, int months, String expected) {
    assertThat(EusageReportsApi.Periods.floorMonths(LocalDate.parse(date), months).toString(), is(expected));
    String sql = "SELECT " + pool.getSchema() + ".floor_months('" + date + "'::date, " + months + ")";
    pool.query(sql).execute().onComplete(context.asyncAssertSuccess(res -> {
      assertThat(sql, res.iterator().next().getLocalDate(0).toString(), is(expected));
    }));
  }

  /*
   * Cannot combine https://github.com/Pragmatists/JUnitParams and
   * https://github.com/vert-x3/vertx-unit in a single test class.
   *
   * TODO: Switch to https://github.com/vert-x3/vertx-junit5 that allows
   * to use JUnit 5 built-in parameterized tests.
   */
  @Test
  public void floorMonths(TestContext context) {
    floorMonths(context, "2019-05-17",   3, "2019-04-01");
    floorMonths(context, "2019-05-17",  12, "2019-01-01");
    floorMonths(context, "2019-12-31",  24, "2018-01-01");
    floorMonths(context, "1919-05-17", 120, "1910-01-01");
    floorMonths(context, "2018-01-01",   5, "2017-12-01");
    floorMonths(context, "2018-09-17",   5, "2018-05-01");
    floorMonths(context, "2018-10-17",   5, "2018-10-01");
    floorMonths(context, "2019-05-17",   5, "2019-03-01");
  }

  private Future<JsonObject> getReqsByPubPeriod(boolean includeOA, String agreementId,
      String start, String end, String periodOfUse) {

    return new EusageReportsApi().getReqsByPubYear(pool, includeOA, agreementId, start, end, periodOfUse);
  }

  @Test
  public void reqsByPubYear(TestContext context) {
    getReqsByPubPeriod(true, a1, "2020-04", "2020-08", "6M")
    .onComplete(context.asyncAssertSuccess(json -> {
      System.out.println(json.encodePrettily());
      assertThat(json.getInteger("totalItemRequestsTotal"), is(70));
      assertThat(json.getInteger("uniqueItemRequestsTotal"), is(50));
      assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("1999", "2000", "2010"));
      assertThat((Long []) json.getValue("totalItemRequestsByPeriod"), is(arrayContaining(5L, 15L, 50L)));
      assertThat((Long []) json.getValue("uniqueItemRequestsByPeriod"), is(arrayContaining(3L, 7L, 40L)));
      assertThat(json.getJsonArray("items").size(), is(16));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("periodOfUse", "2020-01 - 2020-06")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 20)
              .put("accessCountsByPeriod", new JsonArray("[ 5, 15, null ]"))
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("periodOfUse", "2020-07 - 2020-12")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", null)
              .put("accessCountsByPeriod", new JsonArray("[ null, null, null ]"))
              .encodePrettily()));
    }));
  }

  @Test
  public void reqsByPubYearWithoutData(TestContext context) {
    getReqsByPubPeriod(true, a1, "2999-04", "2999-05", "1Y")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getJsonArray("accessCountPeriods").encode(), is("[]"));
    }));
  }
}
