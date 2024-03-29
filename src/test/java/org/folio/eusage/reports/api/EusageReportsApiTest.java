package org.folio.eusage.reports.api;

import static org.folio.eusage.reports.api.EusageReportsApi.agreementEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.packageEntriesTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleDataTable;
import static org.folio.eusage.reports.api.EusageReportsApi.titleEntriesTable;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
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
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.folio.tlib.postgres.TenantPgPool;
import org.folio.tlib.postgres.testing.TenantPgPoolContainer;
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
import java.io.IOException;
import java.io.StringReader;
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
  private static WebClient webClient;
  private static TenantPgPool pool;
  private static String tenant = "tenant";

  @BeforeClass
  public static void beforeClass(TestContext context) {
    vertx = runTestOnContext.vertx();
    webClient = WebClient.create(vertx);
    TenantPgPool.setModule("mod-eusage-reports-api-test");
    pool = TenantPgPool.pool(vertx, tenant);
    pool.execute(List.of(
        "DROP SCHEMA IF EXISTS " + pool.getSchema() + " CASCADE",
        "DROP ROLE IF EXISTS " + pool.getSchema(),
        "CREATE ROLE " + pool.getSchema() + " PASSWORD 'tenant' NOSUPERUSER NOCREATEDB INHERIT LOGIN",
        "GRANT " + pool.getSchema() + " TO CURRENT_USER",
        "CREATE SCHEMA " + pool.getSchema() + " AUTHORIZATION " + pool.getSchema()
    ))
    .compose(x -> new EusageReportsApi(webClient).postInit(vertx, tenant, new JsonObject().put("module_to", "1.1.1")))
    .compose(x -> loadSampleData())
    .onComplete(context.asyncAssertSuccess());
  }

  @Before
  public void setUp(TestContext context) {
    vertx.exceptionHandler(context.exceptionHandler()); // Report uncaught exceptions
  }

  @Test
  public void testPopulateAgreementLine(TestContext context) {
    EusageReportsApi api = new EusageReportsApi(webClient);
    UUID agreementId = UUID.randomUUID();

    api.populateAgreementLine( null, null, new JsonObject(), agreementId, null)
        .onComplete(context.asyncAssertFailure(x ->
            context.assertTrue(x.getMessage().contains("Failed to decode agreement line:"), x.getMessage())));
  }

  private Future<String> getUseOverTime(String format, String startDate, String endDate, boolean csv) {
    return getUseOverTime(format, startDate, endDate, csv, true);
  }

  private Future<String> getUseOverTime(String format, String startDate, String endDate,
      boolean csv, boolean full) {
    RoutingContext ctx = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    if (csv) {
      when(ctx.request().params().get("csv")).thenReturn("true");
    }
    if (!full) {
      when(ctx.request().params().get("full")).thenReturn("false");
    }
    when(ctx.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(ctx.request().params().get("format")).thenReturn(format);
    when(ctx.request().params().get("agreementId")).thenReturn(UUID.randomUUID().toString());
    when(ctx.request().params().get("startDate")).thenReturn(startDate);
    when(ctx.request().params().get("endDate")).thenReturn(endDate);
    return new EusageReportsApi(webClient).getUseOverTime(vertx, ctx)
    .map(x -> {
      ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
      verify(ctx.response()).end(argument.capture());
      return argument.getValue();
    });
  }

  @Test
  public void useOverTimeStartDateAfterEndDateJournalMonth() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("JOURNAL", "2020-04", "2020-02", false));
    assertThat(t.getMessage(), is("startDate=2020-04 is after endDate=2020-02"));
  }

  @Test
  public void useOverTimeStartDateAfterEndDateBookYear() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("BOOK", "2021", "2020", false));
    assertThat(t.getMessage(), is("startDate=2021 is after endDate=2020"));
  }

  @Test
  public void useOverTimeTooManyPeriods() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
        getUseOverTime("BOOK", "1900", "2020", false));
    assertThat(t.getMessage(), is("Too many periods 121 (max is 100) for startDate=1900-01-01, endDate= 2020-01-01 1Y"));
  }

  @Test
  public void useOverTimeCsvOK(TestContext context) {
    getUseOverTime("ALL", "2020", "2021", true)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimeCsvNoItems(TestContext context) {
    getUseOverTime("ALL", "2020", "2021", true, false)
        .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimeJsonOK(TestContext context) {
    getUseOverTime("BOOK", "2020", "2021", false, true)
        .onComplete(context.asyncAssertSuccess(x -> assertThat(x, containsString("\"items\""))));
  }

  @Test
  public void useOverTimeJsonNoItems(TestContext context) {
    getUseOverTime("BOOK", "2020", "2021", false, false)
        .onComplete(context.asyncAssertSuccess(x -> assertThat(x, not(containsString("\"items\"")))));
  }

  @Test
  public void useOverTimeStartDateEndDateLengthMismatch() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("JOURNAL", "2019-05", "2021", false));
    assertThat(t.getMessage(), is("startDate and endDate must have same length: 2019-05 2021"));
  }

  @Test
  public void useOverTimeUnknownFormat() {
    Throwable t = assertThrows(IllegalArgumentException.class, () ->
    getUseOverTime("FOO", "2020-04-01", "2020-02-01", false));
    assertThat(t.getMessage(), containsString("format = FOO"));
  }

  // agreementId
  static String a1  = "10000000-0000-4000-8000-000000000000";
  static String a2  = "20000000-0000-4000-8000-000000000000";
  static String a3  = "30000000-0000-4000-8000-000000000000";
  static String a4  = "40000000-0000-4000-8000-000000000000";
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
            + "(id, agreementId, kbTitleId, kbPackageId)"
            + " VALUES ($1, $2, $3, $4)")
        .execute(Tuple.of(UUID.randomUUID(), agreementId, titleId, packageId));
  }

  private static Future<RowSet<Row>> updateAgreement(String agreementId, String set) {
    return pool.preparedQuery("UPDATE " + agreementEntriesTable(pool) + " SET " + set
        + " WHERE agreementId = $1").execute(Tuple.of(UUID.fromString(agreementId)));
  }

  private static Future<RowSet<Row>> insertPackageEntry(String packageId, String packageName, String titleId) {
    return pool.preparedQuery("INSERT INTO " + packageEntriesTable(pool)
        + "(kbPackageId, kbPackageName, kbTitleId) VALUES ($1, $2, $3)")
    .execute(Tuple.of(packageId, packageName, titleId));
  }

  private static Future<RowSet<Row>> insertTitleSerial(String titleEntryId, String titleId,
      String titleName, String printISSN, String onlineISSN, String publicationType) {
    return pool.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
            + "(id, kbTitleId, kbTitleName, printISSN, onlineISSN, publicationType)"
            +" VALUES ($1, $2, $3, $4, $5, $6)")
        .execute(Tuple.of(titleEntryId, titleId, titleName, printISSN, onlineISSN, publicationType));
  }

  private static Future<RowSet<Row>> insertTitleMonograph(String titleEntryId,
      String titleId, String titleName, String isbn) {
    return pool.preparedQuery("INSERT INTO " + titleEntriesTable(pool)
        + "(id, kbTitleId, kbTitleName, ISBN, publicationType) VALUES ($1, $2, $3, $4, $5)")
    .execute(Tuple.of(titleEntryId, titleId, titleName, isbn, "monograph"));
  }

  private static Future<RowSet<Row>> insertTitleData(String titleEntryId,
      String dateStart, String dateEnd, String publicationYear,
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
        .compose(x -> updateAgreement(a1, "orderType = 'Ongoing', poLineNumber = '[\"p1\"]', invoiceNumber = '[\"i1\"]',"
            + " fiscalYearRange='[2020-01-01,2021-01-01)',"
            + " coverageDateRanges='[1998-01-01,2020-01-01]',"
            + " encumberedCost = 100, invoicedCost = 110"
        ))
        .compose(x -> insertAgreement(a2, t21, null))
        .compose(x -> insertAgreement(a2, t22, null))
        .compose(x -> insertAgreement(a2, t31, null))
        .compose(x -> insertAgreement(a2, t32, null))
        .compose(x -> insertAgreement(a2, t21, null)) // dup
        .compose(x -> insertAgreement(a2, t22, null)) // dup
        .compose(x -> insertAgreement(a2, t31, null)) // dup
        .compose(x -> insertAgreement(a2, t32, null)) // dup
       .compose(x -> updateAgreement(a2, "orderType = 'One-Time', poLineNumber = 'p2', invoiceNumber = 'i2',"
            + " fiscalYearRange='[2020-01-01,2021-01-01)',"
            + " coverageDateRanges='[1998-01-01,2021-01-01]',"
            + " encumberedCost = 200, invoicedCost = 210"
        ))
        .compose(x -> insertAgreement(a3, null, p11))
        .compose(x -> updateAgreement(a3, "orderType = 'Ongoing', poLineNumber = 'p3', invoiceNumber = 'i3',"
            + " subscriptionDateRange = '[2020-03-03, 2021-01-15]', fiscalYearRange='[2020-01-01,2021-01-01)',"
            + " coverageDateRanges='[1998-01-01,2021-01-01]',"
            + " encumberedCost = 300, invoicedCost = 310"
        ))
        .compose(x -> insertAgreement(a4, null, p11))
        .compose(x -> updateAgreement(a4, "orderType = 'Ongoing', poLineNumber = 'p3', invoiceNumber = 'i3',"
            + " subscriptionDateRange = '[2020-05-01, 2021-01-01]',"
            + " coverageDateRanges='[1998-01-01,2021-01-01]',"
            + " encumberedCost = 300, invoicedCost = 310"
        ))
        .compose(x -> insertPackageEntry(p11, "Package 11", t11))
        .compose(x -> insertPackageEntry(p11, "Package 11", t12))
        .compose(x -> insertTitleSerial(te11, t11, "Title 11", "1111-1111", "1111-2222", "journal"))
        .compose(x -> insertTitleSerial(te12, t12, "Title 12", "1212-1111", "1212-2222", "journal"))
        .compose(x -> insertTitleSerial(te21, t21, "Title 21", "2121-1111", null, "serial"))
        .compose(x -> insertTitleSerial(te22, t22, "Title 22", null,        "2222-2222", "serial"))
        .compose(x -> insertTitleMonograph(te31, t31, "Title 31", "3131313131"))
        .compose(x -> insertTitleMonograph(te32, t32, "Title 32", "3232323232"))
        .compose(x -> insertTitleData(te11, "2020-03-01", "2020-04-01", "1999", false, 1, 2))
        .compose(x -> insertTitleData(te11, "2020-04-01", "2020-04-15", "1999", false, 2, 3))
        .compose(x -> insertTitleData(te11, "2020-04-15", "2020-05-01", "2000", false, 3, 3))
        .compose(x -> insertTitleData(te11, "2020-05-01", "2020-06-01", "2000", false, 4, 12))
        .compose(x -> insertTitleData(te11, "2020-06-01", "2020-07-01", "2000", false, 9, 29))
        .compose(x -> insertTitleData(te12, "2020-02-01", "2020-03-01", "1843", false, 0, 0))
        .compose(x -> insertTitleData(te12, "2020-03-01", "2020-04-01", "2010", false, 11, 12))
        .compose(x -> insertTitleData(te12, "2020-04-01", "2020-05-01", "2010", false, 15, 16))
        .compose(x -> insertTitleData(te12, "2020-05-01", "2020-06-01", "2010", false, 14, 22))
        .compose(x -> insertTitleData(te21, "2020-03-01", "2020-04-01", "2010", false, 0, 0))
        .compose(x -> insertTitleData(te21, "2020-05-01", "2020-06-01", "2010", false, 20, 40))
        .compose(x -> insertTitleData(te21, "2020-06-01", "2020-07-01", "2010", true, 1, 2))
        .compose(x -> insertTitleData(te31, "2020-05-01", "2020-06-01", "0001", false, 20, 40))
        .compose(x -> insertTitleData(te32, "2020-06-01", "2020-07-01", "2010", true, 1, 2))
        .mapEmpty();
  }

  private Future<JsonObject> getUseOverTime(Boolean isJournal, Boolean includeOA, String agreementId, String accessCountPeriod, String start, String end) {
    return new EusageReportsApi(webClient).getUseOverTime(pool, isJournal, includeOA, agreementId, accessCountPeriod, start, end);
  }

  @Test
  public void useOverTime(TestContext context) {
    getUseOverTime(true, true, a1, null, "2020-04", "2020-05")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getString("agreementId"), is(a1));
      assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
      assertThat(json.getLong("totalItemRequestsTotal"), is(56L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(38L));
      assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(22L, 34L));
      assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 18L));
assertThat(json.getJsonArray("items").size(), is(4));
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
              .put("kbId", "12000000-0000-4000-8000-000000000000")
              .put("title", "Title 12")
              .put("printISSN", "1212-1111")
              .put("onlineISSN", "1212-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 38)
              .put("accessCountsByPeriod", new JsonArray("[ 16, 22 ]"))
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(3).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "12000000-0000-4000-8000-000000000000")
              .put("title", "Title 12")
              .put("printISSN", "1212-1111")
              .put("onlineISSN", "1212-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Unique_Item_Requests")
              .put("accessCountTotal", 29)
              .put("accessCountsByPeriod", new JsonArray("[ 15, 14 ]"))
              .encodePrettily()));
    })).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimePackage(TestContext context) {
    // similar to useOverTime test since a3 has same titles as a1.
    getUseOverTime(true, true, a3, "auto", "2020-04", "2020-05")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getString("agreementId"), is(a3));
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(56L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(38L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(22L, 34L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 18L));
          assertThat(json.getJsonArray("items").size(), is(4));
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
                  .put("kbId", "12000000-0000-4000-8000-000000000000")
                  .put("title", "Title 12")
                  .put("printISSN", "1212-1111")
                  .put("onlineISSN", "1212-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 38)
                  .put("accessCountsByPeriod", new JsonArray("[ 16, 22 ]"))
                  .encodePrettily()));
        })).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimeCsv(TestContext context) {
    new EusageReportsApi(webClient).getUseOverTime(pool, true, true, a1, null,"2020-04", "2020-05", true, true)
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res, containsString("Title,Print ISSN,Online ISSN,ISBN,Access type,Metric Type,Reporting period total,2020-04,2020-05"));
          assertThat(res, containsString("Totals - total item requests,,,,,,56,22,34"));
          assertThat(res, containsString("Totals - unique item requests,,,,,,38,20,18"));
          assertThat(res, containsString("Title 11,1111-1111,1111-2222,,Controlled,Total_Item_Requests,18,6,12"));
        }));
  }

  @Test
  public void useOverTimeCsvAll(TestContext context) {
    new EusageReportsApi(webClient).getUseOverTime(pool, null, true, a1, null,"2020-04", "2020-05", true, true)
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res, containsString("Title,Print ISSN,Online ISSN,ISBN,Access type,Metric Type,Reporting period total,2020-04,2020-05"));
          assertThat(res, containsString("Totals - total item requests,,,,,,56,22,34"));
          assertThat(res, containsString("Totals - unique item requests,,,,,,38,20,18"));
          assertThat(res, containsString("Title 11,1111-1111,1111-2222,,Controlled,Total_Item_Requests,18,6,12"));
        }));
  }

  @Test
  public void useOverTimeCsvBook(TestContext context) {
    new EusageReportsApi(webClient).getUseOverTime(pool, false, true, a2, null,"2020-05", "2020-06", true, true)
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res, containsString("Title,Print ISSN,Online ISSN,ISBN,Access type,Metric Type,Reporting period total,2020-05,2020-06"));
          assertThat(res, containsString("Totals - total item requests,,,,,,42,40,2"));
          assertThat(res, containsString("Totals - unique item requests,,,,,,21,20,1"));
          assertThat(res, containsString("Title 31,,,3131313131,Controlled,Total_Item_Requests,40,40,"));
          assertThat(res, containsString("Title 32,,,3232323232,OA_Gold,Total_Item_Requests,2,0,2"));
        }));
  }

  @Test
  public void useOverTimeOpenAccess(TestContext context) {
    getUseOverTime(true, true, a2, null, "2020-06", "2020-06")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(2L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(1L));
      JsonObject item0 = json.getJsonArray("items").getJsonObject(0);
      JsonObject item1 = json.getJsonArray("items").getJsonObject(1);
      assertThat(item0.getString("accessType"), is("OA_Gold"));
      assertThat(item1.getString("accessType"), is("OA_Gold"));
      assertThat(item0.getString("metricType"), is("Total_Item_Requests"));
      assertThat(item0.getLong("accessCountTotal"), is(2L));
      assertThat(item1.getString("metricType"), is("Unique_Item_Requests"));
      assertThat(item1.getLong("accessCountTotal"), is(1L));
    }));
  }

  @Test
  public void useOverTimeFormatAll(TestContext context) {
    getUseOverTime(null, true, a3, "auto", "2020-04", "2020-05")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getString("agreementId"), is(a3));
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(56L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(38L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(22L, 34L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 18L));
          assertThat(json.getJsonArray("items").size(), is(4));
        })).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimeFormatBook(TestContext context) {
    getUseOverTime(false, true, a3, "auto", "2020-04", "2020-05")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getString("agreementId"), is(a3));
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("2020-04", "2020-05"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(0L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(0L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(0L, 0L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(0L, 0L));
          assertThat(json.getJsonArray("items").size(), is(0));
        })).onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void useOverTimeAccessCountPeriod(TestContext context) {
    getUseOverTime(true, true, a2, "4M", "2020", "2021")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-01 - 2020-04", "2020-05 - 2020-08", "2020-09 - 2020-12", "2021-01 - 2021-04"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(0L, 42L, 0L, 0L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(0L, 21L, 0L, 0L));
          assertThat(json.getJsonArray("items").size(), is(5));
        }));
  }

  @Test
  public void useOverTimeNoData(TestContext context) {
    // time periods without any data, totals should be 0
    getUseOverTime(true, true, a2, null, "1999", "1999")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(0L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(0L));
      assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(0L));
      assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(0L));
    }));
  }

  @Test
  public void useOverTimeBook(TestContext context) {
    getUseOverTime(false, true, a2, null, "2020-05", "2020-06")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
      assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
      assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(40L, 2L));
      assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 1L));
      assertThat(json.getJsonArray("items").size(), is(4));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "31000000-0000-4000-8000-000000000000")
              .put("title", "Title 31")
              .put("ISBN", "3131313131")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 40L)
              .put("accessCountsByPeriod", new JsonArray("[ 40, 0 ]"))
              .encodePrettily()));
    }));
  }

  @Test
  public void useOverTimeAll(TestContext context) {
    getUseOverTime(null, true, a2, null, "2020-05", "2020-06")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(84L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(42L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(80L, 4L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(40L, 2L));
          assertThat(json.getJsonArray("items").size(), is(9));
        }));
  }

  @Test
  public void useOverTimeJournal(TestContext context) {
    getUseOverTime(true, true, a2, null, "2020-05", "2020-06")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(40L, 2L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 1L));
          assertThat(json.getJsonArray("items").size(), is(5));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "21000000-0000-4000-8000-000000000000")
                  .put("title", "Title 21")
                  .put("printISSN", "2121-1111")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 40L)
                  .put("accessCountsByPeriod", new JsonArray("[ 40, 0 ]"))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(4).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "22000000-0000-4000-8000-000000000000")
                  .put("title", "Title 22")
                  .put("onlineISSN", "2222-2222")
                  .put("accessType", null)
                  .put("metricType", null)
                  .put("accessCountTotal", 0L)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 0 ]"))
                  .encodePrettily()));
        }));
  }

  @Test
  public void reqsByDateOfUseJournal(TestContext context) {
    new EusageReportsApi(webClient).getReqsByDateOfUse(pool, true, true, a2, null, "2020-05", "2020-06", null)
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(40L, 2L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 1L));
          assertThat(json.getJsonArray("totalRequestsPublicationYearsByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject().put("2010", 40))
                  .add(new JsonObject().put("2010", 2))
                  .encodePrettily()));
          assertThat(json.getJsonArray("uniqueRequestsPublicationYearsByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject().put("2010", 20))
                  .add(new JsonObject().put("2010", 1))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "21000000-0000-4000-8000-000000000000")
                  .put("title", "Title 21")
                  .put("printISSN", "2121-1111")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 40L)
                  .put("accessCountsByPeriod", new JsonArray("[ 40, 0 ]"))
                  .put("publicationYear", "2010")
                  .encodePrettily()));
        }));
  }

  @Test
  public void reqsByDateOfUseBook(TestContext context) {
    new EusageReportsApi(webClient).getReqsByDateOfUse(pool, false, true, a2, null, "2020-05", "2020-06", null)
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(40L, 2L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 1L));
          assertThat(json.getJsonArray("items").size(), is(4));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "31000000-0000-4000-8000-000000000000")
                  .put("title", "Title 31")
                  .put("ISBN", "3131313131")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 40)
                  .put("accessCountsByPeriod", new JsonArray("[ 40, 0 ]"))
                  .put("publicationYear", "0001")
                  .encodePrettily()));
        }));
  }

  @Test
  public void reqsByDateOfUseBookNoOA(TestContext context) {
    new EusageReportsApi(webClient).getReqsByDateOfUse(pool, false, false, a2, null, "2020-05", "2020-06", "5Y")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(40L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(20L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(40L, 0L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(20L, 0L));
          assertThat(json.getJsonArray("items").size(), is(3));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "31000000-0000-4000-8000-000000000000")
                  .put("title", "Title 31")
                  .put("ISBN", "3131313131")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 40)
                  .put("accessCountsByPeriod", new JsonArray("[ 40, 0 ]"))
                  .put("publicationYear", "0000 - 0004")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(1).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "31000000-0000-4000-8000-000000000000")
                  .put("title", "Title 31")
                  .put("ISBN", "3131313131")
                  .put("accessType", "Controlled")
                  .put("metricType", "Unique_Item_Requests")
                  .put("accessCountTotal", 20)
                  .put("accessCountsByPeriod", new JsonArray("[ 20, 0 ]"))
                  .put("publicationYear", "0000 - 0004")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "32000000-0000-4000-8000-000000000000")
                  .put("title", "Title 32")
                  .put("ISBN", "3232323232")
                  .put("accessType", null)
                  .put("metricType", null)
                  .put("accessCountTotal", 0)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 0 ]"))
                  .encodePrettily()));
        }));
  }

  @Test
  public void reqsByDateOfUseAll(TestContext context) {
    new EusageReportsApi(webClient).getReqsByDateOfUse(pool, null, true, a2, null, "2020-05", "2020-06", null)
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(84L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(42L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(80L, 4L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(40L, 2L));
          assertThat(json.getJsonArray("items").size(), is(9));
        }));
  }

  @Test
  public void reqsByDateOfUseNoPubYears(TestContext context) {
    new EusageReportsApi(webClient).getReqsByDateOfUse(pool, true, true, a1, null, "2005-02", "2005-06", "auto")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(0L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(0L));
        }));
  }

  @Test
  public void reqsByDateOfUse63(TestContext context) {
    new EusageReportsApi(webClient)
        .getReqsByDateOfUse(pool, true, true, a1, null, "2020-02", "2020-06", null)
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(0L, 14L, 22L, 34L, 29L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(0L, 12L, 20L, 18L, 9L));
          assertThat(json.getJsonArray("totalRequestsPublicationYearsByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject())
                  .add(new JsonObject().put("1999", 2).put("2010", 12))
                  .add(new JsonObject().put("1999", 3).put("2000", 3).put("2010", 16))
                  .add(new JsonObject().put("2000", 12).put("2010", 22))
                  .add(new JsonObject().put("2000", 29))
                  .encodePrettily()));
          assertThat(json.getJsonArray("uniqueRequestsPublicationYearsByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject())
                  .add(new JsonObject().put("1999", 1).put("2010", 11))
                  .add(new JsonObject().put("1999", 2).put("2000", 3).put("2010", 15))
                  .add(new JsonObject().put("2000", 4).put("2010", 14))
                  .add(new JsonObject().put("2000", 9))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 5)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 2, 3, 0, 0 ]"))
                  .put("publicationYear", "1999")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(1).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Unique_Item_Requests")
                  .put("accessCountTotal", 3)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 1, 2, 0, 0 ]"))
                  .put("publicationYear", "1999")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 44)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 0, 3, 12, 29 ]"))
                  .put("publicationYear", "2000")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(3).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Unique_Item_Requests")
                  .put("accessCountTotal", 16)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 0, 3, 4, 9 ]"))
                  .put("publicationYear", "2000")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(4).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "12000000-0000-4000-8000-000000000000")
                  .put("title", "Title 12")
                  .put("printISSN", "1212-1111")
                  .put("onlineISSN", "1212-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 50)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 12, 16, 22, 0 ]"))
                  .put("publicationYear", "2010")
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").getJsonObject(5).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "12000000-0000-4000-8000-000000000000")
                  .put("title", "Title 12")
                  .put("printISSN", "1212-1111")
                  .put("onlineISSN", "1212-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Unique_Item_Requests")
                  .put("accessCountTotal", 40)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 11, 15, 14, 0 ]"))
                  .put("publicationYear", "2010")
                  .encodePrettily()));
        }));
  }

  @Test
  public void reqsByPubYear63(TestContext context) {
    new EusageReportsApi(webClient).getReqsByPubYear(pool, true, true, a1, null, "2020-02", "2020-06", "1M")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("1999", "2000", "2010"));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(5L, 44L, 50L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(3L, 16L, 40L));
          assertThat(json.getJsonArray("totalRequestsPeriodsOfUseByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject().put("2020-03", 2).put("2020-04", 3))
                  .add(new JsonObject().put("2020-04", 3).put("2020-05", 12).put("2020-06", 29))
                  .add(new JsonObject().put("2020-03", 12).put("2020-04", 16).put("2020-05", 22))
                  .encodePrettily()));
          assertThat(json.getJsonArray("uniqueRequestsPeriodsOfUseByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject().put("2020-03", 1).put("2020-04", 2))
                  .add(new JsonObject().put("2020-04", 3).put("2020-05", 4).put("2020-06", 9))
                  .add(new JsonObject().put("2020-03", 11).put("2020-04", 15).put("2020-05", 14))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").size(), is(16));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 2)
                  .put("accessCountsByPeriod", new JsonArray("[ 2, 0, 0 ]"))
                  .put("periodOfUse", "2020-03")
                  .encodePrettily()));
        }));
  }

  @Test
  public void useOverTime63(TestContext context) {
    getUseOverTime(true, true, a1, null, "2020-02", "2020-06")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(0L, 14L, 22L, 34L, 29L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(0L, 12L, 20L, 18L, 9L));
          assertThat(json.getJsonArray("items").size(), is(4));
        }));
  }

  @Test
  public void costPerUse63(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-02");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    when(routingContext.request().params().get("format")).thenReturn("JOURNAL");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat(json.getJsonArray("items").size(), is(4));
        }));
  }

  @Test
  public void reqsByDateOfUseYopInterval2Y(TestContext context) {
    new EusageReportsApi(webClient)
        .getReqsByDateOfUse(pool, null, true, a1, null, "2020-02", "2020-06", "2Y")
        .onComplete(context.asyncAssertSuccess(json -> {
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(0L, 14L, 22L, 34L, 29L));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(0L, 12L, 20L, 18L, 9L));
          assertThat(json.getJsonArray("totalRequestsPublicationYearsByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject())
                  .add(new JsonObject().put("1998 - 1999", 2).put("2010 - 2011", 12))
                  .add(new JsonObject().put("1998 - 1999", 3).put("2000 - 2001", 3).put("2010 - 2011", 16).put("2010 - 2011", 16))
                  .add(new JsonObject().put("2000 - 2001", 12).put("2010 - 2011", 22))
                  .add(new JsonObject().put("2000 - 2001", 29))
                  .encodePrettily()));
          assertThat(json.getJsonArray("uniqueRequestsPublicationYearsByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject())
                  .add(new JsonObject().put("1998 - 1999", 1).put("2010 - 2011", 11))
                  .add(new JsonObject().put("1998 - 1999", 2).put("2000 - 2001", 3).put("2010 - 2011", 16).put("2010 - 2011", 15))
                  .add(new JsonObject().put("2000 - 2001", 4).put("2010 - 2011", 14))
                  .add(new JsonObject().put("2000 - 2001", 9))
                  .encodePrettily()));
          assertThat(json.getJsonArray("items").size(), is(6));
          assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
              is(new JsonObject()
                  .put("kbId", "11000000-0000-4000-8000-000000000000")
                  .put("title", "Title 11")
                  .put("printISSN", "1111-1111")
                  .put("onlineISSN", "1111-2222")
                  .put("accessType", "Controlled")
                  .put("metricType", "Total_Item_Requests")
                  .put("accessCountTotal", 5)
                  .put("accessCountsByPeriod", new JsonArray("[ 0, 2, 3, 0, 0 ]"))
                  .put("publicationYear", "1998 - 1999")
                  .encodePrettily()));
        }));
  }

  @Test
  public void reqsByDateOfUseWithRoutingContext(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getReqsByDateOfUse(vertx, routingContext)
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
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 40)
              .put("accessCountsByPeriod", new JsonArray("[ 40, 0 ]"))
              .put("publicationYear", "2010")
              .encodePrettily()));
    }));
  }

  @Test
  public void reqsByDateOfUseWithRoutingContextNoItems(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("full")).thenReturn("false");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getReqsByDateOfUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat(json.getLong("totalItemRequestsTotal"), is(42L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(21L));
          assertThat(json.getJsonArray("totalItemRequestsByPeriod"), contains(40, 2));
          assertThat(json.getJsonArray("uniqueItemRequestsByPeriod"), contains(20, 1));
          assertThat(json.containsKey("items"), is(false));
        }));
  }

  @Test
  public void reqsByDateOfUseWithRoutingContextCsv(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("csv")).thenReturn("true");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getReqsByDateOfUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          String res = body.getValue();
          assertThat(res, containsString("Title,Print ISSN,Online ISSN,ISBN,Year of publication,Access type,Metric Type,Reporting period total,2020-05,2020-06"));
          assertThat(res, containsString("Totals - total item requests,,,,,,,42,40,2"));
          assertThat(res, containsString("Title 21,2121-1111,,,2010,Controlled,Unique_Item_Requests,20,20,"));
        }));
  }

  private void floorMonths(TestContext context, String date, int months, String expected) {
    assertThat(Periods.floorMonths(LocalDate.parse(date), months).toString(), is(expected));
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
      String accessCountPeriod, String start, String end, String periodOfUse) {

    return new EusageReportsApi(webClient).getReqsByPubYear(pool, true, includeOA, agreementId, accessCountPeriod, start, end, periodOfUse);
  }

  @Test
  public void reqsByPubYearAccessCountPeriodAuto(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("full")).thenReturn("false");
    when(routingContext.request().params().get("accessCountPeriod")).thenReturn("auto");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("periodOfUse")).thenReturn("6M");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getReqsByPubYear(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("1999", "2000", "2010"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat(json.getJsonArray("totalItemRequestsByPeriod"), contains(5, 44, 50));
          assertThat(json.getJsonArray("uniqueItemRequestsByPeriod"), contains(3, 16, 40));
          assertThat(json.getJsonArray("totalRequestsPeriodsOfUseByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject().put("2020-01 - 2020-06", 5))
                  .add(new JsonObject().put("2020-01 - 2020-06", 44))
                  .add(new JsonObject().put("2020-01 - 2020-06", 50))
                  .encodePrettily()));
          assertThat(json.getJsonArray("uniqueRequestsPeriodsOfUseByPeriod").encodePrettily(),
              is(new JsonArray()
                  .add(new JsonObject().put("2020-01 - 2020-06", 3))
                  .add(new JsonObject().put("2020-01 - 2020-06", 16))
                  .add(new JsonObject().put("2020-01 - 2020-06", 40))
                  .encodePrettily()));
          assertThat(json.containsKey("items"), is(false));
        }));
  }

  @Test
  public void reqsByPubYearAccessCountPeriod2Y(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("accessCountPeriod")).thenReturn("2Y");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("periodOfUse")).thenReturn("6M");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getReqsByPubYear(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat(json.getJsonArray("items").size(), is(4));
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("1998 - 1999", "2000 - 2001", "2010 - 2011"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat(json.getJsonArray("totalItemRequestsByPeriod"), contains(5, 44, 50));
          assertThat(json.getJsonArray("uniqueItemRequestsByPeriod"), contains(3, 16, 40));
        }));
  }

  @Test
  public void reqsByPubYearAccessCountPeriod3M(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("accessCountPeriod")).thenReturn("3M");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("periodOfUse")).thenReturn("6M");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getReqsByPubYear(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("1999-01 - 1999-03", "2000-01 - 2000-03", "2010-01 - 2010-03"));
          assertThat(json.getLong("totalItemRequestsTotal"), is(99L));
          assertThat(json.getLong("uniqueItemRequestsTotal"), is(59L));
          assertThat(json.getJsonArray("totalItemRequestsByPeriod"), contains(5, 44, 50));
          assertThat(json.getJsonArray("uniqueItemRequestsByPeriod"), contains(3, 16, 40));
        }));
  }

  @Test
  public void reqsByPubYear(TestContext context) {
    getReqsByPubPeriod(true, a1, null, "2020-04", "2020-08", "6M")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getInteger("totalItemRequestsTotal"), is(99));
      assertThat(json.getInteger("uniqueItemRequestsTotal"), is(59));
      assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(), contains("1999", "2000", "2010"));
      assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(), contains(5L, 44L, 50L));
      assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(), contains(3L, 16L, 40L));
      assertThat(json.getJsonArray("items").size(), is(4));
      assertThat(json.getJsonArray("items").getJsonObject(0).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 49)
              .put("accessCountsByPeriod", new JsonArray("[ 5, 44, 0 ]"))
              .put("periodOfUse", "2020-01 - 2020-06")
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(1).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "11000000-0000-4000-8000-000000000000")
              .put("title", "Title 11")
              .put("printISSN", "1111-1111")
              .put("onlineISSN", "1111-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Unique_Item_Requests")
              .put("accessCountTotal", 19)
              .put("accessCountsByPeriod", new JsonArray("[ 3, 16, 0 ]"))
              .put("periodOfUse", "2020-01 - 2020-06")
              .encodePrettily()));
      assertThat(json.getJsonArray("items").getJsonObject(2).encodePrettily(),
          is(new JsonObject()
              .put("kbId", "12000000-0000-4000-8000-000000000000")
              .put("title", "Title 12")
              .put("printISSN", "1212-1111")
              .put("onlineISSN", "1212-2222")
              .put("accessType", "Controlled")
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", 50)
              .put("accessCountsByPeriod", new JsonArray("[ 0, 0, 50 ]"))
              .put("periodOfUse", "2020-01 - 2020-06")
              .encodePrettily()));
    }));
  }

  @Test
  public void costPerUseWithRoutingContextNoItems(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("full")).thenReturn("false");
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-04", "2020-05", "2020-06", "2020-07", "2020-08"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(2, 2, 1, 0, 0));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(0.83, 0.54, 0.32, null, null));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(0.92, 1.02, 1.02, null, null));
          assertThat(json.getDouble("amountPaidTotal"), is(91.67)); // 5/12 * 220
          assertThat(json.getDouble("amountEncumberedTotal"), is(83.33));
          assertThat(json.containsKey("items"), is(false));
        }));
  }

  @Test
  public void costPerUseWithRoutingContext1(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-04", "2020-05", "2020-06", "2020-07", "2020-08"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(2, 2, 1, 0, 0));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(0.83, 0.54, 0.32, null, null));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(0.92, 1.02, 1.02, null, null));
          assertThat(json.getDouble("amountPaidTotal"), is(91.67)); // 5/12 * 220
          assertThat(json.getDouble("amountEncumberedTotal"), is(83.33));
          assertThat(json.getJsonArray("items").size(), is(3));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("kbId"), is(t11));
          assertThat(json.getJsonArray("items").getJsonObject(0).getBoolean("derivedTitle"), is(false));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("totalItemRequests"), is(3L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("uniqueItemRequests"), is(2L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountEncumbered"), is(20.83));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountPaid"), is(22.92));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("publicationYear"), is("1999"));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerTotalRequest"), is(7.64));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerUniqueRequest"), is(11.46));
          assertThat(json.getJsonArray("items").getJsonObject(1).getString("kbId"), is(t11));
          assertThat(json.getJsonArray("items").getJsonObject(1).getBoolean("derivedTitle"), is(false));
          assertThat(json.getJsonArray("items").getJsonObject(1).getLong("totalItemRequests"), is(44L));
          assertThat(json.getJsonArray("items").getJsonObject(1).getLong("uniqueItemRequests"), is(16L));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("amountEncumbered"), is(20.83));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("amountPaid"), is(22.92));
          assertThat(json.getJsonArray("items").getJsonObject(1).getString("publicationYear"), is("2000"));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("costPerTotalRequest"), is(0.52));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("costPerUniqueRequest"), is(1.43));
          assertThat(json.getJsonArray("items").getJsonObject(2).getString("kbId"), is(t12));
          assertThat(json.getJsonArray("items").getJsonObject(2).getBoolean("derivedTitle"), is(false));
          assertThat(json.getJsonArray("items").getJsonObject(2).getLong("totalItemRequests"), is(38L));
          assertThat(json.getJsonArray("items").getJsonObject(2).getLong("uniqueItemRequests"), is(29L));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("amountEncumbered"), is(41.67));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("amountPaid"), is(45.83));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("costPerTotalRequest"), is(1.21));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("costPerUniqueRequest"), is(1.58));
        }));
  }

  @Test
  public void costPerUseWithRoutingContext2(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-04", "2020-05", "2020-06", "2020-07", "2020-08"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(0, 2, 2, 0, 0));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(),
              contains(0, 80, 4, 0, 0));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(),
              contains(0, 40, 2, 0, 0));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(null, 0.44, 8.75, null, null));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(null, 0.88, 17.5, null, null));
        }));
  }

  @Test
  public void costPerUseWithRoutingContext2NoOA(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("includeOA")).thenReturn("false");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-04", "2020-05", "2020-06", "2020-07", "2020-08"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(0, 2, 0, 0, 0));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(),
              contains(0, 80, 0, 0, 0));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(),
              contains(0, 40, 0, 0, 0));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(null, 0.44, null, null, null));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(null, 0.88, null, null, null));
        }));
  }

  @Test
  public void costPerUseWithRoutingContext3(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a3);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-04", "2020-05", "2020-06"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(2, 2, 1));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(1.28, 0.83, 0.97));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(1.41, 1.57, 3.13));
        }));
  }

  @Test
  public void costPerUseWithRoutingContext4(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a4);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-07");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-04", "2020-05", "2020-06", "2020-07"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(2, 2, 1, 0));
          assertThat((List<?>) json.getJsonArray("totalItemRequestsByPeriod").getList(),
              contains(0, 34, 29, 0));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(null, 1.01, 1.19, null));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(null, 1.91, 3.83, null));
          assertThat((List<?>) json.getJsonArray("uniqueItemRequestsByPeriod").getList(),
              contains(0, 18, 9, 0));
          assertThat((List<?>) json.getJsonArray("costByPeriod").getList(),
              contains(0.0, 34.44, 34.44, 0.0));
          JsonArray items = json.getJsonArray("items");
          assertThat(items.size(), is(3));
          assertThat(items.getJsonObject(0).getBoolean("derivedTitle"), is(true));
          assertThat(items.getJsonObject(1).getBoolean("derivedTitle"), is(true));
          assertThat(items.getJsonObject(2).getBoolean("derivedTitle"), is(true));
          assertThat(items.getJsonObject(0).getDouble("costPerTotalRequest"), is(nullValue()));
          assertThat(items.getJsonObject(1).getDouble("costPerTotalRequest"), is(0.63));
          assertThat(items.getJsonObject(2).getDouble("costPerTotalRequest"), is(2.35));
          assertThat(items.getJsonObject(0).getDouble("costPerUniqueRequest"), is(nullValue()));
          assertThat(items.getJsonObject(1).getDouble("costPerUniqueRequest"), is(1.99));
          assertThat(items.getJsonObject(2).getDouble("costPerUniqueRequest"), is(3.69));
        }));
  }

  @Test
  public void costPerUseAccessCountPeriod1Y(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("startDate")).thenReturn("2020-04");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    when(routingContext.request().params().get("accessCountPeriod")).thenReturn("1Y");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(2));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(2.22));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(3.73));
          assertThat(json.getDouble("amountPaidTotal"), is(220.0));
          assertThat(json.getDouble("amountEncumberedTotal"), is(200.0));
          assertThat(json.getJsonArray("items").size(), is(4));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("kbId"), is(t11));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("totalItemRequests"), is(5L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("uniqueItemRequests"), is(3L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountEncumbered"), is(50.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountPaid"), is(55.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("publicationYear"), is("1999"));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerTotalRequest"), is(11.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerUniqueRequest"), is(18.33));
          assertThat(json.getJsonArray("items").getJsonObject(1).getLong("totalItemRequests"), is(44L));
          assertThat(json.getJsonArray("items").getJsonObject(1).getLong("uniqueItemRequests"), is(16L));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("amountEncumbered"), is(50.0));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("amountPaid"), is(55.0));
          assertThat(json.getJsonArray("items").getJsonObject(1).getString("publicationYear"), is("2000"));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("costPerTotalRequest"), is(1.25));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("costPerUniqueRequest"), is(3.44));
          assertThat(json.getJsonArray("items").getJsonObject(2).getString("kbId"), is(t12));
          assertThat(json.getJsonArray("items").getJsonObject(2).getLong("totalItemRequests"), is(0L));
          assertThat(json.getJsonArray("items").getJsonObject(2).getLong("uniqueItemRequests"), is(0L));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("amountEncumbered"), is(50.0));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("amountPaid"), is(55.0));
          assertThat(json.getJsonArray("items").getJsonObject(2).getString("publicationYear"), is("1843"));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("costPerTotalRequest"), is(nullValue()));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("costPerUniqueRequest"), is(nullValue()));
          assertThat(json.getJsonArray("items").getJsonObject(3).getString("kbId"), is(t12));
          assertThat(json.getJsonArray("items").getJsonObject(3).getLong("totalItemRequests"), is(50L));
          assertThat(json.getJsonArray("items").getJsonObject(3).getLong("uniqueItemRequests"), is(40L));
          assertThat(json.getJsonArray("items").getJsonObject(3).getDouble("amountEncumbered"), is(50.0));
          assertThat(json.getJsonArray("items").getJsonObject(3).getDouble("amountPaid"), is(55.0));
          assertThat(json.getJsonArray("items").getJsonObject(3).getString("publicationYear"), is("2010"));
          assertThat(json.getJsonArray("items").getJsonObject(3).getDouble("costPerTotalRequest"), is(1.1));
          assertThat(json.getJsonArray("items").getJsonObject(3).getDouble("costPerUniqueRequest"), is(1.38));
        }));
  }

  @Test
  public void costPerUseAccessCountPeriod5Y(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("startDate")).thenReturn("2015-09");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-08");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    when(routingContext.request().params().get("accessCountPeriod")).thenReturn("5Y");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2015 - 2019", "2020 - 2024"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(0, 2));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(null, 2.22));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(null, 3.73));
          assertThat(json.getDouble("amountPaidTotal"), is(220.0));
          assertThat(json.getDouble("amountEncumberedTotal"), is(200.0));
          assertThat(json.getJsonArray("items").size(), is(4));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("kbId"), is(t11));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("totalItemRequests"), is(5L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("uniqueItemRequests"), is(3L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountEncumbered"), is(50.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountPaid"), is(55.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("publicationYear"), is("1999"));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerTotalRequest"), is(11.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerUniqueRequest"), is(18.33));
          assertThat(json.getJsonArray("items").getJsonObject(2).getString("kbId"), is(t12));
          assertThat(json.getJsonArray("items").getJsonObject(2).getLong("totalItemRequests"), is(0L));
          assertThat(json.getJsonArray("items").getJsonObject(2).getLong("uniqueItemRequests"), is(0L));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("amountEncumbered"), is(50.0));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("amountPaid"), is(55.0));
          assertThat(json.getJsonArray("items").getJsonObject(2).getString("publicationYear"), is("1843"));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("costPerTotalRequest"), is(nullValue()));
          assertThat(json.getJsonArray("items").getJsonObject(2).getDouble("costPerUniqueRequest"), is(nullValue()));
        }));
  }

  @Test
  public void costPerUseNoOverlap(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a1);
    when(routingContext.request().params().get("startDate")).thenReturn("2022-01");
    when(routingContext.request().params().get("endDate")).thenReturn("2022-02");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2022-01", "2022-02"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(0, 0));
          assertThat(json.getDouble("amountPaidTotal"), is(0.0));
          assertThat(json.getDouble("amountEncumberedTotal"), is(0.0));
          assertThat(json.getJsonArray("items").size(), is(0));
        }));
  }

  @Test
  public void costPerUseNoItems(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("csv")).thenReturn("true");
    when(routingContext.request().params().get("full")).thenReturn("false");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          String res = body.getValue();
          StringReader reader = new StringReader(res);
          try {
            CSVParser parser = new CSVParser(reader, CsvReports.CSV_FORMAT);
            List<CSVRecord> records = parser.getRecords();
            CSVRecord header = records.get(0);
            CSVRecord totals = records.get(1);
            context.assertEquals("Agreement line", header.get(0));
            context.assertEquals("Print ISSN", header.get(2));
            context.assertEquals("Online ISSN", header.get(3));
            context.assertEquals("ISBN", header.get(4));
            context.assertEquals("Year of publication", header.get(5));
            context.assertEquals("Order type", header.get(6));
            context.assertEquals("Totals", totals.get(0));
            context.assertEquals(2, records.size());
          } catch (IOException e) {
            context.fail(e);
          }
        }));
  }

  @Test
  public void costPerFormatAllCsv(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("csv")).thenReturn("true");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          String res = body.getValue();
          StringReader reader = new StringReader(res);
          try {
            CSVParser parser = new CSVParser(reader, CsvReports.CSV_FORMAT);
            List<CSVRecord> records = parser.getRecords();
            CSVRecord header = records.get(0);
            CSVRecord totals = records.get(1);
            context.assertEquals("Agreement line", header.get(0));
            context.assertEquals("Print ISSN", header.get(2));
            context.assertEquals("Online ISSN", header.get(3));
            context.assertEquals("ISBN", header.get(4));
            context.assertEquals("Year of publication", header.get(5));
            context.assertEquals("Order type", header.get(6));
            context.assertEquals("Totals", totals.get(0));
            context.assertEquals(6, records.size());
            context.assertEquals("Title 21", records.get(2).get(0));
            context.assertEquals("Title 22", records.get(3).get(0));
            context.assertEquals("Title 31", records.get(4).get(0));
            context.assertEquals("Title 32", records.get(5).get(0));
            context.assertEquals("2010", records.get(2).get(5));
            context.assertEquals("", records.get(3).get(5));
            context.assertEquals("0001", records.get(4).get(5));
            context.assertEquals("2010", records.get(5).get(5));
            context.assertEquals("Purchase order line", header.get(7));
            context.assertEquals("p2", records.get(2).get(7));
            context.assertEquals("p2", records.get(3).get(7));
            context.assertEquals("Invoice number", header.get(8));
            context.assertEquals("i2", records.get(2).get(8));
            context.assertEquals("i2", records.get(3).get(8));
            context.assertEquals("Cost per request - total", header.get(17));
            context.assertEquals("1.67", totals.get(17));
            context.assertEquals("0.83", records.get(2).get(17));
            context.assertEquals("0.88", records.get(4).get(17));
            context.assertEquals("17.5", records.get(5).get(17));
            context.assertEquals("Cost per request - unique", header.get(18));
            context.assertEquals("3.33", totals.get(18));
            context.assertEquals("1.67", records.get(2).get(18));
            context.assertEquals("1.75", records.get(4).get(18));
            context.assertEquals("35.0", records.get(5).get(18));
          } catch (IOException e) {
            context.fail(e);
          }
        }));
  }

  @Test
  public void costPerUseFormatBookCsv(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("format")).thenReturn("BOOK");
    when(routingContext.request().params().get("csv")).thenReturn("true");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          String res = body.getValue();
          StringReader reader = new StringReader(res);
          try {
            CSVParser parser = new CSVParser(reader, CsvReports.CSV_FORMAT);
            List<CSVRecord> records = parser.getRecords();
            CSVRecord header = records.get(0);
            CSVRecord totals = records.get(1);
            context.assertEquals("Agreement line", header.get(0));
            context.assertEquals("Totals", totals.get(0));
            context.assertEquals("ISBN", header.get(4));
            context.assertEquals("Year of publication", header.get(5));
            context.assertEquals("Order type", header.get(6));
            context.assertEquals(4, records.size());
            context.assertEquals("Title 31", records.get(2).get(0));
            context.assertEquals("Title 32", records.get(3).get(0));
            context.assertEquals("Cost per request - total", header.get(17));
          } catch (IOException e) {
            context.fail(e);
          }
        }));
  }

  @Test
  public void costPerUseFormatJournalCsv(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("format")).thenReturn("JOURNAL");
    when(routingContext.request().params().get("csv")).thenReturn("true");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          String res = body.getValue();
          StringReader reader = new StringReader(res);
          try {
            CSVParser parser = new CSVParser(reader, CsvReports.CSV_FORMAT);
            List<CSVRecord> records = parser.getRecords();
            CSVRecord header = records.get(0);
            CSVRecord totals = records.get(1);
            context.assertEquals("Agreement line", header.get(0));
            context.assertEquals("Totals", totals.get(0));
            context.assertEquals("Print ISSN", header.get(2));
            context.assertEquals("Online ISSN", header.get(3));
            context.assertEquals("ISBN", header.get(4));
            context.assertEquals("Year of publication", header.get(5));
            context.assertEquals("Order type", header.get(6));
            context.assertEquals(4, records.size());
            context.assertEquals("Title 21", records.get(2).get(0));
            context.assertEquals("42", records.get(2).get(15));
            context.assertEquals("21", records.get(2).get(16));
            context.assertEquals("Title 22", records.get(3).get(0));
            context.assertEquals("", records.get(3).get(15));
            context.assertEquals("", records.get(3).get(16));
            context.assertEquals("Cost per request - total", header.get(17));
          } catch (IOException e) {
            context.fail(e);
          }
        }));
  }

  @Test
  public void costPerUseFormatJournalJson(TestContext context) {
    RoutingContext routingContext = mock(RoutingContext.class, RETURNS_DEEP_STUBS);
    when(routingContext.request().getHeader("X-Okapi-Tenant")).thenReturn(tenant);
    when(routingContext.request().params().get("agreementId")).thenReturn(a2);
    when(routingContext.request().params().get("format")).thenReturn("JOURNAL");
    when(routingContext.request().params().get("startDate")).thenReturn("2020-05");
    when(routingContext.request().params().get("endDate")).thenReturn("2020-06");
    when(routingContext.request().params().get("includeOA")).thenReturn("true");
    new EusageReportsApi(webClient).getCostPerUse(vertx, routingContext)
        .onComplete(context.asyncAssertSuccess(x -> {
          ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
          verify(routingContext.response()).end(body.capture());
          JsonObject json = new JsonObject(body.getValue());
          assertThat((List<?>) json.getJsonArray("accessCountPeriods").getList(),
              contains("2020-05", "2020-06"));
          assertThat((List<?>) json.getJsonArray("titleCountByPeriod").getList(),
              contains(1, 1));
          assertThat((List<?>) json.getJsonArray("totalItemCostsPerRequestsByPeriod").getList(),
              contains(0.44, 8.75));
          assertThat((List<?>) json.getJsonArray("uniqueItemCostsPerRequestsByPeriod").getList(),
              contains(0.88, 17.5));
          assertThat(json.getDouble("amountPaidTotal"), is(70.0));
          assertThat(json.getDouble("amountEncumberedTotal"), is(66.67));
          assertThat(json.getJsonArray("items").size(), is(2));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("kbId"), is(t21));
          assertThat(json.getJsonArray("items").getJsonObject(0).getJsonArray("poLineIDs"), is(new JsonArray().add("p2")));
          assertThat(json.getJsonArray("items").getJsonObject(0).getJsonArray("invoiceNumbers"), is(new JsonArray().add("i2")));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("fiscalDateStart"), is("2020-01-01"));
          assertThat(json.getJsonArray("items").getJsonObject(0).getString("fiscalDateEnd"), is("2020-12-31"));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("totalItemRequests"), is(42L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getLong("uniqueItemRequests"), is(21L));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountEncumbered"), is(33.33));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("amountPaid"), is(35.0));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerTotalRequest"), is(0.83));
          assertThat(json.getJsonArray("items").getJsonObject(0).getDouble("costPerUniqueRequest"), is(1.67));
          assertThat(json.getJsonArray("items").getJsonObject(1).getString("kbId"), is(t22));
          assertThat(json.getJsonArray("items").getJsonObject(1).getJsonArray("poLineIDs"), is(new JsonArray().add("p2")));
          assertThat(json.getJsonArray("items").getJsonObject(1).getJsonArray("invoiceNumbers"), is(new JsonArray().add("i2")));
          assertThat(json.getJsonArray("items").getJsonObject(1).getString("fiscalDateStart"), is("2020-01-01"));
          assertThat(json.getJsonArray("items").getJsonObject(1).getString("fiscalDateEnd"), is("2020-12-31"));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("amountEncumbered"), is(33.33));
          assertThat(json.getJsonArray("items").getJsonObject(1).getDouble("amountPaid"), is(35.0));
        }));
  }

  @Test
  public void reqsByPubYearCsv(TestContext context) {
    new EusageReportsApi(webClient).getReqsByPubYear(pool, true, true, a1, null, "2020-04", "2020-08", "6M", true, true)
        .onComplete(context.asyncAssertSuccess(res -> {
          assertThat(res, containsString(",2020-01 - 2020-06,Controlled,"));
        }));
  }

  @Test
  public void reqsByPubYearWithoutData(TestContext context) {
    getReqsByPubPeriod(true, a1, null, "2999-04", "2999-05", "1Y")
    .onComplete(context.asyncAssertSuccess(json -> {
      assertThat(json.getJsonArray("accessCountPeriods").encode(), is("[]"));
      assertThat(json.getJsonArray("items").size(), is(2));
    }));
  }
}
