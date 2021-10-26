package org.folio.eusage.reports.api;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.text.DecimalFormat;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

public final class CsvReports {
  static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180;

  static DecimalFormat costDecimalFormat = new DecimalFormat("#.00");

  static Number formatCost(Double n) {
    return Double.parseDouble(costDecimalFormat.format(n));
  }

  private CsvReports() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  static String getUseOverTime2Csv(
      JsonObject json, boolean groupByPublicationYear,
      boolean periodOfUse) {

    StringWriter stringWriter = new StringWriter();
    try {
      CSVPrinter writer = new CSVPrinter(stringWriter, CSV_FORMAT);
      getUseOverTime2Csv(json, groupByPublicationYear, periodOfUse, writer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return stringWriter.toString();
  }

  private static void getUseOverTime2Csv(JsonObject json, boolean groupByPublicationYear,
      boolean periodOfUse, CSVPrinter writer) throws IOException {

    writer.print("Title");
    writer.print("Print ISSN");
    writer.print("Online ISSN");
    writer.print("ISBN");
    if (periodOfUse) {
      writer.print("Period of use");
    }
    if (groupByPublicationYear) {
      writer.print("Year of publication");
    }
    writer.print("Access type");
    writer.print("Metric Type");
    writer.print("Reporting period total");
    JsonArray accessCountPeriods = json.getJsonArray("accessCountPeriods");
    for (int i = 0; i < accessCountPeriods.size(); i++) {
      writer.print(accessCountPeriods.getString(i));
    }
    writer.println();

    getUseTotalsCsv(json, groupByPublicationYear, periodOfUse, writer, "total");
    getUseTotalsCsv(json, groupByPublicationYear, periodOfUse, writer, "unique");

    JsonArray items = json.getJsonArray("items");
    if (items == null) {
      return;
    }
    for (int j = 0; j < items.size(); j++) {
      JsonObject item = items.getJsonObject(j);
      writer.print(item.getString("title"));
      writer.print(item.getString("printISSN"));
      writer.print(item.getString("onlineISSN"));
      writer.print(item.getString("ISBN"));
      if (groupByPublicationYear) {
        writer.print(item.getString("publicationYear"));
      }
      if (periodOfUse) {
        writer.print(item.getString("periodOfUse"));
      }
      writer.print(item.getString("accessType"));
      writer.print(item.getString("metricType"));
      writer.print(item.getLong("accessCountTotal"));
      JsonArray accessCountsByPeriod = item.getJsonArray("accessCountsByPeriod");
      for (int i = 0; i < accessCountsByPeriod.size(); i++) {
        writer.print(accessCountsByPeriod.getLong(i));
      }
      writer.println();
    }
  }

  private static void getUseTotalsCsv(JsonObject json, boolean groupByPublicationYear,
      boolean periodOfUse, CSVPrinter writer, String lead) throws IOException {

    writer.print("Totals - " + lead + " item requests");
    writer.print(null);
    writer.print(null);
    writer.print(null);
    if (periodOfUse) {
      writer.print(null);
    }
    if (groupByPublicationYear) {
      writer.print(null);
    }
    writer.print(null);
    writer.print(null);
    writer.print(json.getLong(lead + "ItemRequestsTotal"));
    JsonArray totalItemRequestsPeriod = json.getJsonArray(lead + "ItemRequestsByPeriod");
    for (int i = 0; i < totalItemRequestsPeriod.size(); i++) {
      writer.print(totalItemRequestsPeriod.getLong(i));
    }
    writer.println();
  }

  static String getCostPerUse2Csv(JsonObject json) {
    StringWriter stringWriter = new StringWriter();
    try {
      CSVPrinter writer = new CSVPrinter(stringWriter, CSV_FORMAT);
      getCostPerUse2Csv(json, writer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return stringWriter.toString();
  }

  private static void getCostPerUse2Csv(JsonObject json, CSVPrinter writer) throws IOException {
    writer.print("Agreement line");
    writer.print("Derived Title");
    writer.print("Print ISSN");
    writer.print("Online ISSN");
    writer.print("ISBN");
    writer.print("Year of publication");
    writer.print("Order type");
    writer.print("Purchase order line");
    writer.print("Invoice number");
    writer.print("Fiscal start");
    writer.print("Fiscal end");
    writer.print("Subscription start");
    writer.print("Subscription end");
    writer.print("Amount encumbered");
    writer.print("Amount paid");
    writer.print("Total item requests");
    writer.print("Unique item requests");
    writer.print("Cost per request - total");
    writer.print("Cost per request - unique");
    writer.println();

    writer.print("Totals"); // agreement line
    writer.print(null); // publication type
    writer.print(null); // print issn
    writer.print(null); // online ISSN
    writer.print(null); // ISBN
    writer.print(null); // year of publication
    writer.print(null); // Order type
    writer.print(null); // Purchase order line
    writer.print(null); // Invoice number
    writer.print(null);  // fiscal year start
    writer.print(null);  // fiscal year end
    writer.print(null);  // subscription date start
    writer.print(null);  // subscription date end
    writer.print(json.getDouble("amountEncumberedTotal"));
    Double amountPaidTotal = json.getDouble("amountPaidTotal");
    writer.print(amountPaidTotal == null ? null : formatCost(amountPaidTotal));
    JsonArray items = json.getJsonArray("items");
    if (items == null) {
      return;
    }
    long totalItemRequests = getTotalInLongArray(items, "totalItemRequests");
    writer.print(totalItemRequests);
    long uniqueItemRequests = getTotalInLongArray(items, "uniqueItemRequests");
    writer.print(uniqueItemRequests);
    writer.print(totalItemRequests == 0 || amountPaidTotal == null ? null
        : formatCost(amountPaidTotal / totalItemRequests));
    writer.print(uniqueItemRequests == 0 || amountPaidTotal == null ? null
        : formatCost(amountPaidTotal / uniqueItemRequests));
    writer.println();

    for (int i = 0; i < items.size(); i++) {
      JsonObject item = items.getJsonObject(i);
      writer.print(item.getString("title"));
      writer.print(Boolean.TRUE.equals(item.getBoolean("derivedTitle")) ? "Y" : "N");
      writer.print(item.getString("printISSN"));
      writer.print(item.getString("onlineISSN"));
      writer.print(item.getString("ISBN"));
      writer.print(item.getString("publicationYear"));
      writer.print(item.getString("orderType"));
      writer.print(orderLinesToString(item.getJsonArray("poLineIDs")));
      writer.print(orderLinesToString(item.getJsonArray("invoiceNumbers")));
      writer.print(item.getString("fiscalDateStart"));
      writer.print(item.getString("fiscalDateEnd"));
      writer.print(item.getString("subscriptionDateStart"));
      writer.print(item.getString("subscriptionDateEnd"));
      writer.print(item.getString("amountEncumbered"));
      writer.print(item.getString("amountPaid"));
      writer.print(item.getLong("totalItemRequests"));
      writer.print(item.getLong("uniqueItemRequests"));
      writer.print(item.getDouble("costPerTotalRequest"));
      writer.print(item.getDouble("costPerUniqueRequest"));
      writer.println();
    }
  }

  private static long getTotalInLongArray(JsonArray ar, String key) {
    long n = 0;
    for (int i = 0; i < ar.size(); i++) {
      n += ar.getJsonObject(i).getLong(key, 0L);
    }
    return n;
  }

  private static String orderLinesToString(JsonArray ar) {
    if (ar == null) {
      return null;
    }
    return String.join(" ", ar.getList());
  }

}
