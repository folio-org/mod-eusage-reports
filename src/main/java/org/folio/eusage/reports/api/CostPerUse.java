package org.folio.eusage.reports.api;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CostPerUse {
  private static final Logger log = LogManager.getLogger(CostPerUse.class);

  static DecimalFormat costDecimalFormat = new DecimalFormat("#.00");

  static JsonObject titlesToJsonObject2(RowSet<Row> rowSet, Periods usePeriods) {

    JsonArray paidByPeriod = new JsonArray();
    JsonArray totalRequests = new JsonArray();
    JsonArray uniqueRequests = new JsonArray();
    JsonArray titleCountByPeriod = new JsonArray();
    Map<String,JsonObject> totalItems = new HashMap<>();
    Set<UUID> kbIds = new TreeSet<>();
    for (int i = 0; i < usePeriods.size(); i++) {
      paidByPeriod.add(0.0);
      totalRequests.add(0L);
      uniqueRequests.add(0L);
      titleCountByPeriod.add(0L);
    }
    JsonArray items = new JsonArray();
    rowSet.forEach(row -> {
      log.info("AD: {}", row.deepToString());
      UUID kbId = row.getUUID("kbid");
      String usageDateRange = row.getString("usagedaterange");
      if (usageDateRange == null) {
        if (kbIds.add(kbId)) {
          JsonObject item = new JsonObject()
              .put("kbId", kbId)
              .put("title", row.getString("title"))
              .put("derivedTitle", row.getUUID("kbpackageid") != null);
          items.add(item);
        }
        return;
      }
      kbIds.add(kbId);
      LocalDate usageStart = usePeriods.floorMonths(LocalDate.parse(
          usageDateRange.substring(1, 11)));
      int idx = usePeriods.getPeriodEntry(usageStart);

      LocalDate publicationDate = row.getLocalDate("publicationdate");
      String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, 12, "nopub");
      String accessType = row.getBoolean("openaccess") ? "OA_Gold" : "Controlled";
      String itemKey = kbId + "," + pubPeriodLabel + "," + accessType;
      JsonObject item = totalItems.get(itemKey);
      if (item == null) {
        item = new JsonObject();
        totalItems.put(itemKey, item);
        items.add(item);
        item.put("kbId", kbId)
            .put("title", row.getString("title"))
            .put("derivedTitle", row.getUUID("kbpackageid") != null);
        String printIssn = row.getString("printissn");
        if (printIssn != null) {
          item.put("printISSN", printIssn);
        }
        String onlineIssn = row.getString("onlineissn");
        if (onlineIssn != null) {
          item.put("onlineISSN", onlineIssn);
        }
        String isbn = row.getString("isbn");
        if (isbn != null) {
          item.put("ISBN", isbn);
        }
        String orderType = row.getString("ordertype");
        item.put("orderType", orderType != null ? orderType : "Ongoing");

        item.put("poLineIDs", new JsonArray());
        item.put("invoiceNumbers", new JsonArray());

        item.put("amountPaid", 0.0);
        item.put("amountEncumbered", 0.0);
        item.put("totalItemRequests", 0L);
        item.put("uniqueItemRequests", 0L);
      }
      Long totalItemRequestsByPeriod = row.getLong("totalaccesscount");
      if (totalItemRequestsByPeriod != null) {
        totalRequests.set(idx, totalRequests.getLong(idx) + totalItemRequestsByPeriod);
        item.put("uniqueItemRequests", item.getLong("uniqueItemRequests")
            + totalItemRequestsByPeriod);
      }
      Long uniqueItemRequestsByPeriod = row.getLong("uniqueaccesscount");
      if (uniqueItemRequestsByPeriod != null) {
        uniqueRequests.set(idx, uniqueRequests.getLong(idx) + uniqueItemRequestsByPeriod);
        item.put("totalItemRequests", item.getLong("totalItemRequests")
            + uniqueItemRequestsByPeriod);
      }
      String fiscalYearRange = row.getString("fiscalyearrange");
      int subscriptionMonths = 0;
      if (fiscalYearRange != null) {
        DateRange dateRange = new DateRange(fiscalYearRange);
        item.put("fiscalDateStart", dateRange.getStart());
        item.put("fiscalDateEnd", dateRange.getEnd());
        subscriptionMonths = dateRange.getMonths();
      }
      String subscriptionDateRange = row.getString("subscriptiondaterange");
      if (subscriptionDateRange != null) {
        DateRange dateRange = new DateRange(subscriptionDateRange);
        item.put("subscriptionDateStart", dateRange.getStart());
        item.put("subscriptionDateEnd", dateRange.getEnd());
        subscriptionMonths = dateRange.getMonths();
      }
      int monthsInOnePeriod = usePeriods.getMonths();
      if (monthsInOnePeriod > subscriptionMonths) {
        subscriptionMonths = monthsInOnePeriod; // never more than full amount
      }
      int monthsAllPeriods = monthsInOnePeriod * usePeriods.size();
      if (monthsAllPeriods > subscriptionMonths) {
        monthsAllPeriods = subscriptionMonths;
      }

      Number amountPaid = row.getNumeric("invoicedcost");
      if (amountPaid != null) {
        item.put("amountPaid", item.getDouble("amountPaid")
            + amountPaid.doubleValue());
        paidByPeriod.set(idx, monthsInOnePeriod * amountPaid.doubleValue() / subscriptionMonths);

      }
      Number encumberedCost = row.getNumeric("encumberedcost");
      if (encumberedCost != null) {
        item.put("amountEncumbered", item.getDouble("amountEncumbered")
            + encumberedCost.doubleValue());
      }
      JsonArray poLineIDs = item.getJsonArray("poLineIDs");
      String poLineNumber = row.getString("polinenumber");
      if (poLineNumber != null) {
        if (!poLineIDs.contains(poLineNumber)) {
          poLineIDs.add(poLineNumber);
        }
      }
      String invoiceNumber = row.getString("invoicenumber");
      if (invoiceNumber != null) {
        JsonArray invoiceNumbers = item.getJsonArray("invoiceNumbers");
        if (!invoiceNumbers.contains(invoiceNumber)) {
          invoiceNumbers.add(invoiceNumber);
        }
      }
      titleCountByPeriod.set(idx, titleCountByPeriod.getLong(idx) + 1);
    });
    JsonArray totalItemCostsPerRequestsByPeriod = new JsonArray();
    JsonArray uniqueItemCostsPerRequestsByPeriod = new JsonArray();
    for (int i = 0; i < usePeriods.size(); i++) {
      Double p = paidByPeriod.getDouble(i);
      Long n = totalRequests.getLong(i);
      if (n > 0) {
        log.info("totalItemCostsPerRequestsByPerid {} {}/{}", i, p, n);
        totalItemCostsPerRequestsByPeriod.add(formatCost(p / n));
      } else {
        totalItemCostsPerRequestsByPeriod.addNull();
      }
      n = uniqueRequests.getLong(i);
      if (n > 0) {
        log.info("uniqueItemCostsPerRequestsByPerid {} {}/{}", i, p, n);
        uniqueItemCostsPerRequestsByPeriod.add(formatCost(p / n));
      } else {
        uniqueItemCostsPerRequestsByPeriod.addNull();
      }
    }
    JsonObject json = new JsonObject();
    json.put("items", items);
    json.put("accessCountPeriods", usePeriods.getAccessCountPeriods());
    json.put("totalItemCostsPerRequestsByPeriod", totalItemCostsPerRequestsByPeriod);
    json.put("uniqueItemCostsPerRequestsByPeriod", uniqueItemCostsPerRequestsByPeriod);
    json.put("titleCountByPeriod", titleCountByPeriod);
    log.info("AD: new JSON {}", json.encodePrettily());
    return json;
  }


  static JsonObject titlesToJsonObject(RowSet<Row> rowSet, Periods periods) {
    JsonArray paidByPeriod = new JsonArray();
    JsonArray totalRequests = new JsonArray();
    JsonArray uniqueRequests = new JsonArray();
    JsonArray titleCountByPeriod = new JsonArray();
    for (int i = 0; i < periods.size(); i++) {
      paidByPeriod.add(0.0);
      totalRequests.add(0L);
      uniqueRequests.add(0L);
      titleCountByPeriod.add(0L);
    }
    rowSet.forEach(row -> {
      for (int i = 0; i < periods.size(); i++) {
        Long totalAccessCount = row.getLong("total" + i);
        Long uniqueAccessCount = row.getLong("unique" + i);
        log.debug("Inspecting i={} totalAccessCount={} uniqueAccessCount={}",
            i, totalAccessCount, uniqueAccessCount);
        titleCountByPeriod.set(i, titleCountByPeriod.getLong(i) + 1L);
      }
    });
    AtomicLong totalTitles = new AtomicLong();
    for (int i = 0; i < periods.size(); i++) {
      totalTitles.addAndGet(titleCountByPeriod.getLong(i));
    }
    JsonObject json = new JsonObject();
    costPerUseRows(rowSet, json, periods, totalRequests, uniqueRequests, paidByPeriod);
    JsonArray totalItemCostsPerRequestsByPeriod = new JsonArray();
    JsonArray uniqueItemCostsPerRequestsByPeriod = new JsonArray();
    for (int i = 0; i < periods.size(); i++) {
      Double p = paidByPeriod.getDouble(i);
      Long n = totalRequests.getLong(i);
      if (n > 0) {
        log.info("totalItemCostsPerRequestsByPerid {} {}/{}", i, p, n);
        totalItemCostsPerRequestsByPeriod.add(formatCost(p / n));
      } else {
        totalItemCostsPerRequestsByPeriod.addNull();
      }
      n = uniqueRequests.getLong(i);
      if (n > 0) {
        log.info("uniqueItemCostsPerRequestsByPerid {} {}/{}", i, p, n);
        uniqueItemCostsPerRequestsByPeriod.add(formatCost(p / n));
      } else {
        uniqueItemCostsPerRequestsByPeriod.addNull();
      }
    }
    json.put("accessCountPeriods", periods.getAccessCountPeriods());
    json.put("totalItemCostsPerRequestsByPeriod", totalItemCostsPerRequestsByPeriod);
    json.put("uniqueItemCostsPerRequestsByPeriod", uniqueItemCostsPerRequestsByPeriod);
    json.put("titleCountByPeriod", titleCountByPeriod);
    return json;
  }

  private static void costPerUseRows(
      RowSet<Row> rowSet, JsonObject json, Periods periods,
      JsonArray totalRequests, JsonArray uniqueRequests, JsonArray paidByPeriod) {

    JsonArray items = new JsonArray();
    long totalTitles = rowSet.rowCount();
    rowSet.forEach(row -> {
      log.info("cost per use row={}", row.deepToString());
      JsonObject item = new JsonObject()
          .put("kbId", row.getUUID("kbid"))
          .put("title", row.getString("title"))
          .put("derivedTitle", row.getUUID("kbpackageid") != null);

      String printIssn = row.getString("printissn");
      if (printIssn != null) {
        item.put("printISSN", printIssn);
      }
      String onlineIssn = row.getString("onlineissn");
      if (onlineIssn != null) {
        item.put("onlineISSN", onlineIssn);
      }
      String isbn = row.getString("isbn");
      if (isbn != null) {
        item.put("ISBN", isbn);
      }
      String orderType = row.getString("ordertype");
      item.put("orderType", orderType != null ? orderType : "Ongoing");
      String poLineNumber = row.getString("polinenumber");
      if (poLineNumber != null) {
        item.put("poLineIDs", new JsonArray().add(poLineNumber));
      }
      String invoiceNumbers = row.getString("invoicenumber");
      if (invoiceNumbers != null) {
        item.put("invoiceNumbers", new JsonArray().add(invoiceNumbers));
      }
      String fiscalYearRange = row.getString("fiscalyearrange");
      int subscriptionMonths = 0;
      if (fiscalYearRange != null) {
        DateRange dateRange = new DateRange(fiscalYearRange);
        item.put("fiscalDateStart", dateRange.getStart());
        item.put("fiscalDateEnd", dateRange.getEnd());
        subscriptionMonths = dateRange.getMonths();
      }
      String subscriptionDateRange = row.getString("subscriptiondaterange");
      if (subscriptionDateRange != null) {
        DateRange dateRange = new DateRange(subscriptionDateRange);
        item.put("subscriptionDateStart", dateRange.getStart());
        item.put("subscriptionDateEnd", dateRange.getEnd());
        subscriptionMonths = dateRange.getMonths();
      }
      long totalItemRequests = 0L;
      long uniqueItemRequests = 0L;
      for (int i = 0; i < periods.size(); i++) {
        Long totalItemRequestsByPeriod = row.getLong("total" + i);
        if (totalItemRequestsByPeriod != null) {
          totalItemRequests += totalItemRequestsByPeriod;
          totalRequests.set(i, totalRequests.getLong(i) + totalItemRequestsByPeriod);
        }
        Long uniqueItemRequestsByPeriod = row.getLong("unique" + i);
        if (uniqueItemRequestsByPeriod != null) {
          uniqueItemRequests += uniqueItemRequestsByPeriod;
          uniqueRequests.set(i, uniqueRequests.getLong(i) + uniqueItemRequestsByPeriod);
        }
      }
      item.put("totalItemRequests", totalItemRequests);
      item.put("uniqueItemRequests", uniqueItemRequests);

      int monthsInOnePeriod = periods.getMonths();
      if (monthsInOnePeriod > subscriptionMonths) {
        subscriptionMonths = monthsInOnePeriod; // never more than full amount
      }
      int monthsAllPeriods = monthsInOnePeriod * periods.size();
      if (monthsAllPeriods > subscriptionMonths) {
        monthsAllPeriods = subscriptionMonths;
      }
      Number encumberedCost = row.getNumeric("encumberedcost");
      if (encumberedCost != null) {
        item.put("amountEncumbered", formatCost(
            encumberedCost.doubleValue() / totalTitles));
        json.put("amountEncumberedTotal",
            formatCost(monthsAllPeriods * encumberedCost.doubleValue() / subscriptionMonths));
      }
      Number amountPaid = row.getNumeric("invoicedcost");
      if (amountPaid != null) {
        for (int i = 0; i < periods.size(); i++) {
          paidByPeriod.set(i, monthsInOnePeriod * amountPaid.doubleValue() / subscriptionMonths);
        }
        double paidByTitle = amountPaid.doubleValue() / totalTitles;
        item.put("amountPaid", formatCost(paidByTitle));
        json.put("amountPaidTotal",
            formatCost(monthsAllPeriods * amountPaid.doubleValue() / subscriptionMonths));
        if (totalItemRequests != 0L) {
          item.put("costPerTotalRequest",
              formatCost(paidByTitle / totalItemRequests));
        }
        if (uniqueItemRequests != 0L) {
          item.put("costPerUniqueRequest", formatCost(paidByTitle / uniqueItemRequests));
        }
      }
      items.add(item);
    });
    json.put("items", items);
  }

  private static Number formatCost(Double n) {
    return Double.parseDouble(costDecimalFormat.format(n));
  }

  static void getCostPerUse2Csv(JsonObject json, CSVPrinter writer) throws IOException {
    writer.print("Agreement line");
    writer.print("Derived Title");
    writer.print("Print ISSN");
    writer.print("Online ISSN");
    writer.print("ISBN");
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
      writer.print(item.getBoolean("derivedTitle") ? "Y" : "N");
      writer.print(item.getString("printISSN"));
      writer.print(item.getString("onlineISSN"));
      writer.print(item.getString("ISBN"));
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
      n += ar.getJsonObject(i).getLong(key);
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
