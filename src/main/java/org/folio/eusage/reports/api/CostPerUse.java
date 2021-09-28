package org.folio.eusage.reports.api;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.io.IOException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CostPerUse {
  private static final Logger log = LogManager.getLogger(CostPerUse.class);

  static DecimalFormat costDecimalFormat = new DecimalFormat("#.00");

  static JsonObject titlesToJsonObject(RowSet<Row> rowSet, Periods usePeriods) {

    JsonArray totalRequests = new JsonArray();
    JsonArray uniqueRequests = new JsonArray();
    List<Set<UUID>> titlesByPeriod = new ArrayList<>();
    Map<String,JsonObject> totalItems = new HashMap<>();
    Set<UUID> kbIds = new TreeSet<>();
    List<Map<UUID,Double>> paidByPeriodMap = new ArrayList<>();
    for (int i = 0; i < usePeriods.size(); i++) {
      totalRequests.add(0L);
      uniqueRequests.add(0L);
      titlesByPeriod.add(new TreeSet<>());
      paidByPeriodMap.add(new HashMap<>());
    }
    JsonArray items = new JsonArray();
    // determine number of titles in a package
    Map<UUID,Set<UUID>> packageContent = new HashMap<>();
    rowSet.forEach(row -> {
      UUID kbPackageId = row.getUUID("kbpackageid");
      UUID kbId = row.getUUID("kbid");
      packageContent.computeIfAbsent(kbPackageId, x -> new TreeSet<>()).add(kbId);
    });
    Map<UUID,Double> amountEncumberedTotalMap = new HashMap<>();
    Map<UUID,Double> amountPaidTotalMap = new HashMap<>();
    rowSet.forEach(row -> {
      log.info("costPerUse row: {}", () -> row.deepToString());
      UUID kbId = row.getUUID("kbid");
      UUID kbPackageId = row.getUUID("kbpackageid");
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

      String accessType = row.getBoolean("openaccess") ? "OA_Gold" : "Controlled";
      String poLineNumber = row.getString("polinenumber");
      String itemKey = kbId + "," + accessType + "," + poLineNumber;
      JsonObject item = totalItems.get(itemKey);
      titlesByPeriod.get(idx).add(kbId);
      if (item == null) {
        item = new JsonObject();
        totalItems.put(itemKey, item);
        items.add(item);
        item.put("kbId", kbId)
            .put("title", row.getString("title"))
            .put("derivedTitle", kbPackageId != null);
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
      // deal with fiscal year range first, and save the that date range
      String fiscalYearRange = row.getString("fiscalyearrange");
      DateRange subscriptionPeriod = null;
      if (fiscalYearRange != null) {
        subscriptionPeriod = new DateRange(fiscalYearRange);
        item.put("fiscalDateStart", subscriptionPeriod.getStart());
        item.put("fiscalDateEnd", subscriptionPeriod.getEnd());
      }
      // consider subscription date range, Overrides subscription period if present
      String subscriptionDateRange = row.getString("subscriptiondaterange");
      if (subscriptionDateRange != null) {
        subscriptionPeriod = new DateRange(subscriptionDateRange);
        item.put("subscriptionDateStart", subscriptionPeriod.getStart());
        item.put("subscriptionDateEnd", subscriptionPeriod.getEnd());
      }
      if (subscriptionPeriod == null) {
        // neither fiscal, nor subscription dates (shouldn't happen)
        return;
      }
      // number of months in this period
      long thisPeriodMonths = subscriptionPeriod.commonMonths(
          new DateRange(usageStart, usageStart.plusMonths(usePeriods.getMonths())));
      // number of months period in start - end also in subscribed period
      long allPeriodsMonths = subscriptionPeriod.commonMonths(
          new DateRange(usePeriods.startDate, usePeriods.endDate));
      // number of months for subscription
      int subscriptionMonths = subscriptionPeriod.getMonths();
      log.debug("This {} all {} sub {}", thisPeriodMonths, allPeriodsMonths, subscriptionMonths);
      if (thisPeriodMonths == 0) {
        return;
      }
      long totalItemRequestsByPeriod = row.getLong("totalaccesscount");
      totalRequests.set(idx, totalRequests.getLong(idx) + totalItemRequestsByPeriod);
      item.put("totalItemRequests", item.getLong("totalItemRequests")
          + totalItemRequestsByPeriod);
      long uniqueItemRequestsByPeriod = row.getLong("uniqueaccesscount");
      uniqueRequests.set(idx, uniqueRequests.getLong(idx) + uniqueItemRequestsByPeriod);
      item.put("uniqueItemRequests", item.getLong("uniqueItemRequests")
          + uniqueItemRequestsByPeriod);

      UUID paidId = kbPackageId != null ? kbPackageId : kbId;
      int titlesDivide = kbPackageId == null ? 1 : packageContent.get(kbPackageId).size();
      Number encumberedCost = row.getDouble("encumberedcost");
      if (encumberedCost != null) {
        Double amount = allPeriodsMonths * encumberedCost.doubleValue() / subscriptionMonths;
        item.put("amountEncumbered", formatCost(amount / titlesDivide));
        amountEncumberedTotalMap.putIfAbsent(paidId, amount);
      }
      Number amountPaid = row.getNumeric("invoicedcost");
      if (amountPaid != null) {
        Double amount = allPeriodsMonths * amountPaid.doubleValue() / subscriptionMonths;
        Double amountTitle = amount / titlesDivide;
        item.put("amountPaid", formatCost(amountTitle));
        paidByPeriodMap.get(idx).putIfAbsent(paidId, thisPeriodMonths * amountPaid.doubleValue()
            / subscriptionMonths);
        amountPaidTotalMap.putIfAbsent(paidId, amount);
        Long totalItemRequests = item.getLong("totalItemRequests");
        if (totalItemRequests != null && totalItemRequests > 0L) {
          item.put("costPerTotalRequest", formatCost(amountTitle / totalItemRequests));
        }
        Long uniqueItemRequests = item.getLong("uniqueItemRequests");
        if (uniqueItemRequests != null && uniqueItemRequests > 0L) {
          item.put("costPerUniqueRequest", formatCost(amountTitle / uniqueItemRequests));
        }
      }
      JsonArray poLineIDs = item.getJsonArray("poLineIDs");
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
    });
    JsonArray totalItemCostsPerRequestsByPeriod = new JsonArray();
    JsonArray uniqueItemCostsPerRequestsByPeriod = new JsonArray();
    JsonArray totalItemRequestsByPeriod = new JsonArray();
    JsonArray uniqueItemRequestsByPeriod = new JsonArray();
    JsonArray costByPeriod = new JsonArray();
    JsonArray titleCountByPeriod = new JsonArray();
    for (int i = 0; i < usePeriods.size(); i++) {
      titleCountByPeriod.add(titlesByPeriod.get(i).size());
      Double p = 0.0;
      for (Double v : paidByPeriodMap.get(i).values()) {
        p += v;
      }
      costByPeriod.add(formatCost(p));
      Long n = totalRequests.getLong(i);
      totalItemRequestsByPeriod.add(n);
      if (n > 0) {
        log.info("totalItemCostsPerRequestsByPerid {} {}/{}", i, p, n);
        totalItemCostsPerRequestsByPeriod.add(formatCost(p / n));
      } else {
        totalItemCostsPerRequestsByPeriod.addNull();
      }
      n = uniqueRequests.getLong(i);
      uniqueItemRequestsByPeriod.add(n);
      if (n > 0) {
        log.info("uniqueItemCostsPerRequestsByPerid {} {}/{}", i, p, n);
        uniqueItemCostsPerRequestsByPeriod.add(formatCost(p / n));
      } else {
        uniqueItemCostsPerRequestsByPeriod.addNull();
      }
    }
    Double amountEncumberedTotal = 0.0;
    for (Double v : amountEncumberedTotalMap.values()) {
      amountEncumberedTotal += v;
    }
    Double amountPaidTotal = 0.0;
    for (Double v : amountPaidTotalMap.values()) {
      amountPaidTotal += v;
    }
    JsonObject json = new JsonObject();
    json.put("amountEncumberedTotal", formatCost(amountEncumberedTotal));
    json.put("amountPaidTotal", formatCost(amountPaidTotal));
    json.put("accessCountPeriods", usePeriods.getAccessCountPeriods());
    json.put("costByPeriod", costByPeriod);
    json.put("totalItemRequestsByPeriod", totalItemRequestsByPeriod);
    json.put("uniqueItemRequestsByPeriod", uniqueItemRequestsByPeriod);
    json.put("totalItemCostsPerRequestsByPeriod", totalItemCostsPerRequestsByPeriod);
    json.put("uniqueItemCostsPerRequestsByPeriod", uniqueItemCostsPerRequestsByPeriod);
    json.put("titleCountByPeriod", titleCountByPeriod);
    json.put("items", items);
    log.info("costPerUse: JSON {}", () -> json.encodePrettily());
    return json;
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
