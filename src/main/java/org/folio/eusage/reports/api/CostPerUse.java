package org.folio.eusage.reports.api;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class CostPerUse {
  private static final Logger log = LogManager.getLogger(CostPerUse.class);

  private CostPerUse() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  static JsonObject titlesToJsonObject(RowSet<Row> rowSet, Periods usePeriods) {

    JsonArray totalRequests = new JsonArray();
    JsonArray uniqueRequests = new JsonArray();
    List<Set<UUID>> titlesByPeriod = new ArrayList<>();
    Map<String,JsonObject> totalItems = new HashMap<>();
    List<Map<UUID,Double>> paidByPeriodMap = new ArrayList<>();
    Map<String,Set<String>> titlesInSubscription = new HashMap<>();
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
      String fiscalYearRange = row.getString("fiscalyearrange");
      String subscriptionDateRange = row.getString("subscriptiondaterange");
      // deal with fiscal year range first, and save the that date range
      DateRange subscriptionPeriod = null;
      if (fiscalYearRange != null) {
        subscriptionPeriod = new DateRange(fiscalYearRange);
      }
      // consider subscription date range, Overrides subscription period if present
      if (subscriptionDateRange != null) {
        subscriptionPeriod = new DateRange(subscriptionDateRange);
      }
      if (subscriptionPeriod == null) {
        return;
      }
      String poLineNumber = row.getString("polinenumber");
      String payKey = kbId + "," + poLineNumber + "," + fiscalYearRange
          + "," + subscriptionDateRange;
      LocalDate publicationDate = row.getLocalDate("publicationdate");
      String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, 12,"nopub");
      if (titlesInSubscription.containsKey(payKey) && "nopub".equals(pubPeriodLabel)) {
        return;
      }
      titlesInSubscription.putIfAbsent(payKey, new TreeSet<>());
      titlesInSubscription.get(payKey).add(pubPeriodLabel);
    });
    Map<String,Double> amountEncumberedTotalMap = new HashMap<>();
    Map<String,Double> amountPaidTotalMap = new HashMap<>();
    rowSet.forEach(row -> {
      log.debug("costPerUse row: {}", row::deepToString);
      final UUID kbPackageId = row.getUUID("kbpackageid");
      final String orderType = row.getString("ordertype");
      final String fiscalYearRange = row.getString("fiscalyearrange");
      final String subscriptionDateRange = row.getString("subscriptiondaterange");
      // deal with fiscal year range first, and save the that date range
      DateRange subscriptionPeriod = null;
      if (fiscalYearRange != null) {
        subscriptionPeriod = new DateRange(fiscalYearRange);
      }
      // consider subscription date range, Overrides subscription period if present
      if (subscriptionDateRange != null) {
        subscriptionPeriod = new DateRange(subscriptionDateRange);
      }
      if (subscriptionPeriod == null) {
        return;
      }
      LocalDate publicationDate = row.getLocalDate("publicationdate");
      String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, 12,"nopub");
      String poLineNumber = row.getString("polinenumber");
      UUID kbId = row.getUUID("kbid");
      String payKey = kbId + "," + poLineNumber + "," + fiscalYearRange + ","
          + subscriptionDateRange;
      if (!titlesInSubscription.get(payKey).contains(pubPeriodLabel)) {
        return;
      }
      String itemKey = payKey + "," + pubPeriodLabel;
      UUID paidId = kbPackageId != null ? kbPackageId : kbId;
      int titlesDivide = titlesInSubscription.get(payKey).size();
      if (kbPackageId != null) {
        titlesDivide *= packageContent.get(kbPackageId).size();
      }
      // number of months period in start - end also in subscribed period
      long allPeriodsMonths = subscriptionPeriod.commonMonths(
          new DateRange(usePeriods.startDate, usePeriods.endDate));
      if (allPeriodsMonths <= 0) {
        return;
      }
      // at this point, since allPeriodsMonths > 0, then subscriptionMonths > 0
      int subscriptionMonths = subscriptionPeriod.getMonths();
      Number encumberedCost = row.getDouble("encumberedcost");
      Double amountEncumbered = null;
      if (encumberedCost != null) {
        double amount = allPeriodsMonths * encumberedCost.doubleValue() / subscriptionMonths;
        amountEncumbered = amount / titlesDivide;
        amountEncumberedTotalMap.putIfAbsent(itemKey, amountEncumbered);
      }
      Number invoicedCost = row.getNumeric("invoicedcost");
      Double amountPaid = null;
      if (invoicedCost != null) {
        double amount = allPeriodsMonths * invoicedCost.doubleValue() / subscriptionMonths;
        amountPaid = amount / titlesDivide;
        amountPaidTotalMap.putIfAbsent(itemKey, amountPaid);
      }
      final String usageDateRange = row.getString("usagedaterange");
      JsonObject item = totalItems.get(itemKey);
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
        item.put("orderType", orderType != null ? orderType : "Ongoing");

        JsonArray poLineIDs = new JsonArray();
        if (poLineNumber != null) {
          poLineIDs.add(poLineNumber);
        }
        item.put("poLineIDs", poLineIDs);

        JsonArray invoiceNumbers = new JsonArray();
        String invoiceNumber = row.getString("invoicenumber");
        if (invoiceNumber != null) {
          invoiceNumbers.add(invoiceNumber);
        }
        item.put("invoiceNumbers", invoiceNumbers);
        if (usageDateRange != null) {
          item.put("amountPaid", 0.0);
          item.put("amountEncumbered", 0.0);
          item.put("totalItemRequests", 0L);
          item.put("uniqueItemRequests", 0L);
        }
        if (fiscalYearRange != null) {
          DateRange tmp = new DateRange(fiscalYearRange);
          item.put("fiscalDateStart", tmp.getStart());
          item.put("fiscalDateEnd", tmp.getEnd());
        }
        // consider subscription date range, Overrides subscription period if present
        if (subscriptionDateRange != null) {
          DateRange tmp = new DateRange(subscriptionDateRange);
          item.put("subscriptionDateStart", tmp.getStart());
          item.put("subscriptionDateEnd", tmp.getEnd());
        }
        if (!"nopub".equals(pubPeriodLabel)) {
          item.put("publicationYear", pubPeriodLabel);
        }
        if (amountPaid != null) {
          item.put("amountPaid", CsvReports.formatCost(amountPaid));
        }
        if (amountEncumbered != null) {
          item.put("amountEncumbered", CsvReports.formatCost(amountEncumbered));
        }
      }
      if (usageDateRange == null) {
        // or no counter report data
        return;
      }
      LocalDate usageStart = usePeriods.floorMonths(LocalDate.parse(
          usageDateRange.substring(1, 11)));
      int idx = usePeriods.getPeriodEntry(usageStart);
      titlesByPeriod.get(idx).add(kbId);

      // number of months in this period
      long thisPeriodMonths = subscriptionPeriod.commonMonths(
          new DateRange(usageStart, usageStart.plusMonths(usePeriods.getMonths())));
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

      if (invoicedCost != null) {
        double amount = allPeriodsMonths * invoicedCost.doubleValue() / subscriptionMonths;
        Double amountTitle = amount / titlesDivide;
        paidByPeriodMap.get(idx).putIfAbsent(paidId, thisPeriodMonths * invoicedCost.doubleValue()
            / subscriptionMonths);
        Long totalItemRequests = item.getLong("totalItemRequests");
        if (totalItemRequests != null && totalItemRequests > 0L) {
          item.put("costPerTotalRequest", CsvReports.formatCost(amountTitle / totalItemRequests));
        }
        Long uniqueItemRequests = item.getLong("uniqueItemRequests");
        if (uniqueItemRequests != null && uniqueItemRequests > 0L) {
          item.put("costPerUniqueRequest", CsvReports.formatCost(amountTitle / uniqueItemRequests));
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
      costByPeriod.add(CsvReports.formatCost(p));
      Long n = totalRequests.getLong(i);
      totalItemRequestsByPeriod.add(n);
      if (n > 0) {
        totalItemCostsPerRequestsByPeriod.add(CsvReports.formatCost(p / n));
      } else {
        totalItemCostsPerRequestsByPeriod.addNull();
      }
      n = uniqueRequests.getLong(i);
      uniqueItemRequestsByPeriod.add(n);
      if (n > 0) {
        uniqueItemCostsPerRequestsByPeriod.add(CsvReports.formatCost(p / n));
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
    json.put("amountEncumberedTotal", CsvReports.formatCost(amountEncumberedTotal));
    json.put("amountPaidTotal", CsvReports.formatCost(amountPaidTotal));
    json.put("accessCountPeriods", usePeriods.getAccessCountPeriods());
    json.put("costByPeriod", costByPeriod);
    json.put("totalItemRequestsByPeriod", totalItemRequestsByPeriod);
    json.put("uniqueItemRequestsByPeriod", uniqueItemRequestsByPeriod);
    json.put("totalItemCostsPerRequestsByPeriod", totalItemCostsPerRequestsByPeriod);
    json.put("uniqueItemCostsPerRequestsByPeriod", uniqueItemCostsPerRequestsByPeriod);
    json.put("titleCountByPeriod", titleCountByPeriod);
    json.put("items", items);
    log.debug("costPerUse: JSON {}", json::encodePrettily);
    return json;
  }

}
