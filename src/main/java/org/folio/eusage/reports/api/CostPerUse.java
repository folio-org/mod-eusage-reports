package org.folio.eusage.reports.api;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CostPerUse {
  private static final Logger log = LogManager.getLogger(CostPerUse.class);

  static DecimalFormat costDecimalFormat = new DecimalFormat("#.00");

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

  static void costPerUseRows(
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

  static Number formatCost(Double n) {
    return Double.parseDouble(costDecimalFormat.format(n));
  }
}
