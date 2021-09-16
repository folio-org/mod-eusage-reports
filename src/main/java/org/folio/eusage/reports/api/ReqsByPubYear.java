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
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReqsByPubYear {
  private static final Logger log = LogManager.getLogger(ReqsByPubYear.class);

  static JsonObject titlesToJsonObject(
      RowSet<Row> rowSet, Boolean isJournal, String agreementId,
      Periods usePeriods, int pubPeriodInMonths) {
    List<Long> totalItemRequestsByPeriod = new ArrayList<>();
    JsonArray totalRequestsPeriodsOfUseByPeriod = new JsonArray();
    List<Long> uniqueItemRequestsByPeriod = new ArrayList<>();
    JsonArray uniqueRequestsPeriodsOfUseByPeriod = new JsonArray();
    JsonArray items = new JsonArray();
    Map<String,JsonObject> totalItems = new HashMap<>();
    Map<String,JsonObject> uniqueItems = new HashMap<>();
    Set<String> dup = new TreeSet<>();
    SortedSet<String> pubPeriodsSet = new TreeSet<>();

    rowSet.forEach(row -> {
      LocalDate publicationDate = row.getLocalDate("publicationdate");
      String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, pubPeriodInMonths, "nopub");
      pubPeriodsSet.add(pubPeriodLabel);
    });
    Map<String,Integer> pubYearIndexMap = new HashMap<>();
    JsonArray accessCountsPeriods = new JsonArray();
    int numPubPeriods = 0;
    for (String p : pubPeriodsSet) {
      pubYearIndexMap.put(p, numPubPeriods++);
      totalItemRequestsByPeriod.add(0L);
      uniqueItemRequestsByPeriod.add(0L);
      totalRequestsPeriodsOfUseByPeriod.add(new JsonObject());
      uniqueRequestsPeriodsOfUseByPeriod.add(new JsonObject());
      accessCountsPeriods.add(p);
    }
    rowSet.forEach(row -> {
      log.info("AD: 2 {}", row.deepToString());
      String usageDateRange = row.getString("usagedaterange");
      Long totalAccessCount = row.getLong("totalaccesscount");
      Long uniqueAccessCount = row.getLong("uniqueaccesscount");
      if (usageDateRange != null && totalAccessCount > 0L) {
        LocalDate usageStart = usePeriods.floorMonths(LocalDate.parse(
            usageDateRange.substring(1, 11)));
        final String usePeriodLabel = usePeriods.periodLabel(usageStart);

        LocalDate publicationDate = row.getLocalDate("publicationdate");
        String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, pubPeriodInMonths,
            "nopub");
        int idx = pubYearIndexMap.get(pubPeriodLabel);

        pubPeriodsSet.add(pubPeriodLabel);
        String accessType = row.getBoolean("openaccess") ? "OA_Gold" : "Controlled";
        String itemKey = row.getUUID("kbid").toString() + "," + usePeriodLabel + "," + accessType;
        String dupKey = itemKey + "," + publicationDate + "," + usageDateRange;
        if (!dup.add(dupKey)) {
          return;
        }
        totalItemRequestsByPeriod.set(idx, totalAccessCount
            + totalItemRequestsByPeriod.get(idx));

        uniqueItemRequestsByPeriod.set(idx, uniqueAccessCount
            + uniqueItemRequestsByPeriod.get(idx));

        JsonObject o = totalRequestsPeriodsOfUseByPeriod.getJsonObject(idx);
        Long totalAccessCountPeriod = o.getLong(usePeriodLabel, 0L);
        o.put(usePeriodLabel, totalAccessCountPeriod + totalAccessCount);

        o = uniqueRequestsPeriodsOfUseByPeriod.getJsonObject(idx);
        Long uniqueAccessCountPeriod = o.getLong(usePeriodLabel, 0L);
        o.put(usePeriodLabel, uniqueAccessCountPeriod + uniqueAccessCount);

        JsonObject totalItem = totalItems.get(itemKey);
        JsonArray accessCountsByPeriod;
        if (totalItem != null) {
          accessCountsByPeriod = totalItem.getJsonArray("accessCountsByPeriod");
          totalItem.put("accessCountTotal", totalAccessCount
              + totalItem.getLong("accessCountTotal"));
        } else {
          totalItem = new JsonObject()
              .put("kbId", row.getUUID("kbid"))
              .put("title", row.getString("title"));
          if (isJournal == null || isJournal) {
            totalItem
                .put("printISSN", row.getString("printissn"))
                .put("onlineISSN", row.getString("onlineissn"));
          }
          if (isJournal == null || !isJournal) {
            totalItem.put("ISBN", row.getString("isbn"));
          }
          accessCountsByPeriod = new JsonArray();
          for (int i = 0; i < pubYearIndexMap.size(); i++) {
            accessCountsByPeriod.add(0L);
          }
          totalItem
              .put("periodOfUse", usePeriodLabel)
              .put("accessType", accessType)
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", totalAccessCount)
              .put("accessCountsByPeriod", accessCountsByPeriod);
          items.add(totalItem);
          totalItems.put(itemKey, totalItem);
        }
        accessCountsByPeriod.set(idx, accessCountsByPeriod.getLong(idx) + totalAccessCount);

        JsonObject uniqueItem = uniqueItems.get(itemKey);
        if (uniqueItem != null) {
          accessCountsByPeriod = uniqueItem.getJsonArray("accessCountsByPeriod");
          uniqueItem.put("accessCountTotal", uniqueAccessCount
              + uniqueItem.getLong("accessCountTotal"));
        } else {
          uniqueItem = new JsonObject()
              .put("kbId", row.getUUID("kbid"))
              .put("title", row.getString("title"));
          if (isJournal == null || isJournal) {
            uniqueItem.put("printISSN", row.getString("printissn"))
                .put("onlineISSN", row.getString("onlineissn"));
          }
          if (isJournal == null || !isJournal) {
            uniqueItem.put("ISBN", row.getString("isbn"));
          }
          accessCountsByPeriod = new JsonArray();
          for (int i = 0; i < pubYearIndexMap.size(); i++) {
            accessCountsByPeriod.add(0L);
          }
          uniqueItem
              .put("periodOfUse", usePeriodLabel)
              .put("accessType", accessType)
              .put("metricType", "Unique_Item_Requests")
              .put("accessCountTotal", uniqueAccessCount)
              .put("accessCountsByPeriod", accessCountsByPeriod);
          uniqueItems.put(itemKey, uniqueItem);
          items.add(uniqueItem);
        }
        accessCountsByPeriod.set(idx, accessCountsByPeriod.getLong(idx) + uniqueAccessCount);
      }
    });
    Long totalItemRequestsTotal = 0L;
    Long uniqueItemRequestsTotal = 0L;
    for (int i = 0; i < totalItemRequestsByPeriod.size(); i++) {
      totalItemRequestsTotal += totalItemRequestsByPeriod.get(i);
      uniqueItemRequestsTotal += uniqueItemRequestsByPeriod.get(i);
    }
    JsonObject json = new JsonObject()
        .put("agreementId", agreementId)
        .put("accessCountPeriods", accessCountsPeriods)
        .put("totalItemRequestsTotal", totalItemRequestsTotal)
        .put("totalItemRequestsByPeriod", totalItemRequestsByPeriod)
        .put("totalRequestsPeriodsOfUseByPeriod", totalRequestsPeriodsOfUseByPeriod)
        .put("uniqueItemRequestsTotal", uniqueItemRequestsTotal)
        .put("uniqueItemRequestsByPeriod", uniqueItemRequestsByPeriod)
        .put("uniqueRequestsPeriodsOfUseByPeriod", uniqueRequestsPeriodsOfUseByPeriod)
        .put("items", items);
    log.info("JSON={}", json.encodePrettily());
    return json;
  }
}
