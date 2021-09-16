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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReqsByDateOfUse {
  private static final Logger log = LogManager.getLogger(ReqsByDateOfUse.class);

  static JsonObject titlesToJsonObject(RowSet<Row> rowSet, Boolean isJournal, String agreementId,
      Periods usePeriods, int pubPeriodsInMonths) {

    List<Long> totalItemRequestsByPeriod = new ArrayList<>();
    List<Long> uniqueItemRequestsByPeriod = new ArrayList<>();
    Map<String,JsonObject> totalItems = new HashMap<>();
    Map<String,JsonObject> uniqueItems = new HashMap<>();
    Set<String> dup = new TreeSet<>();

    JsonArray totalRequestsPublicationYearsByPeriod = new JsonArray();
    JsonArray uniqueRequestsPublicationYearsByPeriod = new JsonArray();
    JsonArray items = new JsonArray();
    for (int i = 0; i < usePeriods.size(); i++) {
      totalItemRequestsByPeriod.add(0L);
      uniqueItemRequestsByPeriod.add(0L);
      totalRequestsPublicationYearsByPeriod.add(new JsonObject());
      uniqueRequestsPublicationYearsByPeriod.add(new JsonObject());
    }
    rowSet.forEach(row -> {
      String usageDateRange = row.getString("usagedaterange");
      Long totalAccessCount = row.getLong("totalaccesscount");
      Long uniqueAccessCount = row.getLong("uniqueaccesscount");
      if (usageDateRange != null && totalAccessCount > 0L) {
        LocalDate usageStart = usePeriods.floorMonths(LocalDate.parse(
            usageDateRange.substring(1, 11)));
        int idx = usePeriods.getPeriodEntry(usageStart);

        LocalDate publicationDate = row.getLocalDate("publicationdate");
        String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, pubPeriodsInMonths,
            "nopub");
        String accessType = row.getBoolean("openaccess") ? "OA_Gold" : "Controlled";
        String itemKey = row.getUUID("kbid").toString() + "," + pubPeriodLabel + "," + accessType;
        String dupKey = itemKey + "," + usageDateRange + "," + publicationDate;
        if (!dup.add(dupKey)) {
          return;
        }
        totalItemRequestsByPeriod.set(idx, totalAccessCount
            + totalItemRequestsByPeriod.get(idx));

        uniqueItemRequestsByPeriod.set(idx, uniqueAccessCount
            + uniqueItemRequestsByPeriod.get(idx));

        JsonObject o = totalRequestsPublicationYearsByPeriod.getJsonObject(idx);
        Long totalAccessCountPeriod = o.getLong(pubPeriodLabel, 0L);
        o.put(pubPeriodLabel, totalAccessCountPeriod + totalAccessCount);

        o = uniqueRequestsPublicationYearsByPeriod.getJsonObject(idx);
        Long uniqueAccessCountPeriod = o.getLong(pubPeriodLabel, 0L);
        o.put(pubPeriodLabel, uniqueAccessCountPeriod + uniqueAccessCount);

        JsonObject totalItem = totalItems.get(itemKey);
        JsonArray accessCountsByPeriods;
        if (totalItem != null) {
          accessCountsByPeriods = totalItem.getJsonArray("accessCountsByPeriod");
          totalItem.put("accessCountTotal", totalItem.getLong("accessCountTotal")
              + totalAccessCount);
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
          accessCountsByPeriods = new JsonArray();
          for (int i = 0; i < usePeriods.size(); i++) {
            accessCountsByPeriods.add(0L);
          }
          totalItem
              .put("publicationYear", pubPeriodLabel)
              .put("accessType", accessType)
              .put("metricType", "Total_Item_Requests")
              .put("accessCountTotal", totalAccessCount)
              .put("accessCountsByPeriod", accessCountsByPeriods);
          items.add(totalItem);
          totalItems.put(itemKey, totalItem);
        }
        accessCountsByPeriods.set(idx, accessCountsByPeriods.getLong(idx) + totalAccessCount);

        JsonObject uniqueItem = uniqueItems.get(itemKey);
        if (uniqueItem != null) {
          accessCountsByPeriods = uniqueItem.getJsonArray("accessCountsByPeriod");
          uniqueItem.put("accessCountTotal", uniqueItem.getLong("accessCountTotal")
              + uniqueAccessCount);
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
          accessCountsByPeriods = new JsonArray();
          for (int i = 0; i < usePeriods.size(); i++) {
            accessCountsByPeriods.add(0L);
          }
          uniqueItem
              .put("publicationYear", pubPeriodLabel)
              .put("accessType", accessType)
              .put("metricType", "Unique_Item_Requests")
              .put("accessCountTotal", uniqueAccessCount)
              .put("accessCountsByPeriod", accessCountsByPeriods);
          uniqueItems.put(itemKey, uniqueItem);
          items.add(uniqueItem);
        }
        accessCountsByPeriods.set(idx, accessCountsByPeriods.getLong(idx) + uniqueAccessCount);
      }
    });
    Long totalItemRequestsTotal = 0L;
    Long uniqueItemRequestsTotal = 0L;
    for (int i = 0; i < usePeriods.size(); i++) {
      totalItemRequestsTotal += totalItemRequestsByPeriod.get(i);
      uniqueItemRequestsTotal += uniqueItemRequestsByPeriod.get(i);
    }
    JsonObject json = new JsonObject()
        .put("agreementId", agreementId)
        .put("accessCountPeriods", usePeriods.getAccessCountPeriods())
        .put("totalItemRequestsTotal", totalItemRequestsTotal)
        .put("uniqueItemRequestsTotal", uniqueItemRequestsTotal)
        .put("totalItemRequestsByPeriod", new JsonArray(totalItemRequestsByPeriod))
        .put("uniqueItemRequestsByPeriod", new JsonArray(uniqueItemRequestsByPeriod))
        .put("totalRequestsPublicationYearsByPeriod", totalRequestsPublicationYearsByPeriod)
        .put("uniqueRequestsPublicationYearsByPeriod", uniqueRequestsPublicationYearsByPeriod)
        .put("items", items);
    log.debug("JSON={}", () -> json.encodePrettily());
    return json;
  }

}
