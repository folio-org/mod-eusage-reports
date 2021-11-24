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

public final class ReqsByDateOfUse {
  private static final Logger log = LogManager.getLogger(ReqsByDateOfUse.class);

  private ReqsByDateOfUse() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  static JsonObject titlesToJsonObject(RowSet<Row> rowSet, String agreementId,
      Periods usePeriods, int pubPeriodsInMonths) {

    List<Long> totalItemRequestsByPeriod = new ArrayList<>();
    List<Long> uniqueItemRequestsByPeriod = new ArrayList<>();
    Map<String,JsonObject> totalItems = new HashMap<>();
    Map<String,JsonObject> uniqueItems = new HashMap<>();
    Set<UUID> kbIds = new TreeSet<>();

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
      UUID kbId = row.getUUID("kbid");
      String usageDateRange = row.getString("usagedaterange");
      if (usageDateRange == null) {
        if (kbIds.add(kbId)) {
          JsonObject item = UseOverTime.createNonMatchedItem(row,usePeriods.size());
          items.add(item);
        }
        return;
      }
      kbIds.add(kbId);
      Long totalAccessCount = row.getLong("totalaccesscount");
      Long uniqueAccessCount = row.getLong("uniqueaccesscount");
      if (usageDateRange != null && totalAccessCount > 0L) {
        LocalDate usageStart = usePeriods.floorMonths(LocalDate.parse(
            usageDateRange.substring(1, 11)));
        int idx = usePeriods.getPeriodEntry(usageStart);

        LocalDate publicationDate = row.getLocalDate("publicationdate");
        String pubPeriodLabel = Periods.periodLabelFloor(publicationDate, pubPeriodsInMonths,
            "nopub");
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

        String accessType = row.getBoolean("openaccess") ? "OA_Gold" : "Controlled";
        String itemKey = kbId + "," + pubPeriodLabel + "," + accessType;
        JsonObject totalItem = totalItems.get(itemKey);
        JsonArray accessCountsByPeriods;
        if (totalItem != null) {
          accessCountsByPeriods = totalItem.getJsonArray("accessCountsByPeriod");
          totalItem.put("accessCountTotal", totalItem.getLong("accessCountTotal")
              + totalAccessCount);
        } else {
          accessCountsByPeriods = new JsonArray();
          totalItem = UseOverTime.createTotalItem(row, accessType,
              totalAccessCount, accessCountsByPeriods, usePeriods.size());
          totalItem.put("publicationYear", pubPeriodLabel);
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
          accessCountsByPeriods = new JsonArray();
          uniqueItem = UseOverTime.createUniqueItem(row, accessType,
              uniqueAccessCount, accessCountsByPeriods, usePeriods.size());
          uniqueItem.put("publicationYear", pubPeriodLabel);
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
