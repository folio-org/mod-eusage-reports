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

public class UseOverTime {
  private static final Logger log = LogManager.getLogger(UseOverTime.class);

  static JsonObject createTotalItem(Row row, String accessType, Long totalAccessCount,
      JsonArray accessCountsByPeriods, int periodSize) {
    return createItem(row, accessType, "Total_Item_Requests",
        totalAccessCount, accessCountsByPeriods, periodSize);
  }

  static JsonObject createUniqueItem(Row row, String accessType, Long totalAccessCount,
      JsonArray accessCountsByPeriods, int periodSize) {
    return createItem(row, accessType, "Unique_Item_Requests",
        totalAccessCount, accessCountsByPeriods, periodSize);
  }

  static JsonObject createNonMatchedItem(Row row, int periodSize) {
    return createItem(row, null, null,
        0L, new JsonArray(), periodSize);
  }

  private static JsonObject createItem(Row row, String accessType, String metricType,
      Long totalAccessCount, JsonArray accessCountsByPeriods, int periodSize) {

    for (int i = 0; i < periodSize; i++) {
      accessCountsByPeriods.add(0L);
    }
    JsonObject o = new JsonObject()
        .put("kbId", row.getUUID("kbid"))
        .put("title", row.getString("title"));
    String v = row.getString("printissn");
    if (v != null) {
      o.put("printISSN", v);
    }
    v = row.getString("onlineissn");
    if (v != null) {
      o.put("onlineISSN", v);
    }
    v = row.getString("isbn");
    if (v != null) {
      o.put("ISBN", v);
    }
    o.put("accessType", accessType)
        .put("metricType", metricType)
        .put("accessCountTotal", totalAccessCount)
        .put("accessCountsByPeriod", accessCountsByPeriods);
    return o;
  }

  static JsonObject titlesToJsonObject(RowSet<Row> rowSet, String agreementId, Periods usePeriods) {

    List<Long> totalItemRequestsByPeriod = new ArrayList<>();
    List<Long> uniqueItemRequestsByPeriod = new ArrayList<>();
    Map<String,JsonObject> totalItems = new HashMap<>();
    Map<String,JsonObject> uniqueItems = new HashMap<>();
    Set<UUID> kbIds = new TreeSet<>();

    JsonArray items = new JsonArray();
    for (int i = 0; i < usePeriods.size(); i++) {
      totalItemRequestsByPeriod.add(0L);
      uniqueItemRequestsByPeriod.add(0L);
    }
    rowSet.forEach(row -> {
      log.info("useOverTime row: {}", () -> row.deepToString());
      UUID kbId = row.getUUID("kbid");
      String usageDateRange = row.getString("usagedaterange");

      if (usageDateRange == null) {
        if (kbIds.add(kbId)) {
          JsonObject item = createNonMatchedItem(row, usePeriods.size());
          items.add(item);
        }
        return;
      }
      kbIds.add(kbId);
      String accessType = row.getBoolean("openaccess") ? "OA_Gold" : "Controlled";
      String itemKey = kbId + "," + accessType;
      LocalDate usageStart = usePeriods.floorMonths(LocalDate.parse(
          usageDateRange.substring(1, 11)));
      int idx = usePeriods.getPeriodEntry(usageStart);

      Long totalAccessCount = row.getLong("totalaccesscount");
      Long uniqueAccessCount = row.getLong("uniqueaccesscount");

      totalItemRequestsByPeriod.set(idx, totalAccessCount
          + totalItemRequestsByPeriod.get(idx));

      uniqueItemRequestsByPeriod.set(idx, uniqueAccessCount
          + uniqueItemRequestsByPeriod.get(idx));

      JsonObject totalItem = totalItems.get(itemKey);
      JsonArray accessCountsByPeriods;
      if (totalItem != null) {
        accessCountsByPeriods = totalItem.getJsonArray("accessCountsByPeriod");
        totalItem.put("accessCountTotal", totalItem.getLong("accessCountTotal")
            + totalAccessCount);
      } else {
        accessCountsByPeriods = new JsonArray();
        totalItem = createTotalItem(row, accessType, totalAccessCount,
            accessCountsByPeriods, usePeriods.size());
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
        uniqueItem = createUniqueItem(row, accessType, uniqueAccessCount,
            accessCountsByPeriods, usePeriods.size());
        uniqueItems.put(itemKey, uniqueItem);
        items.add(uniqueItem);
      }
      accessCountsByPeriods.set(idx, accessCountsByPeriods.getLong(idx) + uniqueAccessCount);
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
        .put("items", items);
    log.info("useOverTime: JSON {}", () -> json.encodePrettily());
    return json;
  }


}
