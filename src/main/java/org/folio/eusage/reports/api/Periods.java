package org.folio.eusage.reports.api;

import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.Tuple;
import java.time.LocalDate;
import java.time.Period;

public class Periods {
  private final int periodInMonths;
  private final JsonArray accessCountPeriods = new JsonArray();
  final Period period;
  final LocalDate startDate;
  /** End date (exclusive): the first day after the last period. */
  final LocalDate endDate;

  /**
   * Construct Periods with start, end and optional period-of-use.
   * @param start starting date (YYYY-MM-DD or YYYY)
   * @param end end date (YYYY-MM-DD or YYYY) - including the month or year
   * @param periodOfUse NY or NM for period in N months or N years; null or
   *                    "auto" for automatically selecting year (start is YYYY format) or month if
   *                    start is YYYY-MM-DD format.
   */
  public Periods(String start, String end, String periodOfUse) {
    if (start.length() != end.length()) {
      throw new IllegalArgumentException(
          "startDate and endDate must have same length: " + start + " " + end);
    }
    if (start.compareTo(end) > 0) {
      throw new IllegalArgumentException("startDate=" + start + " is after endDate=" + end);
    }
    boolean isYear = start.length() == 4;
    if (periodOfUse == null || "auto".equals(periodOfUse)) {
      periodOfUse = isYear ? "1Y" : "1M";
    }
    periodInMonths = getPeriodInMonths(periodOfUse);
    period = Period.ofMonths(periodInMonths);
    start += isYear ? "-01-01" : "-01";
    end   += isYear ? "-01-01" : "-01";
    startDate = floorMonths(LocalDate.parse(start), periodInMonths);
    endDate   = floorMonths(LocalDate.parse(end), periodInMonths).plus(period);

    if (Period.between(startDate, endDate).getYears() > 10) {
      throw new IllegalArgumentException(
          "Must no exceed 10 years: startDate=" + start + ", endDate= " + end);
    }

    LocalDate date = startDate;
    do {
      accessCountPeriods.add(periodLabel(date));
      date = date.plus(period);
    } while (date.isBefore(endDate));
  }

  /**
   * Convert month string or year string to number of months: 6M -> 6, 2Y -> 24.
   */
  public static int getPeriodInMonths(String period) {
    int months = Integer.parseUnsignedInt(period, 0, period.length() - 1, 10);
    if (period.endsWith("Y")) {
      months *= 12;
    }
    return months;
  }

  /**
   * Start of the period date belongs to, periods are n months long. Examples:
   * <br>Begin of quarter (3 months): floorMonths("2019-05-17", 3) = "2019-04-01".
   * <br>Begin of decade (10 years): floorMonths("2019-05-17", 120) = "2010-01-01".
   */
  static LocalDate floorMonths(LocalDate date, int months) {
    int m = ((12 * date.getYear() + date.getMonthValue() - 1) / months) * months;
    return LocalDate.of(m / 12, m % 12 + 1, 1);
  }

  /**
   * Return period label for period range date to date+months.
   * @param date from date
   * @param periodInMonths plus amount in month
   * @return label in formats such as YYYY, YYYY-MM, YYYY - YYYY, YYYY-MM - YYYY-MM
   */
  public static String periodLabel(LocalDate date, int periodInMonths) {
    String startStr = date.toString();
    if (periodInMonths == 1) {
      return startStr.substring(0, startStr.length() - 3);
    }
    if (periodInMonths == 12) {
      return startStr.substring(0, startStr.length() - 6);
    }
    if (periodInMonths % 12 == 0) {
      String endStr = date.plusMonths(periodInMonths - 1L).toString();
      return startStr.substring(0, startStr.length() - 6) + " - "
          + endStr.subSequence(0, endStr.length() - 6);
    }
    String endStr = date.plusMonths(periodInMonths - 1L).toString();
    return startStr.substring(0, startStr.length() - 3) + " - "
        + endStr.subSequence(0, endStr.length() - 3);
  }

  /**
   * Label like "2021" or "2020 - 2024" or "2021-05" or "2021-04 - 2021-06".
   */
  public String periodLabel(LocalDate date) {
    return periodLabel(date, periodInMonths);
  }

  int getMonths() {
    return periodInMonths;
  }

  public int size() {
    return accessCountPeriods.size();
  }

  /**
   * Add all dates to Tuple in period - except end date.
   * @param tuple where dates are added.
   */
  public void addStartDates(Tuple tuple) {
    LocalDate date = startDate;
    do {
      tuple.addLocalDate(date);
      date = date.plus(period);
    } while (date.isBefore(endDate));
  }

  /**
   * Add end date to Tuple in period.
   * @param tuple where end date is added.
   */
  public void addEnd(Tuple tuple) {
    tuple.addLocalDate(endDate);
  }

  JsonArray getAccessCountPeriods() {
    return accessCountPeriods;
  }
}
