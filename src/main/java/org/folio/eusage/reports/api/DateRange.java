package org.folio.eusage.reports.api;

import java.time.LocalDate;
import java.time.Period;

public class DateRange {
  private final LocalDate start; // inclusive
  private final LocalDate startMonth; // inclusive
  private final LocalDate end;   // inclusive
  private final LocalDate endMonth; // exclusive

  /**
   * Construct DateRange from string as returned from Postgres's daterange.
   * @see <a href="https://www.postgresql.org/docs/13/rangetypes.html#RANGETYPES-IO">Range input/output</a>
   * @param s [start,end) OR [start,end] formats only.
   */
  public DateRange(String s) {
    if (s == null) {
      throw new IllegalArgumentException();
    }
    int idx = s.indexOf(',');
    if (idx == -1) {
      throw new IllegalArgumentException("Missing , in " + s);
    }
    if (s.charAt(0) != '[') {
      throw new IllegalArgumentException("Does not begin with [ in " + s);
    }
    char last = s.charAt(s.length() - 1);
    if (last == ']') {
      end = LocalDate.parse(s.substring(idx + 1, idx + 11));
    } else if (last == ')') {
      end = LocalDate.parse(s.substring(idx + 1, idx + 11)).minusDays(1L);
    } else {
      throw new IllegalArgumentException("Does not end with ] or ) in " + s);
    }
    start = LocalDate.parse(s.substring(1, 11));
    startMonth = LocalDate.of(start.getYear(), start.getMonth(), 1);
    LocalDate tmp = end.plusMonths(1L);
    this.endMonth = LocalDate.of(tmp.getYear(), tmp.getMonth(), 1);
  }

  /**
   * Construct with LocalDate.
   * @param start inclusive start date
   * @param end exclusive end date
   */
  public DateRange(LocalDate start, LocalDate end) {
    this.start = start;
    this.end = end.minusDays(1L);

    startMonth = LocalDate.of(start.getYear(), start.getMonth(), 1);
    LocalDate tmp = this.end.plusMonths(1);
    this.endMonth = LocalDate.of(tmp.getYear(), tmp.getMonth(), 1);
  }

  String getStart() {
    return start.toString();
  }

  String getEnd() {
    return end.toString();
  }

  int getMonths() {
    Period age = Period.between(startMonth, endMonth);
    return age.getYears() * 12 + age.getMonths();
  }

  boolean includes(LocalDate d) {
    return !start.isAfter(d) && end.isAfter(d);
  }

  static long commonMonths(DateRange a, DateRange b) {
    LocalDate commonStart = a.startMonth.isAfter(b.startMonth) ? a.startMonth : b.startMonth;
    LocalDate commonEnd = a.endMonth.isAfter(b.endMonth) ? b.endMonth : a.endMonth;
    long d = Period.between(commonStart, commonEnd).toTotalMonths();
    return d < 0L ? 0L : d;
  }

  long commonMonths(DateRange a) {
    return DateRange.commonMonths(this, a);
  }
}
