package org.folio.eusage.reports.api;

import java.time.LocalDate;
import java.time.Period;

public class DateRange {
  private final LocalDate start; // inclusive
  private final LocalDate end;   // inclusive

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
      end = LocalDate.parse(s.substring(idx + 1, s.length() - 1));
    } else if (last == ')') {
      end = LocalDate.parse(s.substring(idx + 1, s.length() - 1)).minusDays(1);
    } else {
      throw new IllegalArgumentException("Does not end with ] or ) in " + s);
    }
    start = LocalDate.parse(s.substring(1, idx));
  }

  /**
   * Construct with LocalDate.
   * @param start inclusive start date
   * @param end exclusive end date
   */
  public DateRange(LocalDate start, LocalDate end) {
    this.start = start;
    this.end = end.minusDays(1);
  }

  String getStart() {
    return start.toString();
  }

  String getEnd() {
    return end.toString();
  }

  int getMonths() {
    Period age = Period.between(start, end.plusDays(1));
    return age.getYears() * 12 + age.getMonths();
  }

  boolean includes(LocalDate d) {
    return !start.isAfter(d) && end.isAfter(d);
  }

  static long commonMonths(DateRange a, DateRange b) {
    LocalDate commonStart = a.start.isAfter(b.start) ? a.start : b.start;
    LocalDate commonEnd = a.end.isAfter(b.end) ? b.end : a.end;
    long d = Period.between(commonStart, commonEnd.plusDays(1)).toTotalMonths();
    return d < 0L ? 0L : d;
  }

  long commonMonths(DateRange a) {
    return DateRange.commonMonths(this, a);
  }
}
