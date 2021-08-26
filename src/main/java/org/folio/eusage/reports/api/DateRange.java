package org.folio.eusage.reports.api;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DateRange {

  private final LocalDate start;
  private final LocalDate end;

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

  String getStart() {
    return start.format(DateTimeFormatter.ISO_LOCAL_DATE);
  }

  String getEnd() {
    return end.format(DateTimeFormatter.ISO_LOCAL_DATE);
  }
}
