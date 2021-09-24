package org.folio.eusage.reports.api;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import org.junit.Assert;
import org.junit.Test;

public class DateRangeTest {

  @Test
  public void test1() {
    DateRange dateRange = new DateRange("[2020-01-09,2020-01-31]");
    Assert.assertEquals("2020-01-09", dateRange.getStart());
    Assert.assertEquals("2020-01-31", dateRange.getEnd());
  }

  @Test
  public void test2() {
    DateRange dateRange = new DateRange("[2020-01-09,2020-02-01)");
    Assert.assertEquals("2020-01-09", dateRange.getStart());
    Assert.assertEquals("2020-01-31", dateRange.getEnd());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNull() {
    new DateRange(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmpty() {
    new DateRange("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testX1() {
    new DateRange("x");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testXC2() {
    new DateRange("x,");
  }


  @Test(expected = IllegalArgumentException.class)
  public void testXC3() {
    new DateRange("[x,");
  }

  @Test(expected = DateTimeParseException.class)
  public void testDate1() {
    new DateRange("[2020-01-01,)");
  }

  @Test(expected = DateTimeParseException.class)
  public void testDate2() {
    new DateRange("[,2020-01-01)");
  }

  @Test(expected = DateTimeParseException.class)
  public void testDate3() {
    new DateRange("[ 2020-01-09 , 2020-02-01 )");
  }

  public void testIncludes() {
    DateRange d = new DateRange("[ 2020-01-09 , 2020-02-01 )");
    Assert.assertFalse(d.includes(LocalDate.of(2019, 1, 8)));
    Assert.assertFalse(d.includes(LocalDate.of(2020, 1, 8)));
    Assert.assertTrue(d.includes(LocalDate.of(2020, 1, 9)));

    Assert.assertTrue(d.includes(LocalDate.of(2020, 1, 31)));
    Assert.assertFalse(d.includes(LocalDate.of(2020, 2, 1)));
    Assert.assertFalse(d.includes(LocalDate.of(2021, 2, 1)));
  }
}
