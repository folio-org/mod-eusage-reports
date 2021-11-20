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

  @Test(expected = StringIndexOutOfBoundsException.class)
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

  @Test
  public void testIncludes() {
    DateRange d = new DateRange("[2020-01-09,2020-02-01)");
    Assert.assertFalse(d.includes(LocalDate.of(2019, 1, 8)));
    Assert.assertFalse(d.includes(LocalDate.of(2020, 1, 8)));
    Assert.assertTrue(d.includes(LocalDate.of(2020, 1, 9)));
    Assert.assertTrue(d.includes(LocalDate.of(2020, 1, 30)));
    Assert.assertFalse(d.includes(LocalDate.of(2020, 2, 1)));
    Assert.assertFalse(d.includes(LocalDate.of(2021, 2, 1)));
  }

  @Test
  public void testFullDate() {
    DateRange d = new DateRange("[2021-06-18T00:00:00.000+00:00,2022-06-18T13:55:04.957+00:00]");
    Assert.assertEquals(13, d.getMonths());
    Assert.assertEquals(0, new DateRange("[2020-01-01,2020-01-01)").getMonths());
    Assert.assertEquals(1, new DateRange("[2020-01-01,2020-01-01]").getMonths());
    Assert.assertEquals(1, new DateRange("[2020-01-01,2020-01-02]").getMonths());
    Assert.assertEquals(1, new DateRange("[2020-01-01,2020-01-30]").getMonths());
    Assert.assertEquals(1, new DateRange("[2020-01-01,2020-01-31]").getMonths());
    Assert.assertEquals(2, new DateRange("[2020-01-15,2020-02-13]").getMonths());
    Assert.assertEquals(2, new DateRange("[2020-01-15,2020-02-15]").getMonths());
  }

  @Test
  public void testCommonMonths() {
    DateRange d = new DateRange(LocalDate.of(2020,1,1), LocalDate.of(2021,7,1));
    Assert.assertEquals(18, d.getMonths());
    Assert.assertEquals(1L, DateRange.commonMonths(d, new DateRange("[2020-02-01,2020-03-01)")));
    Assert.assertEquals(2L, d.commonMonths(new DateRange("[2020-02-01,2020-03-01]")));
    Assert.assertEquals(2L, d.commonMonths(new DateRange("[2020-02-01,2020-03-30]")));
    Assert.assertEquals(2L, d.commonMonths(new DateRange("[2020-02-01,2020-03-31]")));
    Assert.assertEquals(6L, d.commonMonths(new DateRange("[2021-01-01,2022-01-01)")));
    Assert.assertEquals(12L, d.commonMonths(new DateRange("[2020-01-01,2021-01-01)")));
    Assert.assertEquals(0L, d.commonMonths(new DateRange("[2019-01-01,2020-01-01)")));
    Assert.assertEquals(0L, d.commonMonths(new DateRange("[2021-07-01,2021-08-01)")));
    Assert.assertEquals(0L, d.commonMonths(new DateRange("[2017-01-01,2018-01-01)")));
  }
}
