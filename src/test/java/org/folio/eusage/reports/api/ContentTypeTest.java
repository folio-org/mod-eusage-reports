package org.folio.eusage.reports.api;

import io.vertx.core.MultiMap;
import org.junit.Assert;
import org.junit.Test;

public class ContentTypeTest {

  @Test
  public void testGood() {
    MultiMap h = MultiMap.caseInsensitiveMultiMap();
    ContentType m = new ContentType(h);
    Assert.assertFalse(m.isCsv());
    Assert.assertTrue(m.isFull());
    Assert.assertEquals("application/json", m.getContentType());

    h.set("csv", "false");
    m = new ContentType(h);
    Assert.assertFalse(m.isCsv());
    Assert.assertTrue(m.isFull());
    Assert.assertEquals("application/json", m.getContentType());

    h.set("csv", "true");
    m = new ContentType(h);
    Assert.assertTrue(m.isCsv());
    Assert.assertTrue(m.isFull());
    Assert.assertEquals("text/csv", m.getContentType());

    h.set("full", "true");
    m = new ContentType(h);
    Assert.assertTrue(m.isCsv());
    Assert.assertTrue(m.isFull());
    Assert.assertEquals("text/csv", m.getContentType());

    h.set("full", "false");
    m = new ContentType(h);
    Assert.assertTrue(m.isCsv());
    Assert.assertFalse(m.isFull());
    Assert.assertEquals("text/csv", m.getContentType());
  }
}
