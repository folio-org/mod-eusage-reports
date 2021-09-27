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

    h.set("contentType", "application/json");
    m = new ContentType(h);
    Assert.assertFalse(m.isCsv());
    Assert.assertTrue(m.isFull());

    h.set("contentType", "application/full+json");
    m = new ContentType(h);
    Assert.assertFalse(m.isCsv());
    Assert.assertTrue(m.isFull());

    h.set("contentType", "application/summary+json");
    m = new ContentType(h);
    Assert.assertFalse(m.isCsv());
    Assert.assertFalse(m.isFull());

    h.set("contentType", "text/csv");
    m = new ContentType(h);
    Assert.assertTrue(m.isCsv());
    Assert.assertTrue(m.isFull());

    h.set("contentType", "text/summary+csv");
    m = new ContentType(h);
    Assert.assertTrue(m.isCsv());
    Assert.assertFalse(m.isFull());

    h.set("contentType", "text/full+csv");
    m = new ContentType(h);
    Assert.assertTrue(m.isCsv());
    Assert.assertTrue(m.isFull());

    h.set("contentType", "text/xx");
    Throwable t = Assert.assertThrows(IllegalArgumentException.class, () -> new ContentType(h));
    Assert.assertEquals("Bad value for contentType: text/xx", t.getMessage());
  }
}
