package org.folio.tlib.util;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import org.folio.okapi.testing.UtilityClassTester;
import org.junit.Test;

public class ResourceUtilTest {
  @Test
  public void utilityClass() {
    UtilityClassTester.assertUtilityClass(ResourceUtil.class);
  }

  @Test
  public void notFound() {
    Throwable t = assertThrows(UncheckedIOException.class, () -> ResourceUtil.load("/foo"));
    assertThat(t.getCause(), is(instanceOf(FileNotFoundException.class)));
  }
}
