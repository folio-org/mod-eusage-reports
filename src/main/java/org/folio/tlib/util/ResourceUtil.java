package org.folio.tlib.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public final class ResourceUtil {
  private ResourceUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Return the resource at path as String.
   *
   * @param path resource location with leading slash
   */
  public static String load(String path) {
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      InputStream inputStream = classLoader.getResourceAsStream(path);
      if (inputStream == null && path.startsWith("/")) {  // needed for unit tests without jar
        inputStream = classLoader.getResourceAsStream(path.substring(1));
      }
      if (inputStream == null) {
        throw new FileNotFoundException(path);
      }
      return new String(inputStream.readAllBytes());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
