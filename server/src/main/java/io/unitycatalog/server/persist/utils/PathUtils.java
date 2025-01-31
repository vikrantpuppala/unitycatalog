package io.unitycatalog.server.persist.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PathUtils {
  public static List<String> getParentPathsList(String url) {
    List<String> parentPaths = new ArrayList<>();
    if (!url.contains("://")) {
      throw new IllegalArgumentException(
          "Invalid URL format. Must contain a scheme (e.g., s3://, file://)");
    }

    // Split scheme and path
    int schemeEndIndex = url.indexOf("://") + 3; // Include ://
    String scheme = url.substring(0, schemeEndIndex);
    String pathPart = url.substring(schemeEndIndex);

    Path path = Paths.get(pathPart);
    parentPaths.add(
        scheme + path.toString().replaceAll("/$", "")); // Add full path, strip trailing slash

    while (path != null) {
      path = path.getParent();
      if (path != null) {
        try {
          String normalizedPath = scheme + path.toString().replaceAll("/$", ""); // Prepend scheme
          parentPaths.add(normalizedPath);
        } catch (Exception e) {
          // Ignore invalid paths
        }
      }
    }

    return parentPaths;
  }
}
