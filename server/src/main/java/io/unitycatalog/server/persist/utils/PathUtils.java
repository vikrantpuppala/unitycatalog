package io.unitycatalog.server.persist.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PathUtils {
  public static List<String> getParentPathsList(String url) {
    List<String> parentPaths = new ArrayList<>();

    if (url == null || !url.contains("://")) {
      throw new IllegalArgumentException("Invalid URL: " + url);
    }

    // Extract scheme (e.g., "s3://", "file://", etc.)
    int schemeEndIdx = url.indexOf("://") + 3;
    String scheme = url.substring(0, schemeEndIdx);

    // Extract path part and remove trailing slash if present
    String pathPart = url.substring(schemeEndIdx).replaceAll("/$", "");

    if (pathPart.isEmpty()) {
      return parentPaths; // No parent paths if only scheme is present
    }

    // Split path into components
    String[] parts = pathPart.split("/");

    // Construct parent paths iteratively
    StringBuilder parentPath =
        new StringBuilder(scheme + parts[0]); // Preserve scheme and bucket/container
    for (int i = 1; i < parts.length - 1; i++) {
      parentPath.append("/").append(parts[i]);
      parentPaths.add(parentPath + "/"); // Ensure trailing slash for directories
    }

    return parentPaths;
  }
}
