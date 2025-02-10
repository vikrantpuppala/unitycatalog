package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Session;

public class PathUtils {
  private static final EntityConflictChecker<ExternalLocationDAO> externalLocationConflictChecker =
      new EntityConflictChecker<>(ExternalLocationDAO.class);
  private static final EntityConflictChecker<TableInfoDAO> externalTableConflictChecker =
      new EntityConflictChecker<>(TableInfoDAO.class);
  private static final EntityConflictChecker<VolumeInfoDAO> externalVolumeConflictChecker =
      new EntityConflictChecker<>(VolumeInfoDAO.class);
  private static final EntityConflictChecker<RegisteredModelInfoDAO> modelConflictChecker =
      new EntityConflictChecker<>(RegisteredModelInfoDAO.class);

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

  public static void checkExternalLocationForConflicts(Session session, String url) {
    // cannot be above, under or same as an external location
    externalLocationConflictChecker.cannotOverlapWithEntity(session, url, true, true, true);
    // cannot be under external tables
    externalTableConflictChecker.cannotOverlapWithEntity(session, url, false, true, false);
    // cannot be under ML models
    modelConflictChecker.cannotOverlapWithEntity(session, url, false, true, false);
    // cannot be under external volumes
    externalVolumeConflictChecker.cannotOverlapWithEntity(session, url, false, true, false);
  }

  public static void checkExternalStorageLocationForConflicts(Session session, String url) {
    // cannot be above external locations
    externalLocationConflictChecker.cannotOverlapWithEntity(session, url, true, false, false);
    // cannot be above, under or same as an external table
    externalTableConflictChecker.cannotOverlapWithEntity(session, url, true, true, true);
    // cannot be above, under or same as an ML model
    modelConflictChecker.cannotOverlapWithEntity(session, url, true, true, true);
    // cannot be above, under or same as an external volume
    externalVolumeConflictChecker.cannotOverlapWithEntity(session, url, true, true, true);
  }
}
