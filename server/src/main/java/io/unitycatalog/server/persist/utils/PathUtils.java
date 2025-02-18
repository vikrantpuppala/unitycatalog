package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
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
    parentPaths.add(parentPath + "/"); // Ensure trailing slash for directories
    for (int i = 1; i < parts.length - 1; i++) {
      parentPath.append("/").append(parts[i]);
      parentPaths.add(parentPath + "/"); // Ensure trailing slash for directories
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
