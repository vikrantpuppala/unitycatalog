package io.unitycatalog.server.service.credential.gcp;

import static java.lang.String.format;

import com.google.auth.oauth2.*;
import com.google.common.base.CharMatcher;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.iceberg.Files;

public class GcpCredentialVendor {
  public static final List<String> INITIAL_SCOPES =
      List.of("https://www.googleapis.com/auth/cloud-platform");

  public GcpCredentialVendor() {}

  @SneakyThrows
  public AccessToken vendGcpToken(
      CredentialContext credentialContext,
      Optional<StorageCredentialInfo> optionalStorageCredential) {
    //    String serviceAccountKeyJsonFilePath =
    // metastoreGcsConfig.getServiceAccountKeyJsonFilePath();
    //
    //    if (serviceAccountKeyJsonFilePath != null
    //        && serviceAccountKeyJsonFilePath.startsWith("testing://")) {
    //      // allow pass-through of a dummy value for integration testing
    //      return AccessToken.newBuilder()
    //          .setTokenValue(serviceAccountKeyJsonFilePath)
    //          .setExpirationTime(Date.from(Instant.ofEpochMilli(253370790000000L)))
    //          .build();
    //    }

    // FIXME!! This is not correct. We need to downscope the token based on the context not the
    // metastore credentials,
    //  however, it is still unclear how to model google credentials
    //    return downscopeGcpCreds(
    //            metastoreGoogleCredentials.createScoped(INITIAL_SCOPES), credentialContext)
    //        .refreshAccessToken();
    return new AccessToken("token", Date.from(Instant.ofEpochMilli(253370790000000L)));
  }

  @SneakyThrows
  private static GoogleCredentials getGoogleCredentials(GCSStorageConfig metastoreGcsConfig) {
    String serviceAccountKeyJsonFilePath = metastoreGcsConfig.getServiceAccountKeyJsonFilePath();

    if (serviceAccountKeyJsonFilePath != null && !serviceAccountKeyJsonFilePath.isEmpty()) {
      return ServiceAccountCredentials.fromStream(
          Files.localInput(serviceAccountKeyJsonFilePath).newStream());
    } else {
      try {
        return GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        throw new BaseException(ErrorCode.FAILED_PRECONDITION, "GCS credentials not found.", e);
      }
    }
  }

  private static OAuth2Credentials downscopeGcpCreds(
      GoogleCredentials credentials, CredentialContext context) {
    CredentialAccessBoundary.Builder boundaryBuilder = CredentialAccessBoundary.newBuilder();
    List<String> roles = resolvePrivilegesToRoles(context.getPrivileges());

    context
        .getLocations()
        .forEach(
            location -> {
              URI locationUri = URI.create(location);
              String path = CharMatcher.is('/').trimLeadingFrom(locationUri.getPath());

              String resource =
                  format("//storage.googleapis.com/projects/_/buckets/%s", locationUri.getHost());

              // for reading/writing objects
              String resourceNameStartsWithExpr =
                  format(
                      "resource.name.startsWith('projects/_/buckets/%s/objects/%s')",
                      locationUri.getHost(), path);

              // for listing objects
              String objectListPrefixStartsWithExpr =
                  format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('%s')",
                      path);

              String combinedExpr =
                  resourceNameStartsWithExpr + " || " + objectListPrefixStartsWithExpr;

              boundaryBuilder.addRule(
                  CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
                      .setAvailablePermissions(roles)
                      .setAvailabilityCondition(
                          CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition
                              .newBuilder()
                              .setExpression(combinedExpr)
                              .build())
                      .setAvailableResource(resource)
                      .build());
            });

    return DownscopedCredentials.newBuilder()
        .setSourceCredential(credentials)
        .setCredentialAccessBoundary(boundaryBuilder.build())
        .build();
  }

  private static List<String> resolvePrivilegesToRoles(
      Set<CredentialContext.Privilege> privileges) {
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      return List.of("inRole:roles/storage.objectAdmin");
    } else if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      return List.of("inRole:roles/storage.objectViewer");
    }
    return List.of();
  }
}
