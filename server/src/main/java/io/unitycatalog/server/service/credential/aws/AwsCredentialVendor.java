package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsCredentialVendor {
  private final S3StorageConfig metastoreS3StorageConfiguration;
  private StsClient metastoreStsClient;
  private final ServerProperties serverProperties;
  private static final ExternalLocationRepository EXTERNAL_LOCATION_REPOSITORY =
      ExternalLocationRepository.getInstance();

  public AwsCredentialVendor(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
    this.metastoreS3StorageConfiguration = serverProperties.getMetastoreS3Config().orElseThrow();
  }

  public Credentials vendAwsCredentials(
      CredentialContext context, Optional<StorageCredentialInfo> optionalStorageCredential) {
    StorageCredentialInfo storageCredentialInfo =
        optionalStorageCredential.orElse(
            EXTERNAL_LOCATION_REPOSITORY.getStorageCredentialsForPath(context));
    if (storageCredentialInfo == null) {
      throw new IllegalArgumentException(
          "No storage credentials found for path: " + context.getUri());
    }
    // Only return the test credentials after verifying that storage credentials for the path exist
    // in the repository
    if (serverProperties.getEnvironment().equals(ServerProperties.Environment.TEST)) {
      return Credentials.builder()
          .accessKeyId("test-access-key-id")
          .secretAccessKey("test-secret-access-key")
          .sessionToken("test-session-token")
          .build();
    }
    if (this.metastoreStsClient == null) {
      this.metastoreStsClient = getStsClientForStorageConfig(metastoreS3StorageConfiguration);
    }
    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());
    return metastoreStsClient
        .assumeRole(
            r ->
                r.roleArn(storageCredentialInfo.getAwsIamRole().getRoleArn())
                    .policy(awsPolicy)
                    .roleSessionName(roleSessionName)
                    .durationSeconds((int) Duration.ofHours(1).toSeconds()))
        .credentials();
  }

  private static StsClient getStsClientForStorageConfig(S3StorageConfig s3StorageConfig) {
    AwsCredentialsProvider credentialsProvider;
    if (s3StorageConfig.getSecretKey() != null && !s3StorageConfig.getAccessKey().isEmpty()) {
      credentialsProvider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  s3StorageConfig.getAccessKey(), s3StorageConfig.getSecretKey()));
    } else {
      credentialsProvider = DefaultCredentialsProvider.create();
    }

    // TODO: should we try and set the region to something configurable or specific to the server
    // instead?
    return StsClient.builder()
        .credentialsProvider(credentialsProvider)
        .region(Region.of(s3StorageConfig.getRegion()))
        .build();
  }
}
