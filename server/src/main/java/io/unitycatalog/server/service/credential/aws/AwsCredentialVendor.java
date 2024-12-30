package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import java.time.Duration;
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
  private final StsClient metastoreStsClient;
  private static final ExternalLocationRepository EXTERNAL_LOCATION_REPOSITORY =
      ExternalLocationRepository.getInstance();

  public AwsCredentialVendor(S3StorageConfig metastoreS3StorageConfiguration) {
    this.metastoreS3StorageConfiguration = metastoreS3StorageConfiguration;
    this.metastoreStsClient = getStsClientForStorageConfig(metastoreS3StorageConfiguration);
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    if (metastoreS3StorageConfiguration == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }
    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());
    StorageCredentialInfo storageCredentialInfo =
        EXTERNAL_LOCATION_REPOSITORY.getStorageCredentialsForPath(context);
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
