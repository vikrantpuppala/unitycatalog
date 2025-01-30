package io.unitycatalog.server.service.credential.aws;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.time.Duration;
import java.util.Map;
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
  private StsClient ucMasterRoleStsClient = null;
  private Repositories repositories;
  private final Map<String, S3StorageConfig> s3Configurations;

  public AwsCredentialVendor(ServerProperties serverProperties, Repositories repositories) {
    this.s3Configurations = serverProperties.getS3Configurations();
    S3StorageConfig ucMasterRoleConfig = serverProperties.getUcMasterRoleS3Configuration();
    if (ucMasterRoleConfig != null) {
      this.ucMasterRoleStsClient = getStsClientForStorageConfig(ucMasterRoleConfig);
    }
    this.repositories = repositories;
  }

  public Credentials vendAwsCredentialsUsingStorageCredentials(CredentialContext context) {
    Optional<StorageCredentialInfo> storageCredentialInfo =
        this.repositories.getStorageCredentialRepository().getStorageCredentialsForPath(context);
    if (storageCredentialInfo.isEmpty()) {
      return null;
    }
    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());
    return ucMasterRoleStsClient
        .assumeRole(
            r ->
                r.roleArn(storageCredentialInfo.get().getAwsIamRole().getRoleArn())
                    .policy(awsPolicy)
                    .roleSessionName(roleSessionName)
                    .durationSeconds((int) Duration.ofHours(1).toSeconds()))
        .credentials();
  }

  public Credentials vendAwsCredentialsUsingServerProperties(CredentialContext context) {
    S3StorageConfig s3StorageConfig = s3Configurations.get(context.getStorageBase());
    if (s3StorageConfig == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "S3 bucket configuration not found.");
    }

    if (s3StorageConfig.getSessionToken() != null && !s3StorageConfig.getSessionToken().isEmpty()) {
      // if a session token was supplied, then we will just return static session credentials
      return Credentials.builder()
          .accessKeyId(s3StorageConfig.getAccessKey())
          .secretAccessKey(s3StorageConfig.getSecretKey())
          .sessionToken(s3StorageConfig.getSessionToken())
          .build();
    }

    // TODO: cache sts client
    StsClient stsClient = getStsClientForStorageConfig(s3StorageConfig);

    // TODO: Update this with relevant user/role type info once available
    String roleSessionName = "uc-%s".formatted(UUID.randomUUID());
    String awsPolicy =
        AwsPolicyGenerator.generatePolicy(context.getPrivileges(), context.getLocations());
    return stsClient
        .assumeRole(
            r ->
                r.roleArn(s3StorageConfig.getAwsRoleArn())
                    .policy(awsPolicy)
                    .roleSessionName(roleSessionName)
                    .durationSeconds((int) Duration.ofHours(1).toSeconds()))
        .credentials();
  }

  public Credentials vendAwsCredentials(CredentialContext context) {
    Credentials credentials = null;
    if (this.ucMasterRoleStsClient != null && this.repositories != null) {
      credentials = vendAwsCredentialsUsingStorageCredentials(context);
    }
    if (credentials == null) {
      credentials = vendAwsCredentialsUsingServerProperties(context);
    }
    return credentials;
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
