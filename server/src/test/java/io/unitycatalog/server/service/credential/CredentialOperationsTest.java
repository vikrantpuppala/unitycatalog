package io.unitycatalog.server.service.credential;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sts.model.StsException;

@ExtendWith(MockitoExtension.class)
public class CredentialOperationsTest {
  @Mock ServerProperties serverProperties;
  @Mock ExternalLocationRepository externalLocationRepository;
  CredentialOperations credentialsOperations;

  @Test
  public void testGenerateS3TemporaryCredentials() {
    final String ACCESS_KEY = "accessKey";
    final String SECRET_KEY = "secretKey";
    final String S3_REGION = "us-west-2";
    final String ROLE_ARN = "roleArn";
    try (MockedStatic<ServerProperties> mockedStatic = mockStatic(ServerProperties.class);
        MockedStatic<ExternalLocationRepository> mockedStatic2 =
            mockStatic(ExternalLocationRepository.class)) {
      mockedStatic.when(ServerProperties::getInstance).thenReturn(serverProperties);
      mockedStatic2
          .when(ExternalLocationRepository::getInstance)
          .thenReturn(externalLocationRepository);
      // Test when sts client is called
      when(serverProperties.getMetastoreS3Config())
          .thenReturn(
              Optional.of(
                  S3StorageConfig.builder()
                      .accessKey(ACCESS_KEY)
                      .secretKey(SECRET_KEY)
                      .region(S3_REGION)
                      .awsRoleArn(ROLE_ARN)
                      .build()));
      when(externalLocationRepository.getStorageCredentialsForPath(any()))
          .thenReturn(
              new StorageCredentialInfo().awsIamRole(new AwsIamRoleResponse().roleArn(ROLE_ARN)));
      credentialsOperations = new CredentialOperations(serverProperties);
      assertThatThrownBy(
              () ->
                  credentialsOperations.vendCredential(
                      "s3://storageBase/abc",
                      Set.of(CredentialContext.Privilege.SELECT),
                      Optional.empty()))
          .isInstanceOf(StsException.class);
    }
  }

  @Test
  public void testGenerateAzureTemporaryCredentials() {
    final String CLIENT_ID = "clientId";
    final String CLIENT_SECRET = "clientSecret";
    final String TENANT_ID = "tenantId";
    try (MockedStatic<ExternalLocationRepository> mockedStatic =
        mockStatic(ExternalLocationRepository.class)) {
      mockedStatic
          .when(ExternalLocationRepository::getInstance)
          .thenReturn(externalLocationRepository);
      credentialsOperations = new CredentialOperations(ServerProperties.getInstance());
      //      // Use datalake service client
      //      when(externalLocationRepository.getStorageCredentialsForPath(any()))
      //          .thenReturn(
      //              );
      assertThatThrownBy(
              () ->
                  credentialsOperations.vendCredential(
                      "abfss://test@uctest",
                      Set.of(CredentialContext.Privilege.UPDATE),
                      Optional.of(
                          new StorageCredentialInfo()
                              .azureServicePrincipal(
                                  new AzureServicePrincipal()
                                      .applicationId(CLIENT_ID)
                                      .clientSecret(CLIENT_SECRET)
                                      .directoryId(TENANT_ID)))))
          .isInstanceOf(CompletionException.class);
    }
  }

  // TODO: Fix this test
  /*
  @Test
  public void testGenerateGcpTemporaryCredentials() {
    try (MockedStatic<ServerProperties> mockedStatic = mockStatic(ServerProperties.class)) {
      String gcsBucketPath = "gs://uctest";
      mockedStatic.when(ServerProperties::getInstance).thenReturn(serverProperties);
      // Test mode used
      GCSStorageConfig gcsStorageConfig =
          GCSStorageConfig.builder()
              .bucketPath(gcsBucketPath)
              .serviceAccountKeyJsonFilePath("")
              .build();
      when(serverProperties.getGcsConfigurations())
          .thenReturn(Map.of(gcsBucketPath, gcsStorageConfig));
      credentialsOperations = new CredentialOperations();
      TemporaryCredentials gcpTemporaryCredentials =
          credentialsOperations.vendCredential(
              "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE));
      assertThat(gcpTemporaryCredentials.getGcpOauthToken().getOauthToken()).isNotNull();

      // Use default creds
      GCSStorageConfig gcsStorageConfig2 =
          GCSStorageConfig.builder()
              .bucketPath(gcsBucketPath)
              .serviceAccountKeyJsonFilePath("")
              .build();
      when(serverProperties.getGcsConfigurations())
          .thenReturn(Map.of(gcsBucketPath, gcsStorageConfig2));
      credentialsOperations = new CredentialOperations();
      assertThatThrownBy(
              () ->
                  credentialsOperations.vendCredential(
                      "gs://uctest/abc/xyz", Set.of(CredentialContext.Privilege.UPDATE)))
          .isInstanceOf(BaseException.class);
    }
  }
  */
}
