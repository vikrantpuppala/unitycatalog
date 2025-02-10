package io.unitycatalog.server.base.tempcredential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTemporaryPathCredentialTest extends BaseCRUDTest {
  protected TemporaryCredentialOperations credentialOperations;
  protected ExternalLocationOperations externalLocationOperations;
  protected StorageCredentialOperations storageCredentialOperations;

  protected abstract TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig);

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig serverConfig);

  protected abstract StorageCredentialOperations createStorageCredentialOperations(
      ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    credentialOperations = createTemporaryCredentialsOperations(serverConfig);
    externalLocationOperations = createExternalLocationOperations(serverConfig);
    storageCredentialOperations = createStorageCredentialOperations(serverConfig);
  }

  private void createExternalLocationAndStorageCredential(String url) throws ApiException {
    URI uri = URI.create(url);
    String storageCredentialName = "uctest-credential-" + uri.getScheme();
    String externalLocationName = "uctest-location-" + uri.getScheme();

    CreateStorageCredential createStorageCredential;
    if (url.startsWith("s3://")) {
      createStorageCredential =
          new CreateStorageCredential()
              .name(storageCredentialName)
              .awsIamRole(
                  new AwsIamRoleRequest()
                      .roleArn("arn:aws:iam::123456789012:role/unitycatalog-role"));
    } else if (url.startsWith("abfs://") || url.startsWith("abfss://")) {
      createStorageCredential =
          new CreateStorageCredential()
              .name(storageCredentialName)
              .azureServicePrincipal(
                  new AzureServicePrincipal()
                      .directoryId("test-tenant-id")
                      .applicationId("test-client-id")
                      .clientSecret("test-client-secret"));
    } else {
      throw new IllegalArgumentException("Unsupported URL scheme: " + url);
    }

    storageCredentialOperations.createStorageCredential(createStorageCredential);

    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(externalLocationName)
            .url(url)
            .credentialName(storageCredentialName);
    externalLocationOperations.createExternalLocation(createExternalLocation);
  }

  @Test
  public void testGenerateTemporaryPathCredentials() throws ApiException {
    String[] testUrls = {
      "s3://teststorageaccount/test", "abfs://teststorageaccount/container",
    };

    for (String url : testUrls) {
      createExternalLocationAndStorageCredential(url);

      GenerateTemporaryPathCredential generateTemporaryPathCredential =
          new GenerateTemporaryPathCredential().url(url).operation(PathOperation.PATH_READ);
      TemporaryCredentials temporaryCredentials =
          credentialOperations.generateTemporaryPathCredentials(generateTemporaryPathCredential);

      if (url.startsWith("s3://")) {
        assertThat(temporaryCredentials.getAwsTempCredentials()).isNotNull();
        assertThat(temporaryCredentials.getAwsTempCredentials().getSessionToken())
            .isEqualTo("test-session-token");
        assertThat(temporaryCredentials.getAwsTempCredentials().getAccessKeyId())
            .isEqualTo("test-access-key-id");
        assertThat(temporaryCredentials.getAwsTempCredentials().getSecretAccessKey())
            .isEqualTo("test-secret-access-key");
      } else if (url.startsWith("abfs://") || url.startsWith("abfss://")) {
        assertThat(temporaryCredentials.getAzureUserDelegationSas()).isNotNull();
        assertThat(temporaryCredentials.getAzureUserDelegationSas().getSasToken())
            .isEqualTo("test-sas-token");
      }

      GenerateTemporaryPathCredential generateTemporaryPathCredentialWrite =
          new GenerateTemporaryPathCredential()
              .url(url.replace("teststorageaccount", "teststorageaccountwrite"))
              .operation(PathOperation.PATH_READ_WRITE);
      assertThatThrownBy(
              () ->
                  credentialOperations.generateTemporaryPathCredentials(
                      generateTemporaryPathCredentialWrite))
          .isInstanceOf(ApiException.class);
    }
  }
}
