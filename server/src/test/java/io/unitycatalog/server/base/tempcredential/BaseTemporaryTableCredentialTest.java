package io.unitycatalog.server.base.tempcredential;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTest;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTemporaryTableCredentialTest extends BaseCRUDTest {
  protected TemporaryCredentialOperations credentialOperations;
  protected ExternalLocationOperations externalLocationOperations;
  protected StorageCredentialOperations storageCredentialOperations;
  protected TableOperations tableOperations;
  protected SchemaOperations schemaOperations;

  protected abstract TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig);

  protected abstract ExternalLocationOperations createExternalLocationOperations(
      ServerConfig serverConfig);

  protected abstract StorageCredentialOperations createStorageCredentialOperations(
      ServerConfig serverConfig);

  protected abstract TableOperations createTableOperations(ServerConfig serverConfig);

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    initProperties.put("metastore.s3.bucketPath", "s3://uc/test");
    initProperties.put(
        "metastore.s3.awsRoleArn", "arn:aws:iam::123456789012:role/unitycatalog-role");
    super.setUp();
    credentialOperations = createTemporaryCredentialsOperations(serverConfig);
    externalLocationOperations = createExternalLocationOperations(serverConfig);
    storageCredentialOperations = createStorageCredentialOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
    schemaOperations = createSchemaOperations(serverConfig);
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

  protected void createCatalogAndSchema() throws ApiException {
    CreateCatalog createCatalog =
        new CreateCatalog().name(TestUtils.CATALOG_NAME).comment(TestUtils.COMMENT);
    catalogOperations.createCatalog(createCatalog);

    SchemaInfo schemaInfo =
        schemaOperations.createSchema(
            new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
  }

  @Test
  public void testGenerateTemporaryTableCredentials() throws ApiException, IOException {
    createCatalogAndSchema();
    String doNotCreateLocation = "donotcreatelocation";
    String[] testUrls = {
      "s3://teststorageaccount/test",
      "abfs://teststorageaccount/container",
      "s3://" + doNotCreateLocation + "/test",
      "abfs://" + doNotCreateLocation + "/container"
    };

    for (String url : testUrls) {
      if (!url.contains(doNotCreateLocation)) {
        createExternalLocationAndStorageCredential(url);
      }
      URI uri = URI.create(url);
      String tableName = "testtable-" + uri.getScheme() + "-" + uri.getAuthority();
      TableInfo tableInfo =
          BaseTableCRUDTest.createTestingTable(tableName, url + "/" + tableName, tableOperations);

      GenerateTemporaryTableCredential generateTemporaryTableCredential =
          new GenerateTemporaryTableCredential()
              .tableId(tableInfo.getTableId())
              .operation(TableOperation.READ);

      if (url.contains(doNotCreateLocation)) {
        assertThatThrownBy(
                () ->
                    credentialOperations.generateTemporaryTableCredentials(
                        generateTemporaryTableCredential))
            .isInstanceOf(ApiException.class);
      } else {
        TemporaryCredentials temporaryCredentials =
            credentialOperations.generateTemporaryTableCredentials(
                generateTemporaryTableCredential);
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
      }
    }
  }
}
