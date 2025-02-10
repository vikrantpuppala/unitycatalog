package io.unitycatalog.server.sdk;

import static io.unitycatalog.server.utils.TestUtils.*;
import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.model.ModelOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.volume.VolumeOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.sdk.models.SdkModelOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkStorageCredentialOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.sdk.volume.SdkVolumeOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LocationConflictTest extends BaseCRUDTest {
  private static final String EXTERNAL_LOCATION_NAME = "uc_testexternallocation";
  private static final String EXTERNAL_LOCATION_NAME_2 = "uc_testexternallocation_2";
  private static final String PARENT_URL = "s3://unitycatalog-test/level1/";
  private static final String URL = PARENT_URL + "level2/";
  private static final String NESTED_URL = URL + "level3/";
  private static final String CREDENTIAL_NAME = "uc_testcredential";
  private static final String DUMMY_ROLE_ARN = "arn:aws:iam::123456789012:role/role-name";

  protected ExternalLocationOperations externalLocationOperations;
  protected StorageCredentialOperations storageCredentialOperations;
  protected TableOperations tableOperations;
  protected ModelOperations modelOperations;
  protected VolumeOperations volumeOperations;
  protected SchemaOperations schemaOperations;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }

  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig config) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(config));
  }

  protected StorageCredentialOperations createStorageCredentialOperations(ServerConfig config) {
    return new SdkStorageCredentialOperations(TestUtils.createApiClient(config));
  }

  protected TableOperations createTableOperations(ServerConfig config) {
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  protected ModelOperations createModelOperations(ServerConfig config) {
    return new SdkModelOperations(TestUtils.createApiClient(config));
  }

  protected VolumeOperations createVolumeOperations(ServerConfig config) {
    return new SdkVolumeOperations(TestUtils.createApiClient(config));
  }

  protected void createCommonResources() throws ApiException {
    CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    externalLocationOperations = createExternalLocationOperations(serverConfig);
    storageCredentialOperations = createStorageCredentialOperations(serverConfig);
    tableOperations = createTableOperations(serverConfig);
    modelOperations = createModelOperations(serverConfig);
    volumeOperations = createVolumeOperations(serverConfig);
    schemaOperations = createSchemaOperations(serverConfig);
  }

  @Test
  public void testExternalLocationCannotBeAboveOrUnderAnotherExternalLocation()
      throws ApiException {
    CreateStorageCredential createStorageCredential =
        new CreateStorageCredential()
            .name(CREDENTIAL_NAME)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    storageCredentialOperations.createStorageCredential(createStorageCredential);

    // Create first External Location
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(URL)
            .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(createExternalLocation);

    // Try to create another External Location UNDER the first one → Should Fail
    CreateExternalLocation nestedExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME_2)
            .url(NESTED_URL)
            .credentialName(CREDENTIAL_NAME);

    assertThatThrownBy(
            () -> externalLocationOperations.createExternalLocation(nestedExternalLocation))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.FAILED_PRECONDITION.getHttpStatus().code())
        .hasMessageContaining("Provided path: " + NESTED_URL + " overlaps with");

    // Try to create another External Location ABOVE the first one → Should Fail
    CreateExternalLocation parentExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME_2)
            .url(PARENT_URL)
            .credentialName(CREDENTIAL_NAME);

    assertThatThrownBy(
            () -> externalLocationOperations.createExternalLocation(parentExternalLocation))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.FAILED_PRECONDITION.getHttpStatus().code())
        .hasMessageContaining("Provided path: " + PARENT_URL + " overlaps with");
  }

  @Test
  public void testExternalLocationCannotBeUnderTableOrVolume() throws ApiException, IOException {
    createCommonResources();
    CreateTable createTable = new CreateTable()
            .name(TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(URL);
    tableOperations.createTable(createTable);

    // Trying to create a volume under table → Should Fail
    CreateVolumeRequestContent createVolume =
        new CreateVolumeRequestContent().name(VOLUME_NAME)
                .catalogName(CATALOG_NAME)
                .schemaName(SCHEMA_NAME)
                .volumeType(VolumeType.EXTERNAL)
                .storageLocation(NESTED_URL);

    assertThatThrownBy(
            () -> volumeOperations.createVolume(createVolume))
            .isInstanceOf(ApiException.class)
            .hasFieldOrPropertyWithValue("code", ErrorCode.FAILED_PRECONDITION.getHttpStatus().code())
            .hasMessageContaining("Provided path: " + NESTED_URL + " overlaps with");

    // Try to create External Location under Table → Should Fail
    CreateExternalLocation externalLocationUnderTable =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(NESTED_URL)
            .credentialName(CREDENTIAL_NAME);

    assertThatThrownBy(
            () -> externalLocationOperations.createExternalLocation(externalLocationUnderTable))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.FAILED_PRECONDITION.getHttpStatus().code())
        .hasMessageContaining("Provided path: " + NESTED_URL + " overlaps with");

    // Create External Location on the same level as the Table → Should Succeed
    CreateStorageCredential createStorageCredential =
            new CreateStorageCredential()
                    .name(CREDENTIAL_NAME)
                    .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    storageCredentialOperations.createStorageCredential(createStorageCredential);

    CreateExternalLocation createExternalLocation =
            new CreateExternalLocation()
                    .name(EXTERNAL_LOCATION_NAME)
                    .url(URL)
                    .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(createExternalLocation);
  }

  @Test
  public void testExternalLocationCanBeCreatedAboveTable() throws ApiException, IOException {
    createCommonResources();
    CreateStorageCredential createStorageCredential =
        new CreateStorageCredential()
            .name(CREDENTIAL_NAME)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    storageCredentialOperations.createStorageCredential(createStorageCredential);

    // Create External Location
    CreateTable createTable = new CreateTable().name(TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(URL);
    tableOperations.createTable(createTable);

    // Create External Location ABOVE Table → Should Succeed
    CreateExternalLocation createExternalLocation =
            new CreateExternalLocation()
                    .name(EXTERNAL_LOCATION_NAME)
                    .url(PARENT_URL)
                    .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(createExternalLocation);
  }

  @Test
  public void testTableCannotBeAboveExternalLocation() throws ApiException {
    createCommonResources();
    CreateStorageCredential createStorageCredential =
        new CreateStorageCredential()
            .name(CREDENTIAL_NAME)
            .awsIamRole(new AwsIamRoleRequest().roleArn(DUMMY_ROLE_ARN));
    storageCredentialOperations.createStorageCredential(createStorageCredential);

    // Create External Location
    CreateExternalLocation createExternalLocation =
        new CreateExternalLocation()
            .name(EXTERNAL_LOCATION_NAME)
            .url(URL)
            .credentialName(CREDENTIAL_NAME);
    externalLocationOperations.createExternalLocation(createExternalLocation);

    // Try to create Table ABOVE the External Location → Should Fail
    CreateTable createTable = new CreateTable().name(TABLE_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(PARENT_URL);

    assertThatThrownBy(() -> tableOperations.createTable(createTable))
        .isInstanceOf(ApiException.class)
        .hasFieldOrPropertyWithValue("code", ErrorCode.FAILED_PRECONDITION.getHttpStatus().code())
        .hasMessageContaining("Provided path: " + PARENT_URL + " overlaps with");
  }
}
