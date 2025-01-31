package io.unitycatalog.server.sdk.tempcredential;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.externallocation.ExternalLocationOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.storagecredential.StorageCredentialOperations;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.base.tempcredential.BaseTemporaryTableCredentialTest;
import io.unitycatalog.server.base.tempcredential.TemporaryCredentialOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.externallocation.SdkExternalLocationOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.storagecredential.SdkStorageCredentialOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;

public class SdkTemporaryTableCredentialTest extends BaseTemporaryTableCredentialTest {
  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig) {
    return new SdkTemporaryCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected ExternalLocationOperations createExternalLocationOperations(ServerConfig serverConfig) {
    return new SdkExternalLocationOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected StorageCredentialOperations createStorageCredentialOperations(
      ServerConfig serverConfig) {
    return new SdkStorageCredentialOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig serverConfig) {
    return new SdkTableOperations(TestUtils.createApiClient(serverConfig));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig serverConfig) {
    return new SdkSchemaOperations(TestUtils.createApiClient(serverConfig));
  }
}
