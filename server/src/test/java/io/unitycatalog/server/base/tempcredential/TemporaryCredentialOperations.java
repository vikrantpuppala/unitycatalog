package io.unitycatalog.server.base.tempcredential;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TemporaryCredentials;

public interface TemporaryCredentialOperations {
  TemporaryCredentials generateTemporaryModelVersionCredentials(
      GenerateTemporaryModelVersionCredential generateTemporaryModelVersionCredentials)
      throws ApiException;

  TemporaryCredentials generateTemporaryTableCredentials(
      GenerateTemporaryTableCredential generateTemporaryTableCredential) throws ApiException;

  TemporaryCredentials generateTemporaryPathCredentials(
      GenerateTemporaryPathCredential generateTemporaryPathCredential) throws ApiException;
}
