package io.unitycatalog.server.service.credential;

import com.google.auth.oauth2.AccessToken;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.service.credential.aws.AwsCredentialVendor;
import io.unitycatalog.server.service.credential.azure.AzureCredential;
import io.unitycatalog.server.service.credential.azure.AzureCredentialVendor;
import io.unitycatalog.server.service.credential.gcp.GcpCredentialVendor;
import io.unitycatalog.server.utils.ServerProperties;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.unitycatalog.server.utils.Constants.*;

public class CredentialOperations {

  private AwsCredentialVendor awsCredentialVendor = null;
  private AzureCredentialVendor azureCredentialVendor = null;
  private GcpCredentialVendor gcpCredentialVendor = null;
  private ServerProperties serverProperties;

  public CredentialOperations(ServerProperties serverProperties) {
    if (serverProperties.getMetastoreS3Config().isPresent()) {
      this.awsCredentialVendor = new AwsCredentialVendor(serverProperties);
    }
    this.azureCredentialVendor = new AzureCredentialVendor(serverProperties);
    this.gcpCredentialVendor = new GcpCredentialVendor(serverProperties);
    this.serverProperties = serverProperties;
    // if server properties are for a test environment, then we need to use the test credentials
  }

  public TemporaryCredentials vendCredential(String path, Set<CredentialContext.Privilege> privileges, Optional<StorageCredentialInfo> optionalStorageCredential) {
    if (path == null || path.isEmpty()) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Storage location is null or empty.");
    }
    URI storageLocationUri = URI.create(path);
    // TODO: At some point, we need to check if user/subject has privileges they are asking for
    CredentialContext credentialContext = CredentialContext.create(storageLocationUri, privileges);
    return vendCredential(credentialContext, optionalStorageCredential);
  }

  public TemporaryCredentials vendCredential(CredentialContext context, Optional<StorageCredentialInfo> optionalStorageCredential) {
    TemporaryCredentials temporaryCredentials = new TemporaryCredentials();
    switch (context.getStorageScheme()) {
      case URI_SCHEME_ABFS, URI_SCHEME_ABFSS -> {
        AzureCredential azureCredential = vendAzureCredential(context, optionalStorageCredential);
        temporaryCredentials.azureUserDelegationSas(new AzureUserDelegationSAS().sasToken(azureCredential.getSasToken()))
          .expirationTime(azureCredential.getExpirationTimeInEpochMillis());
      }
      case URI_SCHEME_GS -> {
        AccessToken gcpToken = vendGcpToken(context, optionalStorageCredential);
        temporaryCredentials.gcpOauthToken(new GcpOauthToken().oauthToken(gcpToken.getTokenValue()))
          .expirationTime(gcpToken.getExpirationTime().getTime());
      }
      case URI_SCHEME_S3 -> {
        Credentials awsSessionCredentials = vendAwsCredential(context, optionalStorageCredential);
        temporaryCredentials.awsTempCredentials(new AwsCredentials()
          .accessKeyId(awsSessionCredentials.accessKeyId())
          .secretAccessKey(awsSessionCredentials.secretAccessKey())
          .sessionToken(awsSessionCredentials.sessionToken()));
      }
    }

    return temporaryCredentials;
  }

  public Credentials vendAwsCredential(CredentialContext context, Optional<StorageCredentialInfo> optionalStorageCredential) {
    if (awsCredentialVendor == null) {
      throw new BaseException(ErrorCode.FAILED_PRECONDITION, "Metastore S3 configuration not provided.");
    }
    return awsCredentialVendor.vendAwsCredentials(context, optionalStorageCredential);
  }

  public AzureCredential vendAzureCredential(CredentialContext context, Optional<StorageCredentialInfo> optionalStorageCredential) {
    return azureCredentialVendor.vendAzureCredential(context, optionalStorageCredential);
  }

  public AccessToken vendGcpToken(CredentialContext context, Optional<StorageCredentialInfo> optionalStorageCredential) {
    return gcpCredentialVendor.vendGcpToken(context, optionalStorageCredential);
  }
}