package io.unitycatalog.server.service.credential.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.util.Context;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.implementation.util.DataLakeSasImplUtil;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import io.unitycatalog.server.model.AzureServicePrincipal;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.persist.ExternalLocationRepository;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.Set;

public class AzureCredentialVendor {
  private static final ExternalLocationRepository EXTERNAL_LOCATION_REPOSITORY =
      ExternalLocationRepository.getInstance();
  private final ServerProperties serverProperties;

  public AzureCredentialVendor(ServerProperties serverProperties) {
    this.serverProperties = serverProperties;
  }

  public AzureCredential vendAzureCredential(
      CredentialContext context, Optional<StorageCredentialInfo> optionalStorageCredential) {
    ADLSLocationUtils.ADLSLocationParts locationParts =
        ADLSLocationUtils.parseLocation(context.getUri());
    AzureServicePrincipal azureServicePrincipal =
        optionalStorageCredential
            .orElse(EXTERNAL_LOCATION_REPOSITORY.getStorageCredentialsForPath(context))
            .getAzureServicePrincipal();
    if (azureServicePrincipal == null) {
      throw new IllegalArgumentException(
          "No storage credentials found for path: " + context.getUri());
    }
    ADLSStorageConfig adlsStorageConfig =
        ADLSStorageConfig.builder()
            .tenantId(azureServicePrincipal.getDirectoryId())
            .clientId(azureServicePrincipal.getApplicationId())
            .clientSecret(azureServicePrincipal.getClientSecret())
            .build();
    // Return tests credentials only after ensuring that storage credentials for the path have been
    // created
    if (serverProperties.getEnvironment().equals(ServerProperties.Environment.TEST)) {
      return AzureCredential.builder()
          .sasToken("test-sas-token")
          .expirationTimeInEpochMillis(System.currentTimeMillis() + 3600000)
          .build();
    }
    DataLakeServiceAsyncClient serviceClient =
        new DataLakeServiceClientBuilder()
            .httpClient(HttpClient.createDefault())
            .endpoint("https://" + locationParts.account())
            .credential(getTokenCredential(adlsStorageConfig))
            .buildAsyncClient();

    // TODO: possibly make this configurable - defaulted to 1 hour right now
    OffsetDateTime start = OffsetDateTime.now();
    OffsetDateTime expiry = start.plusHours(1);
    UserDelegationKey key = serviceClient.getUserDelegationKey(start, expiry).toFuture().join();

    PathSasPermission perms = resolvePrivileges(context.getPrivileges());
    DataLakeServiceSasSignatureValues sasSignatureValues =
        new DataLakeServiceSasSignatureValues(expiry, perms).setStartTime(start);

    // azure supports only downscoping to a single location for now
    // azure wants only the path
    String path = URI.create(context.getUri()).getPath();
    // remove any preceding forward slashes or trailing forward slashes
    // hadoop ABFS strips trailing slash when preforming some operations so we need to vend
    // a cred for path without trailing slash
    path = path.replaceAll("^/+|/*$", "");

    String sasToken =
        new DataLakeSasImplUtil(sasSignatureValues, locationParts.container(), path, true)
            .generateUserDelegationSas(key, locationParts.accountName(), Context.NONE);

    return AzureCredential.builder()
        .sasToken(sasToken)
        .expirationTimeInEpochMillis(expiry.toInstant().toEpochMilli())
        .build();
  }

  private static TokenCredential getTokenCredential(ADLSStorageConfig adlsStorageConfig) {
    if (adlsStorageConfig == null) {
      // fallback to creating credential from environment variables (or somewhere on the default
      // chain)
      return new DefaultAzureCredentialBuilder().build();
    }
    return new ClientSecretCredentialBuilder()
        .tenantId(adlsStorageConfig.getTenantId())
        .clientId(adlsStorageConfig.getClientId())
        .clientSecret(adlsStorageConfig.getClientSecret())
        .build();
  }

  private static PathSasPermission resolvePrivileges(Set<CredentialContext.Privilege> privileges) {
    PathSasPermission result = new PathSasPermission();
    if (privileges.contains(CredentialContext.Privilege.UPDATE)) {
      result.setWritePermission(true);
      result.setDeletePermission(true);
    }
    if (privileges.contains(CredentialContext.Privilege.SELECT)) {
      result.setReadPermission(true);
      result.setListPermission(true);
    }
    return result;
  }
}
