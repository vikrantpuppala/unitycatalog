package io.unitycatalog.server.service.credential.azure;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ADLSStorageConfig {
  private String storageAccountName;
  private String containerName;
  private String tenantId;
  private String clientId;
  private String clientSecret;
  private boolean testMode;

  public String getBasePath() {
      return String.format("abfs://%s@%s.dfs.core.windows.net", containerName, storageAccountName);
  }
}
