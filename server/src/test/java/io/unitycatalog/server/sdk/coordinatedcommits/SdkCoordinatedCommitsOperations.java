package io.unitycatalog.server.sdk.coordinatedcommits;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CoordinatedCommitsApi;
import io.unitycatalog.client.model.Commit;
import io.unitycatalog.client.model.GetCommitsResponse;
import io.unitycatalog.server.base.coordinatedcommits.CoordinatedCommitsOperations;

public class SdkCoordinatedCommitsOperations implements CoordinatedCommitsOperations {
  private final CoordinatedCommitsApi coordinatedCommitsApi;

  public SdkCoordinatedCommitsOperations(ApiClient apiClient) {
    this.coordinatedCommitsApi = new CoordinatedCommitsApi(apiClient);
  }

  @Override
  public void commit(Commit commit) throws ApiException {
    coordinatedCommitsApi.commit(commit);
  }

  @Override
  public GetCommitsResponse getCommits(String tableId, Long startVersion) throws ApiException {
    return coordinatedCommitsApi.getCommits(tableId, null, startVersion, null, 100);
  }
}
