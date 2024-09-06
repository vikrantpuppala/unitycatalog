package io.unitycatalog.server.base.coordinatedcommits;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.Commit;
import io.unitycatalog.client.model.GetCommitsResponse;

public interface CoordinatedCommitsOperations {
  void commit(Commit commit) throws ApiException;

  GetCommitsResponse getCommits(String tableId, Long startVersion) throws ApiException;
}
