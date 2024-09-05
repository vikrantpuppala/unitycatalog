package io.unitycatalog.server.service;

import static io.unitycatalog.server.handler.CoordinatedCommitsHandler.*;
import static io.unitycatalog.server.persist.CommitRepository.MAX_NUM_COMMITS_PER_TABLE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.GetCommitsResponse;
import io.unitycatalog.server.persist.CommitRepository;
import io.unitycatalog.server.persist.dao.CommitDAO;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CoordinatedCommitsService {
  public static final CommitRepository COMMIT_REPOSITORY = CommitRepository.getInstance();

  @Post("")
  public HttpResponse commit(Commit commit) {
    validate(commit);
    validateTablePath(commit);

    List<CommitDAO> firstAndLastCommits =
        COMMIT_REPOSITORY.getFirstAndLastCommits(UUID.fromString(commit.getTableId()));
    if (firstAndLastCommits.isEmpty()) {
      handleFirstCommit(commit);
    } else {
      if (commit.getCommitInfo() == null) {
        handleBackfillOnlyCommit(
            commit.getTableId(),
            commit.getLatestBackfilledVersion(),
            firstAndLastCommits.get(0),
            firstAndLastCommits.get(1));
      } else {
        if (firstAndLastCommits.get(1).getIsDisownCommit()) {
          // TODO: Confirm if we need to handle disown and reboard commits
          handleReboardCommit();
        } else {
          handleNormalCommit(commit, firstAndLastCommits.get(0), firstAndLastCommits.get(1));
        }
      }
    }
    if (commit.getMetadata() != null) {
      COMMIT_REPOSITORY.updateTableMetadata(commit);
    }
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("")
  public HttpResponse getCommits(
      @Param("table_id") String tableId,
      @Param("table_uri") String tableUri,
      @Param("start_version") long startVersion,
      @Param("table_full_name") Optional<String> tableFullName,
      @Param("max_num_commits") Optional<Integer> maxNumCommits) {
    assert tableId != null;
    assert startVersion >= 0;
    assert maxNumCommits.orElse(0) >= 0;

    List<CommitDAO> commits =
        COMMIT_REPOSITORY.getLatestCommits(
            UUID.fromString(tableId), maxNumCommits.orElse(MAX_NUM_COMMITS_PER_TABLE));
    int commitCount = commits.size();
    if (commitCount == 0) {
      return HttpResponse.ofJson(new GetCommitsResponse().latestTableVersion(-1L));
    } else if (commits.get(commitCount - 1).getIsBackfilledLatestCommit()) {
      return HttpResponse.ofJson(
          new GetCommitsResponse()
              .latestTableVersion(commits.get(commitCount - 1).getCommitVersion()));
    } else {
      long endVersion = Math.max(startVersion, commits.get(0).getCommitVersion() + 1);
      return HttpResponse.ofJson(
          new GetCommitsResponse()
              .commits(
                  commits.stream()
                      .filter(
                          c ->
                              c.getCommitVersion() >= startVersion
                                  && c.getCommitVersion() < endVersion)
                      .map(CommitDAO::toCommitInfo)
                      .collect(Collectors.toList()))
              .latestTableVersion(commits.get(0).getCommitVersion()));
    }
  }
}
