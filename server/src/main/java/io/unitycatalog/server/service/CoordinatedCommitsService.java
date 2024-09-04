package io.unitycatalog.server.service;

import static io.unitycatalog.server.handler.CoordinatedCommitsHandler.*;
import static io.unitycatalog.server.persist.CommitRepository.MAX_NUM_COMMITS_PER_TABLE;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.GetCommitsResponse;
import io.unitycatalog.server.persist.CommitRepository;
import io.unitycatalog.server.persist.dao.CommitDAO;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@ExceptionHandler(GlobalExceptionHandler.class)
public class CoordinatedCommitsService {
  public static final CommitRepository COMMIT_REPOSITORY = CommitRepository.getInstance();

  @Post("")
  public HttpResponse commit(Commit commit) {
    validate(commit);
    validateTablePath(commit);

    List<CommitDAO> firstAndLastCommits =
        COMMIT_REPOSITORY.getFirstAndLastCommits(commit.getTableId());
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
      String tableId,
      String tableUri,
      Long startVersion,
      Optional<String> tableFullName,
      Optional<Integer> maxNumCommits) {
    assert tableId != null;
    assert startVersion >= 0;
    assert maxNumCommits.orElse(0) >= 0;

    List<CommitDAO> commits =
        COMMIT_REPOSITORY.getLatestCommits(
            tableId, maxNumCommits.orElse(MAX_NUM_COMMITS_PER_TABLE));
    if (commits.isEmpty()) {
      return HttpResponse.ofJson(new GetCommitsResponse().latestTableVersion(-1L));
    } else if (commits.getLast().getIsBackfilledLatestCommit()) {
      return HttpResponse.ofJson(
          new GetCommitsResponse().latestTableVersion(commits.getLast().getCommitVersion()));
    } else {
      long endVersion = Math.max(startVersion, commits.getFirst().getCommitVersion());
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
              .latestTableVersion(commits.getFirst().getCommitVersion()));
    }
  }
}
