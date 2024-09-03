package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.persist.CommitRepository;
import io.unitycatalog.server.persist.dao.CommitDAO;

import java.util.List;

import static io.unitycatalog.server.handler.CoordinatedCommitsHandler.*;

@ExceptionHandler(GlobalExceptionHandler.class)

public class CoordinatedCommitsService {
    public static final CommitRepository COMMIT_REPOSITORY = CommitRepository.getInstance();
    @Post("/delta/commits")
    public HttpResponse commit(Commit commit) {
        validate(commit);
        validateTablePath(commit);

        List<CommitDAO> firstAndLastCommits = COMMIT_REPOSITORY.getFirstAndLastCommits(commit.getTableId());
        if (firstAndLastCommits.isEmpty()) {
            handleFirstCommit(commit);
        } else {
            if (commit.getCommitInfo() == null) {
                handleBackfillOnlyCommit(commit.getTableId(), commit.getLatestBackfilledVersion(), firstAndLastCommits.get(0), firstAndLastCommits.get(1));
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

        }
    }
}
