package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.Commit;

import java.util.List;

import static io.unitycatalog.server.utils.CommitUtils.*;

@ExceptionHandler(GlobalExceptionHandler.class)

public class CommitService {
    @Post("/delta/commits")
    public HttpResponse commit(Commit commit) {
        validate(commit);
        validateTablePath(commit);

        List<Commit> firstAndLastCommits = getFirstAndLastCommits(commit.getTableId());
    }
}
