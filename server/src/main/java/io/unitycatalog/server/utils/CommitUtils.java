package io.unitycatalog.server.utils;

import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.CommitRepository;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;

import java.util.ArrayList;
import java.util.List;

public class CommitUtils {
    public static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();
    public static final CommitRepository COMMIT_REPOSITORY = CommitRepository.getInstance();

    public static void validate(Commit commit) {
        // Validate the commit object
        assert commit.getTableId() != null;
        assert commit.getTableUri() != null;

        // TODO: Add other assertions like the table URI path exists
    }

    public static void validateTablePath(Commit commit) {
        TableInfo tableInfo = TABLE_REPOSITORY.getTableById(commit.getTableId());
        assert tableInfo.getTableType() == TableType.MANAGED;
        assert tableInfo.getDataSourceFormat() == DataSourceFormat.DELTA;

        // TODO: Add other assertions like verifying the table path (tableInfo.getStorageLocation)
    }

    public static List<Commit> getFirstAndLastCommits(String tableId) {
        List<CommitDAO> firstCommit = COMMIT_REPOSITORY.getNCommits(tableId, 1, "asc");
        List<CommitDAO> lastCommit = COMMIT_REPOSITORY.getNCommits(tableId, 1, "desc");
        List<Commit> firstAndLastCommits = new ArrayList<>();
        if (!firstCommit.isEmpty())
            firstAndLastCommits.add(firstCommit.get(0).toCommit());
        if (!lastCommit.isEmpty())
            firstAndLastCommits.add(lastCommit.get(0).toCommit());
        return firstAndLastCommits;
    }
}
