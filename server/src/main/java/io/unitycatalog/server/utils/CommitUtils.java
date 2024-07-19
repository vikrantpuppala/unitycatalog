package io.unitycatalog.server.utils;

import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.TableInfoDAO;

public class CommitUtils {
    public static final TableRepository TABLE_REPOSITORY = TableRepository.getInstance();

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

    public static void getFirstAndLastCommits(String tableId) {

    }
}
