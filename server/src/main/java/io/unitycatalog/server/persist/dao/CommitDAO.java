package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.CommitInfo;
import io.unitycatalog.server.model.Metadata;
import io.unitycatalog.server.model.Protocol;
import jakarta.persistence.*;
import lombok.*;

import java.util.Date;
import java.util.UUID;

@Entity
@Table(
        name = "uc_commits",
        uniqueConstraints = {
                @UniqueConstraint(columnNames = {"metastore_id", "table_id", "commit_version"})
        })
// Lombok
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CommitDAO {
    @Id
    @Column(name = "id", columnDefinition = "BINARY(16)")
    private UUID id;

    @Column(name = "table_id", nullable = false, columnDefinition = "BINARY(16)")
    private UUID tableId;

    @Column(name = "commit_version", nullable = false)
    private Long commitVersion;

    @Column(name = "commit_filename", nullable = false)
    private String commitFilename;

    @Column(name = "commit_filesize", nullable = false)
    private Long commitFilesize;

    @Column(name = "commit_file_modification_timestamp", nullable = false)
    private Date commitFileModificationTimestamp;

    @Column(name = "commit_timestamp", nullable = false)
    private Date commitTimestamp;

    @Column(name = "is_backfilled_latest_commit", nullable = false)
    private Boolean isBackfilledLatestCommit;

    @Column(name = "is_preregistration_commit", nullable = false)
    private Boolean isPreregistrationCommit;

    @Column(name = "is_disown_commit", nullable = false)
    private Boolean isDisownCommit;

    @Column(name = "metastore_id", nullable = false)
    private String metastoreId;

    public Commit toCommit() {
        Commit commit = new Commit()
                .tableId(tableId.toString())
                .commitInfo(new CommitInfo()
                        .version(commitVersion)
                        .fileName(commitFilename)
                        .fileSize(commitFilesize)
                        .fileModificationTimestamp(commitFileModificationTimestamp.getTime())
                        .timestamp(commitTimestamp.getTime())
                        .isDisownCommit(isDisownCommit));
        if (isBackfilledLatestCommit)
            commit.latestBackfilledVersion(commitVersion);
        return commit;
    }
}
