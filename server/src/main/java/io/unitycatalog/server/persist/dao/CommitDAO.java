package io.unitycatalog.server.persist.dao;

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
}
