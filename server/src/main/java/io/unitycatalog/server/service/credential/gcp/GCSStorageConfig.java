package io.unitycatalog.server.service.credential.gcp;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class GCSStorageConfig {
    private String bucketPath;
    private String serviceAccountKeyJsonFilePath;
}
