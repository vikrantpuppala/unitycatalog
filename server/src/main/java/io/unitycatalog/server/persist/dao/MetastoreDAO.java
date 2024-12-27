package io.unitycatalog.server.persist.dao;

import io.unitycatalog.server.model.GetMetastoreSummaryResponse;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Entity
@Table(name = "uc_metastore")
// Lombok
@Getter
@Setter
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode
public class MetastoreDAO {
  @Id
  @Column(name = "id")
  private UUID id;

  @Column(name = "storage_root_credential_id")
  private UUID storageRootCredentialId;

  @Column(name = "storage_root_url")
  private String storageRootUrl;

  public GetMetastoreSummaryResponse toGetMetastoreSummaryResponse() {
    return new GetMetastoreSummaryResponse().metastoreId(getId().toString());
  }
}
