package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.utils.FileUtils;
import io.unitycatalog.server.persist.utils.HibernateUtils;

import java.util.Objects;
import java.util.UUID;

import io.unitycatalog.server.utils.ServerProperties;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreRepository {
  private static final MetastoreRepository INSTANCE = new MetastoreRepository();
  private static final Logger LOGGER = LoggerFactory.getLogger(MetastoreRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();
  private static final StorageCredentialRepository STORAGE_CREDENTIAL_REPOSITORY =
      StorageCredentialRepository.getInstance();

  private MetastoreRepository() {}

  public static MetastoreRepository getInstance() {
    return INSTANCE;
  }

  public GetMetastoreSummaryResponse getMetastoreSummary() {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      MetastoreDAO metastoreDAO = getMetastoreDAO(session);
      if (metastoreDAO == null) {
        throw new BaseException(
            ErrorCode.NOT_FOUND,
            "No metastore found. Please check if the server is initialized properly.");
      }
      return metastoreDAO.toGetMetastoreSummaryResponse();
    }
  }

  public UUID getMetastoreId() {
    return UUID.fromString(getMetastoreSummary().getMetastoreId());
  }

  public MetastoreDAO getMetastoreDAO(Session session) {
    Query<MetastoreDAO> query = session.createQuery("FROM MetastoreDAO", MetastoreDAO.class);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public MetastoreDAO initMetastoreIfNeeded(ServerProperties serverProperties) {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        MetastoreDAO metastoreDAO = getMetastoreDAO(session);
        if (metastoreDAO == null) {
          LOGGER.info("No metastore found, initializing a metastore for the server...");
          metastoreDAO = new MetastoreDAO();
          metastoreDAO.setId(UUID.randomUUID());
        }
        createOrUpdateMetastoreProperties(serverProperties, metastoreDAO);
        session.persist(metastoreDAO);
        tx.commit();
        LOGGER.info("Server initialized with metastore id: {}", metastoreDAO.getId());
        return metastoreDAO;
      } catch (Exception e) {
        LOGGER.error("Failed to initialize metastore", e);
        tx.rollback();
        throw e;
      }
    }
  }

  private void createOrUpdateMetastoreProperties(ServerProperties serverProperties, MetastoreDAO metastoreDAO) {
    int cloudStoragesDefined =
        (serverProperties.getMetastoreS3Config().isPresent() ? 1 : 0)
            + (serverProperties.getMetastoreAdlsConfig().isPresent() ? 1 : 0)
            + (serverProperties.getMetastoreGcsConfig().isPresent() ? 1 : 0);
    if (cloudStoragesDefined > 1) {
        throw new BaseException(
            ErrorCode.INVALID_ARGUMENT,
            "Only one cloud storage configuration is allowed for the metastore.");
    }
    // TODO: this credential needs to be able to self-assume role
    //  or we could simply define two credentials: one to assume other roles and one for managed storage
    CreateStorageCredential createStorageCredential = new CreateStorageCredential();
    if (serverProperties.getMetastoreS3Config().isPresent()
            && !Objects.equals(metastoreDAO.getStorageRootUrl(), serverProperties.getMetastoreS3Config().get().getBucketPath())) {
      metastoreDAO.setStorageRootUrl(FileUtils.convertRelativePathToURI(serverProperties.getMetastoreS3Config().get().getBucketPath()));
      AwsIamRoleRequest awsIamRoleRequest = new AwsIamRoleRequest().roleArn(serverProperties.getMetastoreS3Config().get().getAwsRoleArn());
      createStorageCredential.awsIamRole(awsIamRoleRequest);
    }
    if (serverProperties.getMetastoreAdlsConfig().isPresent()
            && !Objects.equals(metastoreDAO.getStorageRootUrl(), serverProperties.getMetastoreAdlsConfig().get().getStorageAccountName())) {
      metastoreDAO.setStorageRootUrl(FileUtils.convertRelativePathToURI(serverProperties.getMetastoreAdlsConfig().get().getBasePath()));
      AzureServicePrincipal azureServicePrincipal = new AzureServicePrincipal()
              .directoryId(serverProperties.getMetastoreAdlsConfig().get().getTenantId())
              .applicationId(serverProperties.getMetastoreAdlsConfig().get().getClientId())
              .clientSecret(serverProperties.getMetastoreAdlsConfig().get().getClientSecret());
      createStorageCredential.azureServicePrincipal(azureServicePrincipal);
    }
    if (serverProperties.getMetastoreGcsConfig().isPresent()
            && !Objects.equals(metastoreDAO.getStorageRootUrl(), serverProperties.getMetastoreGcsConfig().get().getBucketPath())) {
      metastoreDAO.setStorageRootUrl(FileUtils.convertRelativePathToURI(serverProperties.getMetastoreGcsConfig().get().getBucketPath()));
      // TODO: gcp creds
    }
    STORAGE_CREDENTIAL_REPOSITORY.addStorageCredential(createStorageCredential);
  }
}
