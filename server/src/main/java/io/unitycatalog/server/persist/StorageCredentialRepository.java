package io.unitycatalog.server.persist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CreateStorageCredential;
import io.unitycatalog.server.model.ListStorageCredentialsResponse;
import io.unitycatalog.server.model.StorageCredentialInfo;
import io.unitycatalog.server.model.UpdateStorageCredential;
import io.unitycatalog.server.persist.dao.StorageCredentialDAO;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.persist.utils.StorageCredentialUtils;
import io.unitycatalog.server.utils.IdentityUtils;
import io.unitycatalog.server.utils.ValidationUtils;
import java.time.Instant;
import java.util.*;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class StorageCredentialRepository {
  private final Repositories repositories;
  private final SessionFactory sessionFactory;
  private static final PagedListingHelper<StorageCredentialDAO> LISTING_HELPER =
      new PagedListingHelper<>(StorageCredentialDAO.class);
  public static ObjectMapper objectMapper = new ObjectMapper();

  public StorageCredentialRepository(Repositories repositories, SessionFactory sessionFactory) {
    this.repositories = repositories;
    this.sessionFactory = sessionFactory;
  }

  public StorageCredentialInfo addStorageCredential(
      CreateStorageCredential createStorageCredential) {
    ValidationUtils.validateSqlObjectName(createStorageCredential.getName());
    String callerId = IdentityUtils.findPrincipalEmailAddress();
    UUID storageCredentialId = UUID.randomUUID();
    StorageCredentialInfo storageCredentialInfo =
        new StorageCredentialInfo()
            .id(storageCredentialId.toString())
            .name(createStorageCredential.getName())
            .comment(createStorageCredential.getComment())
            .owner(callerId)
            .createdAt(Instant.now().toEpochMilli())
            .createdBy(callerId);

    if (createStorageCredential.getAwsIamRole() != null) {
      storageCredentialInfo.setAwsIamRole(
          StorageCredentialUtils.fromAwsIamRoleRequest(createStorageCredential.getAwsIamRole()));
    } else if (createStorageCredential.getAzureServicePrincipal() != null) {
      storageCredentialInfo.setAzureServicePrincipal(
          createStorageCredential.getAzureServicePrincipal());
    } else {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT,
          "Storage credential must have one of aws_iam_role, azure_service_principal, azure_managed_identity or gcp_service_account");
    }

    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        if (getStorageCredentialDAO(session, createStorageCredential.getName()) != null) {
          throw new BaseException(
              ErrorCode.ALREADY_EXISTS,
              "Storage credential already exists: " + createStorageCredential.getName());
        }
        session.persist(StorageCredentialDAO.from(storageCredentialInfo));
        tx.commit();
        return storageCredentialInfo;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public StorageCredentialInfo getStorageCredential(String name) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        StorageCredentialDAO dao = getStorageCredentialDAO(session, name);
        if (dao == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
        }
        tx.commit();
        return dao.toStorageCredentialInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  protected StorageCredentialDAO getStorageCredentialDAO(Session session, String name) {
    Query<StorageCredentialDAO> query =
        session.createQuery(
            "FROM StorageCredentialDAO WHERE name = :value", StorageCredentialDAO.class);
    query.setParameter("value", name);
    query.setMaxResults(1);
    return query.uniqueResult();
  }

  public ListStorageCredentialsResponse listStorageCredentials(
      Optional<Integer> maxResults, Optional<String> pageToken) {
    try (Session session = sessionFactory.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        List<StorageCredentialDAO> daoList =
            LISTING_HELPER.listEntity(session, maxResults, pageToken, null);
        String nextPageToken = LISTING_HELPER.getNextPageToken(daoList, maxResults);
        List<StorageCredentialInfo> results = new ArrayList<>();
        for (StorageCredentialDAO dao : daoList) {
          results.add(dao.toStorageCredentialInfo());
        }
        tx.commit();
        return new ListStorageCredentialsResponse()
            .storageCredentials(results)
            .nextPageToken(nextPageToken);
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public StorageCredentialInfo updateStorageCredential(
      String name, UpdateStorageCredential updateStorageCredential) {
    String callerId = IdentityUtils.findPrincipalEmailAddress();

    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        StorageCredentialDAO existingCredential = getStorageCredentialDAO(session, name);
        if (existingCredential == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
        }

        // Update fields if provided
        if (updateStorageCredential.getNewName() != null) {
          ValidationUtils.validateSqlObjectName(updateStorageCredential.getNewName());
          if (getStorageCredentialDAO(session, updateStorageCredential.getNewName()) != null) {
            throw new BaseException(
                ErrorCode.ALREADY_EXISTS,
                "Storage credential already exists: " + updateStorageCredential.getNewName());
          }
          existingCredential.setName(updateStorageCredential.getNewName());
        }
        updateCredentialFields(existingCredential, updateStorageCredential);
        if (updateStorageCredential.getComment() != null) {
          existingCredential.setComment(updateStorageCredential.getComment());
        }
        existingCredential.setUpdatedAt(new Date());
        existingCredential.setUpdatedBy(callerId);

        session.merge(existingCredential);
        tx.commit();
        return existingCredential.toStorageCredentialInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public static void updateCredentialFields(
      StorageCredentialDAO existingCredential, UpdateStorageCredential updateStorageCredential) {
    try {
      if (updateStorageCredential.getAwsIamRole() != null) {
        existingCredential.setCredentialType(StorageCredentialDAO.CredentialType.AWS_IAM_ROLE);
        existingCredential.setCredential(
            objectMapper.writeValueAsString(
                StorageCredentialUtils.fromAwsIamRoleRequest(
                    updateStorageCredential.getAwsIamRole())));
      } else if (updateStorageCredential.getAzureServicePrincipal() != null) {
        existingCredential.setCredentialType(
            StorageCredentialDAO.CredentialType.AZURE_SERVICE_PRINCIPAL);
        existingCredential.setCredential(
            objectMapper.writeValueAsString(updateStorageCredential.getAzureServicePrincipal()));
      }
    } catch (JsonProcessingException e) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Failed to serialize credential: " + e.getMessage());
    }
  }

  public StorageCredentialInfo deleteStorageCredential(String name) {
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        StorageCredentialDAO existingCredential = getStorageCredentialDAO(session, name);
        if (existingCredential == null) {
          throw new BaseException(ErrorCode.NOT_FOUND, "Storage credential not found: " + name);
        }
        session.remove(existingCredential);
        tx.commit();
        return existingCredential.toStorageCredentialInfo();
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }
}
