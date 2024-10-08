package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.GetMetastoreSummaryResponse;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.Constants;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
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
  private static MetastoreDAO CACHED_METASTORE_DAO = null;

  private MetastoreRepository() {}

  public static MetastoreRepository getInstance() {
    return INSTANCE;
  }

  public GetMetastoreSummaryResponse getMetastoreSummary() {
    if (CACHED_METASTORE_DAO != null) {
      return CACHED_METASTORE_DAO.toGetMetastoreSummaryResponse();
    }
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      MetastoreDAO metastoreDAO = getMetastoreDAO(session);
      if (metastoreDAO == null) {
        throw new BaseException(ErrorCode.NOT_FOUND, "No metastore found!");
      }
      CACHED_METASTORE_DAO = metastoreDAO;
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

  public MetastoreDAO initMetastoreIfNeeded() {
    try (Session session = SESSION_FACTORY.openSession()) {
      session.setDefaultReadOnly(true);
      Transaction tx = session.beginTransaction();
      try {
        MetastoreDAO metastoreDAO = getMetastoreDAO(session);
        if (metastoreDAO == null) {
          LOGGER.info("No metastore found, initializing a metastore for the server...");
          metastoreDAO = new MetastoreDAO();
          metastoreDAO.setId(UUID.randomUUID());
          metastoreDAO.setName(Constants.DEFAULT_METASTORE_NAME);
          metastoreDAO.setCreatedAt(Date.from(Instant.now()));
          session.persist(metastoreDAO);
          tx.commit();
        }
        LOGGER.info("Server initialized with metastore id: {}", metastoreDAO.getId());
        CACHED_METASTORE_DAO = metastoreDAO;
        return metastoreDAO;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }
}
