package io.unitycatalog.server.persist;

import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.model.Metadata;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.Constants;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitRepository {
  private static final CommitRepository INSTANCE = new CommitRepository();
  private static final Logger LOGGER = LoggerFactory.getLogger(CommitRepository.class);
  private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

  // The maximum number of commits per table.
  public static final Integer MAX_NUM_COMMITS_PER_TABLE = 50;
  private static final Integer NUM_COMMITS_PER_BATCH = 100;

  public static CommitRepository getInstance() {
    return INSTANCE;
  }

  public void saveCommit(Commit commit) {
    CommitDAO commitDAO = CommitDAO.from(commit);
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        session.persist(commitDAO);
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to save commit", e);
        throw e;
      }
    }
  }

  public void backfillCommits(
      UUID tableId, Long upTo, CommitDAO firstCommit, Long highestCommitVersion) {
    assert upTo >= firstCommit.getCommitVersion();
    assert upTo < highestCommitVersion;
    long numCommitsToDelete = upTo - firstCommit.getCommitVersion() + 1;
    if (numCommitsToDelete <= 0) {
      return;
    }
    // Retry backfilling 5 times to prioritize cleaning of the commit table and log bugs where there
    // are more
    // commits in the table than MAX_NUM_COMMITS_PER_TABLE
    for (int i = 0; i < 5 && numCommitsToDelete > 0; i++) {
      numCommitsToDelete -= deleteCommits(tableId, upTo);
      if (numCommitsToDelete > 0) {
        LOGGER.error(
            "Failed to backfill commits for tableId: {}, upTo: {}, in batch: {}, commits left: {}",
            tableId,
            upTo,
            i,
            numCommitsToDelete);
      }
    }
  }

  public int deleteCommits(UUID tableId, Long upTo) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        NativeQuery<CommitDAO> query =
            session.createNativeQuery(
                "DELETE FROM uc_commits WHERE table_id = :tableId AND commit_version <= :upTo LIMIT :numCommitsPerBatch",
                CommitDAO.class);
        query.setParameter("tableId", tableId);
        query.setParameter("upTo", upTo);
        query.setParameter("numCommitsPerBatch", NUM_COMMITS_PER_BATCH);
        int numDeleted = query.executeUpdate();
        tx.commit();
        return numDeleted;
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to delete commits", e);
        throw e;
      }
    }
  }

  public void markCommitAsLatestBackfilled(UUID tableId, Long commitVersion) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        NativeQuery<CommitDAO> query =
            session.createNativeQuery(
                "UPDATE uc_commits SET is_backfilled_latest_commit = true WHERE table_id = :tableId "
                    + "AND commit_version = :commitVersion",
                CommitDAO.class);
        query.setParameter("tableId", tableId);
        query.setParameter("commitVersion", commitVersion);
        query.executeUpdate();
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to mark commit as latest backfilled", e);
        throw e;
      }
    }
  }

  public List<CommitDAO> getFirstAndLastCommits(UUID tableId) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        // Use native SQL to get the first and last commits since HQL doesn't support UNION ALL
        String sql =
            "(SELECT * FROM uc_commits WHERE table_id = :tableId ORDER BY commit_version ASC LIMIT 1) "
                + "UNION ALL "
                + "(SELECT * FROM uc_commits WHERE table_id = :tableId ORDER BY commit_version DESC LIMIT 1)";

        Query<CommitDAO> query = session.createNativeQuery(sql, CommitDAO.class);
        query.setParameter("tableId", tableId);
        List<CommitDAO> result = query.getResultList();
        // Sort to ensure the first commit is at index 0
        result.sort(Comparator.comparing(CommitDAO::getCommitVersion));
        tx.commit();
        return result;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }

  public void updateTableMetadata(Commit commit) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        TableInfoDAO tableInfoDAO =
            session.get(TableInfoDAO.class, UUID.fromString(commit.getTableId()));
        Metadata metadata = commit.getMetadata();
        tableInfoDAO.setComment(metadata.getDescription());
        // Update properties
        PropertyRepository.findProperties(session, tableInfoDAO.getId(), Constants.TABLE)
            .forEach(session::remove);
        PropertyDAO.from(metadata.getProperties(), tableInfoDAO.getId(), Constants.TABLE)
            .forEach(session::persist);
        // Update columns
        tableInfoDAO.setColumns(ColumnInfoDAO.fromList(metadata.getSchema().getColumns()));
        session.merge(tableInfoDAO);
        tx.commit();
      } catch (Exception e) {
        tx.rollback();
        LOGGER.error("Failed to update table metadata", e);
        throw e;
      }
    }
  }

  public List<CommitDAO> getLatestCommits(UUID tableId, Integer maxNumCommits) {
    try (Session session = SESSION_FACTORY.openSession()) {
      Transaction tx = session.beginTransaction();
      try {
        Query<CommitDAO> query =
            session.createQuery(
                "FROM CommitDAO WHERE tableId = :tableId ORDER BY commitVersion DESC",
                CommitDAO.class);
        query.setParameter("tableId", tableId);
        query.setMaxResults(maxNumCommits);
        List<CommitDAO> result = query.list();
        tx.commit();
        return result;
      } catch (Exception e) {
        tx.rollback();
        throw e;
      }
    }
  }
}
