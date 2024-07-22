package io.unitycatalog.server.persist;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.Commit;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.CommitDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.utils.HibernateUtils;
import io.unitycatalog.server.utils.Constants;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CommitRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogRepository.class);
    private static final SessionFactory SESSION_FACTORY = HibernateUtils.getSessionFactory();

    public List<CommitDAO> getNCommits(String tableId, int n, String order) {
        assert order.equals("asc") || order.equals("desc");
        try (Session session = SESSION_FACTORY.openSession()) {
            Transaction tx = session.beginTransaction();
            try {
                Query<CommitDAO> query =
                        session.createQuery("FROM CommitDAO WHERE tableId = :value order by commitVersion " + order, CommitDAO.class);
                query.setParameter("value", tableId);
                query.setMaxResults(n);
                tx.commit();
                return query.list();
            } catch (Exception e) {
                tx.rollback();
                throw e;
            }
        }
    }
}
