package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.*;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hibernate.Session;

public class EntityConflictChecker<T extends IdentifiableDAO> {
  private final Class<T> entityClass;
  private final String locationColumnName;

  public EntityConflictChecker(Class<T> entityClass) {
    this.entityClass = entityClass;
    if (TableInfoDAO.class.equals(entityClass) ||
            ExternalLocationDAO.class.equals(entityClass) ||
            RegisteredModelInfoDAO.class.equals(entityClass)) {
      this.locationColumnName = "url";
    } else if (VolumeInfoDAO.class.equals(entityClass)) {
      this.locationColumnName = "storageLocation";
    } else {
      this.locationColumnName = null;
    }
  }

  public void cannotOverlapWithEntity(
      Session session,
      String url,
      boolean cannotBeAboveExternalLocations,
      boolean cannotBeUnderExternalLocations,
      boolean cannotBeTheSameExternalLocation) {
    CriteriaBuilder cb = session.getCriteriaBuilder();
    CriteriaQuery<T> cr = cb.createQuery(entityClass);
    Root<T> root = cr.from(entityClass);

    List<Predicate> predicates =
        buildPredicatesForQuery(
            url,
            cannotBeAboveExternalLocations,
            cannotBeUnderExternalLocations,
            cannotBeTheSameExternalLocation,
            cb,
            root);
    Optional<Predicate> entityBasedPredicate = getPredicateForEntity(cb, root);

    Predicate finalPredicate = cb.or(predicates.toArray(new Predicate[0]));
    if (entityBasedPredicate.isPresent()) {
      finalPredicate = cb.and(entityBasedPredicate.get(), finalPredicate);
    }

    cr.select(root).where(finalPredicate);
    List<T> overlappingEntities = session.createQuery(cr).getResultList();

    overlappingEntities.stream()
        .findFirst()
        .ifPresent(
            entity -> {
              throw new BaseException(
                  ErrorCode.FAILED_PRECONDITION,
                  "Provided path: " + url + " overlaps with " + entityClass.getSimpleName() + " " + entity.getName());
            });
  }

  private Optional<Predicate> getPredicateForEntity(CriteriaBuilder cb, Root<T> root) {
    if (entityClass == TableInfoDAO.class) {
      return Optional.of(cb.equal(root.get("type"), "EXTERNAL"));
    } else if (entityClass == VolumeInfoDAO.class) {
      return Optional.of(cb.equal(root.get("volumeType"), "EXTERNAL"));
    }
    return Optional.empty();
  }

  private static String getUrlWithTrailingSlash(String url) {
    if (url.endsWith("/")) {
      return url;
    }
    return url + "/";
  }

  private List<Predicate> buildPredicatesForQuery(
      String url,
      boolean cannotBeAboveExternalLocations,
      boolean cannotBeUnderExternalLocations,
      boolean cannotBeTheSameExternalLocation,
      CriteriaBuilder cb,
      Root<T> root) {
    List<Predicate> predicates = new ArrayList<>();
    String urlWithTrailingSlash = getUrlWithTrailingSlash(url);
    if (cannotBeAboveExternalLocations) {
      predicates.add(cb.like(root.get(locationColumnName), urlWithTrailingSlash + "%"));
    }
    if (cannotBeTheSameExternalLocation) {
      predicates.add(cb.equal(root.get(locationColumnName), url));
    }
    if (cannotBeUnderExternalLocations) {
      List<String> parentPaths = FileOperations.getParentPathsList(url);
      if (!parentPaths.isEmpty()) {
        predicates.add(root.get(locationColumnName).in(parentPaths));
      }
    }
    return predicates;
  }
}
