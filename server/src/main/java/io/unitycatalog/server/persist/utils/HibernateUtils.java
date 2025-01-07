package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HibernateUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(HibernateUtils.class);

  private static SessionFactory sessionFactory;
  private static Properties hibernateProperties;

  private HibernateUtils() {
    // Private constructor to prevent instantiation
  }

  public static synchronized void initialize() {
    if (sessionFactory == null) {
      hibernateProperties = new Properties();
      sessionFactory = createSessionFactory(ServerProperties.getInstance());
    }
  }

  public static SessionFactory getSessionFactory() {
    if (sessionFactory == null) {
      throw new RuntimeException("SessionFactory not initialized. Please call initialize() first.");
    }
    return sessionFactory;
  }

  public static Properties getHibernateProperties() {
    if (hibernateProperties == null) {
      throw new RuntimeException(
          "Hibernate properties not initialized. Please call initialize() first.");
    }
    return hibernateProperties;
  }

  private static SessionFactory createSessionFactory(ServerProperties serverProperties) {
    try {
      if (serverProperties == null) {
        throw new RuntimeException("PropertiesUtil instance is null in createSessionFactory");
      }

      Path hibernatePropertiesPath = Paths.get("etc/conf/hibernate.properties");
      if (!hibernatePropertiesPath.toFile().exists()) {
        LOGGER.warn("Hibernate properties file not found: {}", hibernatePropertiesPath);
        hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        hibernateProperties.setProperty(
            "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "update");
      } else {
        InputStream input = Files.newInputStream(hibernatePropertiesPath);
        hibernateProperties.load(input);
      }

      if (serverProperties.getEnvironment().equals(ServerProperties.Environment.TEST)) {
        hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        hibernateProperties.setProperty(
            "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        LOGGER.debug("Hibernate configuration set for testing");
      }
      Configuration configuration = new Configuration().setProperties(hibernateProperties);

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(RegisteredModelInfoDAO.class);
      configuration.addAnnotatedClass(ModelVersionInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);
      configuration.addAnnotatedClass(UserDAO.class);
      configuration.addAnnotatedClass(MetastoreDAO.class);
      configuration.addAnnotatedClass(StorageCredentialDAO.class);
      configuration.addAnnotatedClass(ExternalLocationDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (IOException e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }
}
