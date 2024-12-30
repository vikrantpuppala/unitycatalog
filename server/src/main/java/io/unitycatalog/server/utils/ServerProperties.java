package io.unitycatalog.server.utils;

import io.unitycatalog.server.service.credential.aws.S3StorageConfig;
import io.unitycatalog.server.service.credential.azure.ADLSStorageConfig;
import io.unitycatalog.server.service.credential.gcp.GCSStorageConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerProperties {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerProperties.class);
  @Getter private static final ServerProperties instance = new ServerProperties();
  private final Properties properties;

  public static final String SERVER_PROPERTIES_FILE = "etc/conf/server.properties";

  private ServerProperties() {
    properties = new Properties();
    loadProperties();
  }

  // Load properties from a configuration file
  private void loadProperties() {
    Path path = Paths.get(SERVER_PROPERTIES_FILE);
    if (!path.toFile().exists()) {
      LOGGER.error("Server properties file not found: {}", path);
      return;
    }
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
      LOGGER.debug("Server properties loaded successfully: {}", path);
    } catch (IOException ex) {
      LOGGER.error("Exception during loading properties", ex);
    }
  }

  public Optional<S3StorageConfig> getMetastoreS3Config() {
    String bucketPath = properties.getProperty("metastore.s3.bucketPath");
    String region = properties.getProperty("metastore.s3.region");
    String awsRoleArn = properties.getProperty("metastore.s3.awsRoleArn");
    String accessKey = properties.getProperty("metastore.s3.accessKey");
    String secretKey = properties.getProperty("metastore.s3.secretKey");
    String sessionToken = properties.getProperty("metastore.s3.sessionToken");

    if (bucketPath == null || awsRoleArn == null) {
      return Optional.empty();
    }

    return Optional.of(
        S3StorageConfig.builder()
            .bucketPath(bucketPath)
            .region(region)
            .awsRoleArn(awsRoleArn)
            .accessKey(accessKey)
            .secretKey(secretKey)
            .sessionToken(sessionToken)
            .build());
  }

  public Optional<ADLSStorageConfig> getMetastoreAdlsConfig() {
    String storageAccountName = properties.getProperty("metastore.adls.storageAccountName");
    String containerName = properties.getProperty("metastore.adls.containerName");
    String tenantId = properties.getProperty("metastore.adls.tenantId");
    String clientId = properties.getProperty("metastore.adls.clientId");
    String clientSecret = properties.getProperty("metastore.adls.clientSecret");
    String testMode = properties.getProperty("metastore.adls.testMode");

    if (storageAccountName == null
        || tenantId == null
        || clientId == null
        || clientSecret == null) {
      return Optional.empty();
    }

    return Optional.of(
        ADLSStorageConfig.builder()
            .storageAccountName(storageAccountName)
            .containerName(containerName)
            .tenantId(tenantId)
            .clientId(clientId)
            .clientSecret(clientSecret)
            .testMode(testMode != null && testMode.equalsIgnoreCase("true"))
            .build());
  }

  public Optional<GCSStorageConfig> getMetastoreGcsConfig() {
    String bucketPath = properties.getProperty("metastore.gcs.bucketPath");
    String jsonKeyFilePath = properties.getProperty("metastore.gcs.jsonKeyFilePath");

    if (bucketPath == null || jsonKeyFilePath == null) {
      return Optional.empty();
    }

    return Optional.of(
        GCSStorageConfig.builder()
            .bucketPath(bucketPath)
            .serviceAccountKeyJsonFilePath(jsonKeyFilePath)
            .build());
  }

  /**
   * Get a property value by key.
   *
   * <p>The key can be one of the following (in that order) before looking it up in the server
   * properties:
   *
   * <ol>
   *   <li>System property
   *   <li>Environment variable
   * </ol>
   */
  public String getProperty(String key) {
    if (System.getProperty(key) != null) return System.getProperty(key);
    if (System.getenv().containsKey(key)) return System.getenv(key);
    return properties.getProperty(key);
  }

  /**
   * Get a property value by key with a default value
   *
   * @see Properties#getProperty(String key, String defaultValue)
   */
  public String getProperty(String key, String defaultValue) {
    String val = getProperty(key);
    return (val == null) ? defaultValue : val;
  }

  public boolean isAuthorizationEnabled() {
    String authorization = instance.getProperty("server.authorization", "disable");
    return authorization.equalsIgnoreCase("enable");
  }
}
