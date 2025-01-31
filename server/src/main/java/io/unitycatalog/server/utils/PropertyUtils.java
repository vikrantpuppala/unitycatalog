package io.unitycatalog.server.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUtils.class);

  // Load properties from a configuration file
  public static Properties readPropertiesFromFile(String propertiesFile) {
    Path path = Paths.get(propertiesFile);
    Properties propertiesFromFile = new Properties();
    if (path.toFile().exists()) {
      try (InputStream input = Files.newInputStream(path)) {
        propertiesFromFile.load(input);
        LOGGER.debug("Server properties loaded successfully: {}", path);
      } catch (IOException ex) {
        LOGGER.error("Exception during loading properties", ex);
      }
    }
    return propertiesFromFile;
  }
}
