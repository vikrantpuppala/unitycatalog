package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.persist.utils.PathUtils;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PathUtilsTest {

  @Test
  public void testS3Url() {
    String url = "s3://uc/test";
    List<String> expected = Arrays.asList("s3://uc/");
    assertThat(PathUtils.getParentPathsList(url)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testAbfsUrl() {
    String url = "abfs://myaccount/container/folder";
    List<String> expected = Arrays.asList("abfs://myaccount/container/", "abfs://myaccount/");
    assertThat(PathUtils.getParentPathsList(url)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testAbfssUrl() {
    String url = "abfss://myaccount/container/folder/subfolder";
    List<String> expected =
        Arrays.asList(
            "abfss://myaccount/container/folder/",
            "abfss://myaccount/container/",
            "abfss://myaccount/");
    assertThat(PathUtils.getParentPathsList(url)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testGsUrl() {
    String url = "gs://bucket/folder/subfolder";
    List<String> expected = Arrays.asList("gs://bucket/folder/", "gs://bucket/");
    assertThat(PathUtils.getParentPathsList(url)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testInvalidUrl() {
    String url = "invalidpath/test";
    assertThatThrownBy(() -> PathUtils.getParentPathsList(url))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid URL");
  }
}
