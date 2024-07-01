/*
 * Unity Catalog API
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 0.1
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package io.unitycatalog.client.model;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import io.unitycatalog.client.model.CatalogInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * ListCatalogsResponse
 */
@JsonPropertyOrder({
  ListCatalogsResponse.JSON_PROPERTY_CATALOGS,
  ListCatalogsResponse.JSON_PROPERTY_NEXT_PAGE_TOKEN
})
@jakarta.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.5.0")
public class ListCatalogsResponse {
  public static final String JSON_PROPERTY_CATALOGS = "catalogs";
  private List<CatalogInfo> catalogs = new ArrayList<>();

  public static final String JSON_PROPERTY_NEXT_PAGE_TOKEN = "next_page_token";
  private String nextPageToken;

  public ListCatalogsResponse() { 
  }

  public ListCatalogsResponse catalogs(List<CatalogInfo> catalogs) {
    this.catalogs = catalogs;
    return this;
  }

  public ListCatalogsResponse addCatalogsItem(CatalogInfo catalogsItem) {
    if (this.catalogs == null) {
      this.catalogs = new ArrayList<>();
    }
    this.catalogs.add(catalogsItem);
    return this;
  }

   /**
   * An array of catalog information objects.
   * @return catalogs
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_CATALOGS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<CatalogInfo> getCatalogs() {
    return catalogs;
  }


  @JsonProperty(JSON_PROPERTY_CATALOGS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setCatalogs(List<CatalogInfo> catalogs) {
    this.catalogs = catalogs;
  }


  public ListCatalogsResponse nextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
    return this;
  }

   /**
   * Opaque token to retrieve the next page of results. Absent if there are no more pages. __page_token__ should be set to this value for the next request (for the next page of results). 
   * @return nextPageToken
  **/
  @jakarta.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_NEXT_PAGE_TOKEN)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getNextPageToken() {
    return nextPageToken;
  }


  @JsonProperty(JSON_PROPERTY_NEXT_PAGE_TOKEN)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
  }


  /**
   * Return true if this ListCatalogsResponse object is equal to o.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ListCatalogsResponse listCatalogsResponse = (ListCatalogsResponse) o;
    return Objects.equals(this.catalogs, listCatalogsResponse.catalogs) &&
        Objects.equals(this.nextPageToken, listCatalogsResponse.nextPageToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalogs, nextPageToken);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ListCatalogsResponse {\n");
    sb.append("    catalogs: ").append(toIndentedString(catalogs)).append("\n");
    sb.append("    nextPageToken: ").append(toIndentedString(nextPageToken)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

  /**
   * Convert the instance into URL query string.
   *
   * @return URL query string
   */
  public String toUrlQueryString() {
    return toUrlQueryString(null);
  }

  /**
   * Convert the instance into URL query string.
   *
   * @param prefix prefix of the query string
   * @return URL query string
   */
  public String toUrlQueryString(String prefix) {
    String suffix = "";
    String containerSuffix = "";
    String containerPrefix = "";
    if (prefix == null) {
      // style=form, explode=true, e.g. /pet?name=cat&type=manx
      prefix = "";
    } else {
      // deepObject style e.g. /pet?id[name]=cat&id[type]=manx
      prefix = prefix + "[";
      suffix = "]";
      containerSuffix = "]";
      containerPrefix = "[";
    }

    StringJoiner joiner = new StringJoiner("&");

    // add `catalogs` to the URL query string
    if (getCatalogs() != null) {
      for (int i = 0; i < getCatalogs().size(); i++) {
        if (getCatalogs().get(i) != null) {
          joiner.add(getCatalogs().get(i).toUrlQueryString(String.format("%scatalogs%s%s", prefix, suffix,
          "".equals(suffix) ? "" : String.format("%s%d%s", containerPrefix, i, containerSuffix))));
        }
      }
    }

    // add `next_page_token` to the URL query string
    if (getNextPageToken() != null) {
      joiner.add(String.format("%snext_page_token%s=%s", prefix, suffix, URLEncoder.encode(String.valueOf(getNextPageToken()), StandardCharsets.UTF_8).replaceAll("\\+", "%20")));
    }

    return joiner.toString();
  }
}

