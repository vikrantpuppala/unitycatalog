package io.unitycatalog.server.base.tempcredential;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseTemporaryPathCredentialTest extends BaseCRUDTest {
  protected TemporaryCredentialOperations credentialOperations;

  protected abstract TemporaryCredentialOperations createTemporaryCredentialsOperations(
      ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    initProperties.put("metastore.s3.bucketPath", "s3://uc/test");
    initProperties.put(
        "metastore.s3.awsRoleArn", "arn:aws:iam::123456789012:role/unitycatalog-role");
    initProperties.put("metastore.s3.region", "us-west-2");
    super.setUp();
    credentialOperations = createTemporaryCredentialsOperations(serverConfig);
  }

  @Test
  public void testGenerateTemporaryPathCredentials() throws ApiException {
    GenerateTemporaryPathCredential generateTemporaryPathCredential =
        new GenerateTemporaryPathCredential()
            .url("s3://uc/test")
            .operation(PathOperation.PATH_READ);
    TemporaryCredentials temporaryCredentials =
        credentialOperations.generateTemporaryPathCredentials(generateTemporaryPathCredential);
    System.out.println(temporaryCredentials);
    assertThat(temporaryCredentials.getAwsTempCredentials()).isNotNull();
    assertThat(temporaryCredentials.getAwsTempCredentials().getSessionToken()).isNotNull();
  }
}
