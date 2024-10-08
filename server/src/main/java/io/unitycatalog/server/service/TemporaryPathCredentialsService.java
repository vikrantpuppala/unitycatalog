package io.unitycatalog.server.service;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Post;
import io.unitycatalog.server.auth.UnityCatalogAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.decorator.KeyMapperUtil;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.GlobalExceptionHandler;
import io.unitycatalog.server.model.*;
import io.unitycatalog.server.service.credential.CredentialContext;
import io.unitycatalog.server.service.credential.CredentialOperations;
import io.unitycatalog.server.utils.IdentityUtils;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static io.unitycatalog.server.model.SecurableType.METASTORE;
import static io.unitycatalog.server.model.SecurableType.VOLUME;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.SELECT;
import static io.unitycatalog.server.service.credential.CredentialContext.Privilege.UPDATE;

@ExceptionHandler(GlobalExceptionHandler.class)
public class TemporaryPathCredentialsService {
    private final CredentialOperations credentialOps;
    private final UnityAccessEvaluator evaluator;

    @SneakyThrows
    public TemporaryPathCredentialsService(UnityCatalogAuthorizer authorizer, CredentialOperations credentialOps) {
        this.credentialOps = credentialOps;
        this.evaluator = new UnityAccessEvaluator(authorizer);
    }

    @Post("")
    @AuthorizeExpression("#authorize(#principal, #metastore, OWNER)")
    @AuthorizeKey(METASTORE)
    public HttpResponse generateTemporaryPathCredential(
        GenerateTemporaryPathCredential generateTemporaryPathCredential) {
        return HttpResponse.ofJson(
                credentialOps.vendCredential(
                        generateTemporaryPathCredential.getUrl(),
                        pathOperationToPrivileges(generateTemporaryPathCredential.getOperation())));
    }

    private Set<CredentialContext.Privilege> pathOperationToPrivileges(PathOperation pathOperation) {
        return switch (pathOperation) {
            case PATH_READ -> Set.of(SELECT);
            case PATH_READ_WRITE, PATH_CREATE_TABLE -> Set.of(SELECT, UPDATE);
            case UNKNOWN_PATH_OPERATION -> Collections.emptySet();
        };
    }


    private void authorizeForOperation(GenerateTemporaryPathCredential generateTemporaryPathCredential) {
        // TODO: This is a short term solution to conditional expression evaluation based on additional request parameters.
        // This should be replaced with more direct annotations and syntax in the future.

        Map<SecurableType, Object> resourceKeys = KeyMapperUtil.mapResourceKeys(
                Map.of(METASTORE, "metastore"));
        if (!evaluator.evaluate(IdentityUtils.findPrincipalId(), "#authorize(#principal, #metastore, OWNER)", resourceKeys)) {
            throw new BaseException(ErrorCode.PERMISSION_DENIED, "Access denied.");
        }
    }

}
