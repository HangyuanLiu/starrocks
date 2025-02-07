// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.authentication;

import com.starrocks.sql.analyzer.SemanticException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class OAuth2SecurityIntegration extends SecurityIntegration {
    public static final String SECURITY_INTEGRATION_TYPE_OAUTH2 = "oauth2";
    public static final String OAUTH2_TOKEN_SERVER_URL = "oauth2_token_server_url";
    public static final String OAUTH2_REDIRECT_URL = "oauth2_redirect_url";
    public static final String OAUTH2_CLIENT_ID = "oauth2_client_id";
    public static final String OAUTH2_CLIENT_SECRET = "oauth2_client_secret";
    public static final String OIDC_JWKS_URL = "oidc_jwks_url";
    public static final String OIDC_PRINCIPAL_FIELD = "oidc_principal_field";
    public static final String OIDC_REQUIRED_ISSUER = "oidc_required_issuer";
    public static final String OIDC_REQUIRED_AUDIENCE = "oidc_required_audience";
    public static final String OAUTH2_CONNECT_WAIT_TIMEOUT = "oauth2_connect_wait_timeout";

    final Set<String> requiredProperties = new HashSet<>(Arrays.asList(
            SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
            OAuth2SecurityIntegration.OAUTH2_TOKEN_SERVER_URL,
            OAuth2SecurityIntegration.OAUTH2_REDIRECT_URL,
            OAuth2SecurityIntegration.OAUTH2_CLIENT_ID,
            OAuth2SecurityIntegration.OAUTH2_CLIENT_SECRET,
            OAuth2SecurityIntegration.OIDC_JWKS_URL,
            OAuth2SecurityIntegration.OIDC_PRINCIPAL_FIELD));

    public OAuth2SecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
        String tokenServerUrl = propertyMap.get(OAUTH2_TOKEN_SERVER_URL);
        String oauthRedirectUrl = propertyMap.get(OAUTH2_REDIRECT_URL);
        String clientId = propertyMap.get(OAUTH2_CLIENT_ID);
        String clientSecret = propertyMap.get(OAUTH2_CLIENT_SECRET);
        String jwkSetUri = propertyMap.get(OIDC_JWKS_URL);
        String principalField = propertyMap.get(OIDC_PRINCIPAL_FIELD);
        String requireIssuer = propertyMap.get(OIDC_REQUIRED_ISSUER);
        String requireAudience = propertyMap.get(OIDC_REQUIRED_AUDIENCE);
        Long connectWaitTimeout = Long.valueOf(propertyMap.getOrDefault(OAUTH2_CONNECT_WAIT_TIMEOUT, "300"));

        return new OAuth2AuthenticationProvider(tokenServerUrl, oauthRedirectUrl, clientId, clientSecret, jwkSetUri,
                principalField, requireIssuer, requireAudience, connectWaitTimeout);
    }

    @Override
    public void analyzeProperties(Map<String, String> properties) {
        requiredProperties.forEach(s -> {
            if (!propertyMap.containsKey(s)) {
                throw new SemanticException("missing required property: " + s);
            }
        });

        validateIntegerProp(properties, OAUTH2_CONNECT_WAIT_TIMEOUT, 0, Integer.MAX_VALUE);
    }

    private void validateIntegerProp(Map<String, String> propertyMap, String key, int min, int max)
            throws SemanticException {
        if (propertyMap.containsKey(key)) {
            String val = propertyMap.get(key);
            try {
                int intVal = Integer.parseInt(val);
                if (intVal < min || intVal > max) {
                    throw new NumberFormatException("current value of '" +
                            key + "' is invalid, value: " + intVal +
                            ", should be in range [" + min + ", " + max + "]");
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("invalid '" +
                        key + "' property value: " + val + ", error: " + e.getMessage(), e);
            }
        }
    }
}
