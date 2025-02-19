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

import com.nimbusds.jose.jwk.JWKSet;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Map;

public class OAuth2AuthenticationProvider implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_OAUTH2.name();

    private final String tokenServerUrl;
    private final String oauthRedirectUrl;
    private final String clientId;
    private final String clientSecret;
    private final String jwksUrl;
    private final String principalFiled;
    private final String requiredIssuer;
    private final String requiredAudience;
    private final Long connectWaitTimeout;

    public OAuth2AuthenticationProvider(String tokenServerUrl,
                                        String oauthRedirectUrl,
                                        String clientId,
                                        String clientSecret,
                                        String jwksUrl,
                                        String principalFiled,
                                        String requiredIssuer,
                                        String requiredAudience,
                                        Long connectWaitTimeout) {
        this.tokenServerUrl = tokenServerUrl;
        this.oauthRedirectUrl = oauthRedirectUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.jwksUrl = jwksUrl;
        this.principalFiled = principalFiled;
        this.requiredIssuer = requiredIssuer;
        this.requiredAudience = requiredAudience;
        this.connectWaitTimeout = connectWaitTimeout;
    }

    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        UserAuthenticationInfo info = new UserAuthenticationInfo();
        info.setAuthPlugin(PLUGIN_NAME);
        info.setPassword(MysqlPassword.EMPTY_PASSWORD);
        info.setOrigUserHost(userIdentity.getUser(), userIdentity.getHost());
        info.setTextForAuthPlugin(userAuthOption == null ? null : userAuthOption.getAuthString());
        return info;
    }

    @Override
    public void authenticate(String user, String host, byte[] password, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        ConnectContext context = ConnectContext.get();
        long connectionId = context.getConnectionId();

        String authorizationCode = getAuthorizationCode();
        String oidcToken = getToken(authorizationCode, connectionId);
        try {
            JWKSet jwkSet;
            try {
                jwkSet = GlobalStateMgr.getCurrentState().getJwkMgr().getJwkSet(jwksUrl);
            } catch (IOException | ParseException e) {
                throw new AuthenticationException(e.getMessage());
            }
            OpenIdConnectVerifier.verify(oidcToken, user, jwkSet, principalFiled, requiredIssuer, requiredAudience);
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }

    public String getAuthorizationCode() throws AuthenticationException {
        ConnectContext context = ConnectContext.get();
        long startTime = System.currentTimeMillis();

        String authorizationCode;
        while (true) {
            authorizationCode = context.getAuthorizationCode();

            if (authorizationCode != null) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (System.currentTimeMillis() - startTime > connectWaitTimeout * 1000) {
                break;
            }
        }

        if (authorizationCode == null) {
            throw new AuthenticationException("OIDC wait callback timeout");
        }

        return authorizationCode;
    }

    /*
    OAuth2TokenMgr.Resource getResource() throws AuthenticationException {
        ConnectContext context = ConnectContext.get();
        long connectionId = context.getConnectionId();
        OAuth2TokenMgr oAuth2TokenMgr = GlobalStateMgr.getCurrentState().getoAuth2TokenMgr();

        long startTime = System.currentTimeMillis();
        OAuth2TokenMgr.Resource resource;
        while (true) {
            resource = oAuth2TokenMgr.oAuth2WaitCallbackList.remove(connectionId);

            if (resource != null) {
                break;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (System.currentTimeMillis() - startTime > connectWaitTimeout * 1000) {
                break;
            }
        }

        if (resource == null) {
            throw new AuthenticationException("OIDC wait callback timeout");
        }

        return resource;
    }
     */

    private String getToken(String authorizationCode, Long connectionId) {
        // Step 3: 通过授权码获取令牌
        Map<Object, Object> body = Map.of(
                "grant_type", "authorization_code",
                "code", authorizationCode,
                "redirect_uri", oauthRedirectUrl + "?connectionId=" + connectionId,
                "client_id", clientId,
                "client_secret", clientSecret
        );

        String requestBody = body.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8) + "=" +
                        URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8))
                .reduce((a, b) -> a + "&" + b)
                .orElse("");

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenServerUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        // Step 4: 处理响应
        if (response.statusCode() == 200) {
            return response.body();
        } else {
            return null;
        }
    }
}