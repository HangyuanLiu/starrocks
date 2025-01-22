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

package com.starrocks.http.rest;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.JWTUtils;
import com.starrocks.common.Config;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OAuth2Action extends RestBaseAction {
    public OAuth2Action(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/callback", new OAuth2Action(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String authorizationCode = getSingleParameter(request, "code", r -> r);
        String connectionIdStr = getSingleParameter(request, "connectionId", r -> r);
        Long connectionId = Long.valueOf(connectionIdStr);

        String openIdConnect;
        try {
            openIdConnect = getToken(authorizationCode, connectionId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (openIdConnect == null) {
            response.appendContent("Login Fail");
            sendResult(request, response);
        } else {
            try {
                JWKSet jwkSet = JWTUtils.getPublicKeyFromJWKS();

                JSONObject openIdConnectJson = new JSONObject(new String(openIdConnect));
                // 提取不同的 JWT
                String idToken = openIdConnectJson.getString("id_token");
                String accessToken = openIdConnectJson.getString("access_token");
                JWTUtils.verifyJWT(idToken, jwkSet);
                JWTUtils.verifyJWT(accessToken, jwkSet);

                SignedJWT signedJWT = SignedJWT.parse(idToken);

                // 获取 JWT 声明
                JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
                String jwtUserName = claims.getStringClaim("preferred_username");

                AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
                AuthenticationMgr.Resource resource = authenticationMgr.oAuth2WaitCallbackList
                        .get(connectionId);
                if (resource == null) {
                    resource = new AuthenticationMgr.Resource();
                    authenticationMgr.oAuth2WaitCallbackList.put(connectionId, resource);
                    try {
                        boolean result = resource.countDownLatch.await(30, TimeUnit.SECONDS);

                        if (!result) {
                            throw new AuthenticationException(AuthPlugin.AUTHENTICATION_OAUTH2_CLIENT.name()
                                    + " authentication timed out.");
                        }
                    } catch (InterruptedException e) {
                        throw new AuthenticationException(e.getMessage());
                    } finally {
                        authenticationMgr.oAuth2WaitCallbackList.remove(connectionId);
                    }
                } else {
                    resource.countDownLatch.countDown();
                }

                response.appendContent("Login Success");
                response.appendContent(openIdConnect);
                sendResult(request, response);
            } catch (Exception e) {
                response.appendContent("Login Fail");
                response.appendContent(e.getMessage());
                sendResult(request, response);
            }
        }
    }

    private String getToken(String authorizationCode, Long connectionId) throws IOException {
        // Step 3: 通过授权码获取令牌
        String tokenUrl = Config.token_server_url;
        Map<Object, Object> body = Map.of(
                "grant_type", "authorization_code",
                "code", authorizationCode,
                "redirect_uri", Config.redirect_url + "?connectionId=" + connectionId,
                "client_id", Config.client_id,
                "client_secret", Config.client_secret
        );

        String requestBody = body.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8) + "=" +
                        URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8))
                .reduce((a, b) -> a + "&" + b)
                .orElse("");

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
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
