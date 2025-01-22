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
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserIdentity;
import org.json.JSONObject;

import java.nio.ByteBuffer;

public class OpenIdConnectAuthenticationProvider implements AuthenticationProvider {
    public static final String PLUGIN_NAME = AuthPlugin.AUTHENTICATION_OPENID_CONNECT.name();

    @Override
    public UserAuthenticationInfo analyzeAuthOption(UserIdentity userIdentity, UserAuthOption userAuthOption)
            throws AuthenticationException {
        return null;
    }

    @Override
    public void authenticate(String user, String host, byte[] authResponse, byte[] randomString,
                             UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
        try {
            JWKSet jwkSet = JWTUtils.getPublicKeyFromJWKS();

            ByteBuffer authBuffer = ByteBuffer.wrap(authResponse);
            int len = MysqlProto.readInt1(authBuffer);
            byte[] openIdConnect = MysqlProto.readLenEncodedString(authBuffer);
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
            if (!jwtUserName.equalsIgnoreCase(user)) {
                throw new AuthenticationException("Login name " + user + " is not matched to user " + jwtUserName);
            }
        } catch (Exception e) {
            throw new AuthenticationException(e.getMessage());
        }
    }
}